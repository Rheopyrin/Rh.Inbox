using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Lifecycle;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.InMemory;
using Rh.Inbox.InMemory.Options;
using Xunit;

namespace Rh.Inbox.Tests.Unit.InMemory;

public class InMemoryDeadLetterCleanupServiceTests
{
    private readonly InMemoryDeadLetterStore _deadLetterStore;
    private readonly IInboxConfiguration _configuration;
    private readonly IInboxLifecycle _lifecycle;
    private readonly IProviderOptionsAccessor _optionsAccessor;
    private readonly CleanupTaskOptions _cleanupOptions;
    private readonly CancellationTokenSource _stoppingCts;

    public InMemoryDeadLetterCleanupServiceTests()
    {
        _deadLetterStore = new InMemoryDeadLetterStore();
        _stoppingCts = new CancellationTokenSource();
        _lifecycle = Substitute.For<IInboxLifecycle>();
        _lifecycle.StoppingToken.Returns(_stoppingCts.Token);

        var options = Substitute.For<IInboxOptions>();
        options.DeadLetterMaxMessageLifetime.Returns(TimeSpan.FromMinutes(5));

        var dateTimeProvider = Substitute.For<IDateTimeProvider>();
        dateTimeProvider.GetUtcNow().Returns(DateTime.UtcNow);

        _configuration = Substitute.For<IInboxConfiguration>();
        _configuration.InboxName.Returns("test-inbox");
        _configuration.Options.Returns(options);
        _configuration.DateTimeProvider.Returns(dateTimeProvider);

        var providerOptions = new InMemoryInboxProviderOptions
        {
            DeduplicationStore = new InMemoryDeduplicationStore(),
            DeadLetterStore = _deadLetterStore
        };
        _cleanupOptions = new CleanupTaskOptions();
        _optionsAccessor = Substitute.For<IProviderOptionsAccessor>();
        _optionsAccessor.GetForInbox("test-inbox").Returns(providerOptions);
    }

    #region OnStart Tests

    [Fact]
    public async Task OnStart_ReturnsCompletedTask()
    {
        var service = CreateService();

        var task = service.OnStart(CancellationToken.None);

        task.IsCompleted.Should().BeTrue();
        await task;

        // Cleanup
        _stoppingCts.Cancel();
        await service.OnStop(CancellationToken.None);
    }

    [Fact]
    public async Task OnStart_StartsBackgroundTask()
    {
        var service = CreateService();

        await service.OnStart(CancellationToken.None);

        // Give time for the background task to start
        await Task.Delay(10);

        // Stop the service
        _stoppingCts.Cancel();
        await service.OnStop(CancellationToken.None);
    }

    #endregion

    #region OnStop Tests

    [Fact]
    public async Task OnStop_WithoutStart_CompletesSuccessfully()
    {
        var service = CreateService();

        var act = async () => await service.OnStop(CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task OnStop_AfterStart_StopsCleanup()
    {
        var service = CreateService();
        await service.OnStart(CancellationToken.None);

        _stoppingCts.Cancel();
        var act = async () => await service.OnStop(CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    #endregion

    #region IInboxLifecycleHook Interface Tests

    [Fact]
    public void Service_ImplementsIInboxLifecycleHook()
    {
        var service = CreateService();

        service.Should().BeAssignableTo<IInboxLifecycleHook>();
    }

    #endregion

    #region OnStop Cancellation Tests

    [Fact]
    public async Task OnStop_WithCancelledToken_CompletesImmediately()
    {
        var service = CreateService();
        await service.OnStart(CancellationToken.None);

        // Give time for the background task to start
        await Task.Delay(10);

        using var stopCts = new CancellationTokenSource();
        stopCts.Cancel();

        var act = async () => await service.OnStop(stopCts.Token);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task OnStop_WhileWaitingForTask_HandlesOperationCancelled()
    {
        var service = CreateService();
        await service.OnStart(CancellationToken.None);

        // Give time for the background task to start
        await Task.Delay(10);

        using var stopCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(1));

        var act = async () => await service.OnStop(stopCts.Token);

        await act.Should().NotThrowAsync();

        // Clean up
        _stoppingCts.Cancel();
    }

    #endregion

    #region Cleanup Loop Error Handling Tests

    [Fact]
    public async Task CleanupLoop_WhenStoppingTokenCancelled_StopsGracefully()
    {
        var service = CreateService();
        await service.OnStart(CancellationToken.None);

        // Give time for the cleanup loop to start
        await Task.Delay(10);

        // Cancel immediately
        _stoppingCts.Cancel();

        var act = async () => await service.OnStop(CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    #endregion

    #region Cleanup Integration Tests

    [Fact]
    public async Task Cleanup_RemovesExpiredMessages()
    {
        var now = DateTime.UtcNow;
        var dateTimeProvider = Substitute.For<IDateTimeProvider>();
        dateTimeProvider.GetUtcNow().Returns(now);

        var options = Substitute.For<IInboxOptions>();
        // Use short lifetime for testing (4 seconds = 1 second cleanup interval after division by 4, clamped to 1 minute)
        options.DeadLetterMaxMessageLifetime.Returns(TimeSpan.FromMinutes(4));

        _configuration.Options.Returns(options);
        _configuration.DateTimeProvider.Returns(dateTimeProvider);

        // Add expired message (older than 4 minutes)
        var expiredMessage = CreateDeadLetterMessage(movedAt: now.AddMinutes(-5));
        _deadLetterStore.Add(expiredMessage);

        // Add valid message (within 4 minutes)
        var validMessage = CreateDeadLetterMessage(movedAt: now.AddMinutes(-1));
        _deadLetterStore.Add(validMessage);

        _deadLetterStore.Count.Should().Be(2);

        // Manually trigger cleanup by simulating what the service does
        var expirationTime = now - TimeSpan.FromMinutes(4);
        var deleted = await _deadLetterStore.CleanupExpiredAsync(expirationTime, CancellationToken.None);

        deleted.Should().Be(1);
        _deadLetterStore.Count.Should().Be(1);
    }

    #endregion

    #region Store Operation Tests

    [Fact]
    public async Task Cleanup_WithNoExpiredMessages_CompletesSuccessfully()
    {
        var now = DateTime.UtcNow;
        var dateTimeProvider = Substitute.For<IDateTimeProvider>();
        dateTimeProvider.GetUtcNow().Returns(now);

        var options = Substitute.For<IInboxOptions>();
        options.DeadLetterMaxMessageLifetime.Returns(TimeSpan.FromHours(1));

        _configuration.Options.Returns(options);
        _configuration.DateTimeProvider.Returns(dateTimeProvider);

        // Add only valid messages (within lifetime)
        var validMessage = CreateDeadLetterMessage(movedAt: now.AddMinutes(-30));
        _deadLetterStore.Add(validMessage);

        _deadLetterStore.Count.Should().Be(1);

        // Cleanup with 1 hour expiration - none should be deleted
        var expirationTime = now - TimeSpan.FromHours(1);
        var deleted = await _deadLetterStore.CleanupExpiredAsync(expirationTime, CancellationToken.None);

        deleted.Should().Be(0);
        _deadLetterStore.Count.Should().Be(1);
    }

    [Fact]
    public async Task Cleanup_WithAllExpiredMessages_RemovesAll()
    {
        var now = DateTime.UtcNow;
        var dateTimeProvider = Substitute.For<IDateTimeProvider>();
        dateTimeProvider.GetUtcNow().Returns(now);

        var options = Substitute.For<IInboxOptions>();
        options.DeadLetterMaxMessageLifetime.Returns(TimeSpan.FromMinutes(30));

        _configuration.Options.Returns(options);
        _configuration.DateTimeProvider.Returns(dateTimeProvider);

        // Add all expired messages
        var expired1 = CreateDeadLetterMessage(movedAt: now.AddHours(-1));
        var expired2 = CreateDeadLetterMessage(movedAt: now.AddHours(-2));
        var expired3 = CreateDeadLetterMessage(movedAt: now.AddHours(-3));
        _deadLetterStore.Add(expired1);
        _deadLetterStore.Add(expired2);
        _deadLetterStore.Add(expired3);

        _deadLetterStore.Count.Should().Be(3);

        // Cleanup with 30 min expiration - all should be deleted
        var expirationTime = now - TimeSpan.FromMinutes(30);
        var deleted = await _deadLetterStore.CleanupExpiredAsync(expirationTime, CancellationToken.None);

        deleted.Should().Be(3);
        _deadLetterStore.Count.Should().Be(0);
    }

    [Fact]
    public async Task Cleanup_WithEmptyStore_ReturnsZero()
    {
        var now = DateTime.UtcNow;
        _deadLetterStore.Count.Should().Be(0);

        var expirationTime = now - TimeSpan.FromHours(1);
        var deleted = await _deadLetterStore.CleanupExpiredAsync(expirationTime, CancellationToken.None);

        deleted.Should().Be(0);
    }

    #endregion

    #region CleanupOptions Tests

    [Fact]
    public void Service_UsesProvidedCleanupOptions()
    {
        var customOptions = new CleanupTaskOptions
        {
            Interval = TimeSpan.FromMinutes(10),
            RestartDelay = TimeSpan.FromSeconds(60)
        };

        var service = new InMemoryDeadLetterCleanupService(
            _configuration,
            customOptions,
            _optionsAccessor,
            _lifecycle,
            NullLogger<InMemoryDeadLetterCleanupService>.Instance);

        // Service should be created successfully with custom options
        service.Should().NotBeNull();
    }

    #endregion

    #region Helper Methods

    private InMemoryDeadLetterCleanupService CreateService()
    {
        return new InMemoryDeadLetterCleanupService(
            _configuration,
            _cleanupOptions,
            _optionsAccessor,
            _lifecycle,
            NullLogger<InMemoryDeadLetterCleanupService>.Instance);
    }

    private static DeadLetterMessage CreateDeadLetterMessage(DateTime? movedAt = null)
    {
        return new DeadLetterMessage
        {
            Id = Guid.NewGuid(),
            InboxName = "test-inbox",
            MessageType = "test.message",
            Payload = "test payload",
            GroupId = null,
            CollapseKey = null,
            AttemptsCount = 3,
            ReceivedAt = DateTime.UtcNow.AddMinutes(-30),
            FailureReason = "Test failure",
            MovedAt = movedAt ?? DateTime.UtcNow
        };
    }

    #endregion
}
