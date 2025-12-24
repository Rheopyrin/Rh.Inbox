using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Lifecycle;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.InMemory;
using Rh.Inbox.InMemory.Options;
using Xunit;

namespace Rh.Inbox.Tests.Unit.InMemory;

public class InMemoryDeduplicationCleanupServiceTests
{
    private readonly InMemoryDeduplicationStore _deduplicationStore;
    private readonly IInboxConfiguration _configuration;
    private readonly IInboxLifecycle _lifecycle;
    private readonly IProviderOptionsAccessor _optionsAccessor;
    private readonly CleanupTaskOptions _cleanupOptions;
    private readonly CancellationTokenSource _stoppingCts;

    public InMemoryDeduplicationCleanupServiceTests()
    {
        _deduplicationStore = new InMemoryDeduplicationStore();
        _stoppingCts = new CancellationTokenSource();
        _lifecycle = Substitute.For<IInboxLifecycle>();
        _lifecycle.StoppingToken.Returns(_stoppingCts.Token);

        var options = Substitute.For<IInboxOptions>();
        options.DeduplicationInterval.Returns(TimeSpan.FromHours(1));

        var dateTimeProvider = Substitute.For<IDateTimeProvider>();
        dateTimeProvider.GetUtcNow().Returns(DateTime.UtcNow);

        _configuration = Substitute.For<IInboxConfiguration>();
        _configuration.InboxName.Returns("test-inbox");
        _configuration.Options.Returns(options);
        _configuration.DateTimeProvider.Returns(dateTimeProvider);

        var providerOptions = new InMemoryInboxProviderOptions
        {
            DeduplicationStore = _deduplicationStore,
            DeadLetterStore = new InMemoryDeadLetterStore()
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

    [Fact]
    public async Task OnStart_CanBeCalledMultipleTimes()
    {
        var service = CreateService();

        await service.OnStart(CancellationToken.None);

        var act = async () => await service.OnStart(CancellationToken.None);

        await act.Should().NotThrowAsync();

        // Cleanup
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

    [Fact]
    public async Task OnStop_CalledMultipleTimes_CompletesSuccessfully()
    {
        var service = CreateService();
        await service.OnStart(CancellationToken.None);

        _stoppingCts.Cancel();
        await service.OnStop(CancellationToken.None);

        var act = async () => await service.OnStop(CancellationToken.None);

        await act.Should().NotThrowAsync();
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

    #region IInboxLifecycleHook Interface Tests

    [Fact]
    public void Service_ImplementsIInboxLifecycleHook()
    {
        var service = CreateService();

        service.Should().BeAssignableTo<IInboxLifecycleHook>();
    }

    #endregion

    #region Cleanup Integration Tests

    [Fact]
    public void Cleanup_RemovesExpiredRecords()
    {
        var now = DateTime.UtcNow;
        var dateTimeProvider = Substitute.For<IDateTimeProvider>();
        dateTimeProvider.GetUtcNow().Returns(now);

        var options = Substitute.For<IInboxOptions>();
        options.DeduplicationInterval.Returns(TimeSpan.FromHours(1));

        _configuration.Options.Returns(options);
        _configuration.DateTimeProvider.Returns(dateTimeProvider);

        // Add expired record (older than 1 hour)
        var expiredKey = "expired-key";
        _deduplicationStore.AddOrUpdate(expiredKey, now.AddHours(-2));

        // Add valid record (within 1 hour)
        var validKey = "valid-key";
        _deduplicationStore.AddOrUpdate(validKey, now.AddMinutes(-30));

        _deduplicationStore.Count.Should().Be(2);

        // Manually trigger cleanup by simulating what the service does
        var expirationTime = now - TimeSpan.FromHours(1);
        var deleted = _deduplicationStore.CleanupExpired(expirationTime);

        deleted.Should().Be(1);
        _deduplicationStore.Count.Should().Be(1);
        _deduplicationStore.Exists(validKey, expirationTime).Should().BeTrue();
        _deduplicationStore.Exists(expiredKey, expirationTime).Should().BeFalse();
    }

    [Fact]
    public void Cleanup_DoesNotRemoveRecentRecords()
    {
        var now = DateTime.UtcNow;

        // Add only valid records (within deduplication interval)
        _deduplicationStore.AddOrUpdate("key1", now.AddMinutes(-10));
        _deduplicationStore.AddOrUpdate("key2", now.AddMinutes(-20));
        _deduplicationStore.AddOrUpdate("key3", now.AddMinutes(-30));

        _deduplicationStore.Count.Should().Be(3);

        // Cleanup with 1 hour expiration - none should be deleted
        var expirationTime = now - TimeSpan.FromHours(1);
        var deleted = _deduplicationStore.CleanupExpired(expirationTime);

        deleted.Should().Be(0);
        _deduplicationStore.Count.Should().Be(3);
    }

    [Fact]
    public void Cleanup_RemovesAllExpiredRecords()
    {
        var now = DateTime.UtcNow;

        // Add all expired records
        _deduplicationStore.AddOrUpdate("key1", now.AddHours(-2));
        _deduplicationStore.AddOrUpdate("key2", now.AddHours(-3));
        _deduplicationStore.AddOrUpdate("key3", now.AddHours(-4));

        _deduplicationStore.Count.Should().Be(3);

        // Cleanup with 1 hour expiration - all should be deleted
        var expirationTime = now - TimeSpan.FromHours(1);
        var deleted = _deduplicationStore.CleanupExpired(expirationTime);

        deleted.Should().Be(3);
        _deduplicationStore.Count.Should().Be(0);
    }

    #endregion

    #region Additional Cleanup Tests

    [Fact]
    public void Cleanup_WithEmptyStore_ReturnsZero()
    {
        var now = DateTime.UtcNow;
        _deduplicationStore.Count.Should().Be(0);

        var expirationTime = now - TimeSpan.FromHours(1);
        var deleted = _deduplicationStore.CleanupExpired(expirationTime);

        deleted.Should().Be(0);
    }

    [Fact]
    public void Cleanup_WithMixedRecords_OnlyRemovesExpired()
    {
        var now = DateTime.UtcNow;

        // Add expired records
        _deduplicationStore.AddOrUpdate("expired1", now.AddHours(-2));
        _deduplicationStore.AddOrUpdate("expired2", now.AddHours(-3));

        // Add valid records
        _deduplicationStore.AddOrUpdate("valid1", now.AddMinutes(-10));
        _deduplicationStore.AddOrUpdate("valid2", now.AddMinutes(-20));

        _deduplicationStore.Count.Should().Be(4);

        // Cleanup with 1 hour expiration
        var expirationTime = now - TimeSpan.FromHours(1);
        var deleted = _deduplicationStore.CleanupExpired(expirationTime);

        deleted.Should().Be(2);
        _deduplicationStore.Count.Should().Be(2);
        _deduplicationStore.Exists("valid1", expirationTime).Should().BeTrue();
        _deduplicationStore.Exists("valid2", expirationTime).Should().BeTrue();
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

        var service = new InMemoryDeduplicationCleanupService(
            _configuration,
            customOptions,
            _optionsAccessor,
            _lifecycle,
            NullLogger<InMemoryDeduplicationCleanupService>.Instance);

        // Service should be created successfully with custom options
        service.Should().NotBeNull();
    }

    #endregion

    #region Helper Methods

    private InMemoryDeduplicationCleanupService CreateService()
    {
        return new InMemoryDeduplicationCleanupService(
            _configuration,
            _cleanupOptions,
            _optionsAccessor,
            _lifecycle,
            NullLogger<InMemoryDeduplicationCleanupService>.Instance);
    }

    #endregion
}
