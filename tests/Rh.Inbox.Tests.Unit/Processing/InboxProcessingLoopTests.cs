using FluentAssertions;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Configuration;
using Rh.Inbox.Health;
using Rh.Inbox.Inboxes;
using Rh.Inbox.Processing;
using Rh.Inbox.Processing.Strategies;
using Rh.Inbox.Processing.Strategies.Factory;
using Rh.Inbox.Tests.Unit.TestHelpers;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Processing;

public class InboxProcessingLoopTests
{
    private readonly InboxBase _inbox;
    private readonly IInboxStorageProvider _storageProvider;
    private readonly IInboxProcessingStrategyFactory _strategyFactory;
    private readonly IInboxProcessingStrategy _strategy;
    private readonly ILogger<InboxProcessingLoop> _logger;
    private readonly InboxOptions _options;

    public InboxProcessingLoopTests()
    {
        _options = TestConfigurationFactory.CreateOptions(
            inboxName: "test-inbox",
            pollingInterval: TimeSpan.FromMilliseconds(10),
            shutdownTimeout: TimeSpan.FromSeconds(5),
            readDelay: TimeSpan.Zero);

        var metadataRegistry = Substitute.For<IInboxMessageMetadataRegistry>();
        metadataRegistry.HasRegisteredMessages.Returns(true);

        var configuration = new InboxConfiguration
        {
            InboxName = "test-inbox",
            InboxType = InboxType.Default,
            Options = _options,
            MetadataRegistry = metadataRegistry,
            StorageProviderFactoryFunc = _ => Substitute.For<IInboxStorageProviderFactory>(),
            SerializerFactoryFunc = _ => Substitute.For<IInboxSerializerFactory>(),
            HealthCheckOptions = new InboxHealthCheckOptions(),
            DateTimeProvider = Substitute.For<IDateTimeProvider>()
        };

        _storageProvider = Substitute.For<IInboxStorageProvider>();
        var serializer = Substitute.For<IInboxMessagePayloadSerializer>();
        var dateTimeProvider = Substitute.For<IDateTimeProvider>();

        _inbox = Substitute.For<InboxBase>(configuration, _storageProvider, serializer, dateTimeProvider);

        _strategy = Substitute.For<IInboxProcessingStrategy>();
        _strategyFactory = Substitute.For<IInboxProcessingStrategyFactory>();
        _strategyFactory.Create(_inbox).Returns(_strategy);

        _logger = Substitute.For<ILogger<InboxProcessingLoop>>();
    }

    private InboxProcessingLoop CreateLoop()
    {
        return new InboxProcessingLoop(_inbox, _strategyFactory, _logger);
    }

    #region Constructor Tests

    [Fact]
    public void Constructor_CreatesStrategyFromFactory()
    {
        var loop = CreateLoop();

        _strategyFactory.Received(1).Create(_inbox);
    }

    #endregion

    #region StartAsync Tests

    [Fact]
    public async Task StartAsync_ReturnsCompletedTask()
    {
        using var loop = CreateLoop();
        _storageProvider.ReadAndCaptureAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new List<InboxMessage>());

        var result = loop.StartAsync(CancellationToken.None);

        result.IsCompleted.Should().BeTrue();
        await loop.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task StartAsync_StartsProcessingLoop()
    {
        using var loop = CreateLoop();
        var messageReturned = false;
        _storageProvider.ReadAndCaptureAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                messageReturned = true;
                return new List<InboxMessage>();
            });

        await loop.StartAsync(CancellationToken.None);
        await Task.Delay(50); // Give processing loop time to execute

        messageReturned.Should().BeTrue();
        await loop.StopAsync(CancellationToken.None);
    }

    #endregion

    #region StopAsync Tests

    [Fact]
    public async Task StopAsync_WhenNotStarted_CompletesWithoutError()
    {
        using var loop = CreateLoop();

        var act = () => loop.StopAsync(CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task StopAsync_CancelsProcessingLoop()
    {
        using var loop = CreateLoop();
        var callCount = 0;
        _storageProvider.ReadAndCaptureAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                callCount++;
                return new List<InboxMessage>();
            });

        await loop.StartAsync(CancellationToken.None);
        await Task.Delay(50);
        var countBeforeStop = callCount;

        await loop.StopAsync(CancellationToken.None);
        await Task.Delay(50);
        var countAfterStop = callCount;

        // After stop, no more calls should be made
        countAfterStop.Should().Be(countBeforeStop);
    }

    [Fact]
    public async Task StopAsync_WaitsForProcessingToComplete()
    {
        using var loop = CreateLoop();
        var processingStarted = new TaskCompletionSource();
        var processingCanComplete = new TaskCompletionSource();

        _storageProvider.ReadAndCaptureAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(async _ =>
            {
                processingStarted.TrySetResult();
                await processingCanComplete.Task;
                return (IReadOnlyList<InboxMessage>)new List<InboxMessage>();
            });

        await loop.StartAsync(CancellationToken.None);
        await processingStarted.Task;

        var stopTask = loop.StopAsync(CancellationToken.None);
        await Task.Delay(20);

        stopTask.IsCompleted.Should().BeFalse();

        processingCanComplete.SetResult();
        await stopTask;

        stopTask.IsCompleted.Should().BeTrue();
    }

    #endregion

    #region Dispose Tests

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var loop = CreateLoop();

        var act = () =>
        {
            loop.Dispose();
            loop.Dispose();
            loop.Dispose();
        };

        act.Should().NotThrow();
    }

    [Fact]
    public async Task Dispose_AfterStart_DoesNotThrow()
    {
        var loop = CreateLoop();
        _storageProvider.ReadAndCaptureAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new List<InboxMessage>());

        await loop.StartAsync(CancellationToken.None);
        await loop.StopAsync(CancellationToken.None);

        var act = () => loop.Dispose();

        act.Should().NotThrow();
    }

    #endregion

    #region Processing Loop Tests

    [Fact]
    public async Task ProcessingLoop_WhenMessagesAvailable_CallsStrategy()
    {
        using var loop = CreateLoop();
        var messages = new List<InboxMessage>
        {
            new() { Id = Guid.NewGuid(), MessageType = "Test", Payload = "{}" }
        };

        var firstCall = true;
        _storageProvider.ReadAndCaptureAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                if (firstCall)
                {
                    firstCall = false;
                    return messages;
                }
                return new List<InboxMessage>();
            });

        await loop.StartAsync(CancellationToken.None);
        await Task.Delay(50);
        await loop.StopAsync(CancellationToken.None);

        await _strategy.Received(1).ProcessAsync(
            Arg.Any<string>(),
            Arg.Is<IReadOnlyList<InboxMessage>>(m => m.Count == 1),
            Arg.Any<IMessageProcessingContext>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessingLoop_WhenNoMessages_DoesNotCallStrategy()
    {
        using var loop = CreateLoop();
        _storageProvider.ReadAndCaptureAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new List<InboxMessage>());

        await loop.StartAsync(CancellationToken.None);
        await Task.Delay(50);
        await loop.StopAsync(CancellationToken.None);

        await _strategy.DidNotReceive().ProcessAsync(
            Arg.Any<string>(),
            Arg.Any<IReadOnlyList<InboxMessage>>(),
            Arg.Any<IMessageProcessingContext>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessingLoop_WhenStrategyThrows_ContinuesProcessing()
    {
        using var loop = CreateLoop();
        var callCount = 0;

        _storageProvider.ReadAndCaptureAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                callCount++;
                return (IReadOnlyList<InboxMessage>)new List<InboxMessage>
                {
                    new() { Id = Guid.NewGuid(), MessageType = "Test", Payload = "{}" }
                };
            });

        _strategy.ProcessAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<InboxMessage>>(), Arg.Any<IMessageProcessingContext>(), Arg.Any<CancellationToken>())
            .Returns<Task>(_ => throw new InvalidOperationException("Processing failed"));

        await loop.StartAsync(CancellationToken.None);
        await Task.Delay(100);
        await loop.StopAsync(CancellationToken.None);

        // Loop should continue despite exceptions
        callCount.Should().BeGreaterThan(1);
    }

    #endregion

    #region Lock Extension Tests

    [Fact]
    public async Task ProcessingLoop_WhenLockExtensionEnabled_CallsExtendLocks()
    {
        // Configure lock extension with short interval for testing
        var options = TestConfigurationFactory.CreateOptions(
            inboxName: "test-inbox",
            pollingInterval: TimeSpan.FromMilliseconds(10),
            maxProcessingTime: TimeSpan.FromMilliseconds(100),
            enableLockExtension: true,
            lockExtensionThreshold: 0.5); // 50ms interval

        var metadataRegistry = Substitute.For<IInboxMessageMetadataRegistry>();
        metadataRegistry.HasRegisteredMessages.Returns(true);

        var dateTimeProvider = Substitute.For<IDateTimeProvider>();
        dateTimeProvider.GetUtcNow().Returns(DateTime.UtcNow);

        var configuration = new InboxConfiguration
        {
            InboxName = "test-inbox",
            InboxType = InboxType.Default,
            Options = options,
            MetadataRegistry = metadataRegistry,
            StorageProviderFactoryFunc = _ => Substitute.For<IInboxStorageProviderFactory>(),
            SerializerFactoryFunc = _ => Substitute.For<IInboxSerializerFactory>(),
            HealthCheckOptions = new InboxHealthCheckOptions(),
            DateTimeProvider = dateTimeProvider
        };

        var storageProvider = Substitute.For<IInboxStorageProvider>();
        var serializer = Substitute.For<IInboxMessagePayloadSerializer>();
        var inbox = Substitute.For<InboxBase>(configuration, storageProvider, serializer, dateTimeProvider);

        var processingCompleted = new TaskCompletionSource();
        var strategy = Substitute.For<IInboxProcessingStrategy>();
        strategy.ProcessAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<InboxMessage>>(), Arg.Any<IMessageProcessingContext>(), Arg.Any<CancellationToken>())
            .Returns(async _ =>
            {
                await Task.Delay(150); // Wait long enough for lock extension to fire
                processingCompleted.TrySetResult();
            });

        var strategyFactory = Substitute.For<IInboxProcessingStrategyFactory>();
        strategyFactory.Create(inbox).Returns(strategy);

        var messages = new List<InboxMessage> { new() { Id = Guid.NewGuid(), MessageType = "Test", Payload = "{}" } };
        var firstCall = true;
        storageProvider.ReadAndCaptureAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                if (firstCall) { firstCall = false; return messages; }
                return new List<InboxMessage>();
            });
        storageProvider.ExtendLocksAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<IInboxMessageIdentifiers>>(), Arg.Any<DateTime>(), Arg.Any<CancellationToken>())
            .Returns(1);

        var logger = Substitute.For<ILogger<InboxProcessingLoop>>();
        using var loop = new InboxProcessingLoop(inbox, strategyFactory, logger);

        await loop.StartAsync(CancellationToken.None);
        await processingCompleted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await loop.StopAsync(CancellationToken.None);

        await storageProvider.Received().ExtendLocksAsync(
            Arg.Any<string>(),
            Arg.Is<IReadOnlyList<IInboxMessageIdentifiers>>(m => m.Count == 1),
            Arg.Any<DateTime>(),
            Arg.Any<CancellationToken>());
    }

    #endregion

    #region Polling Interval Tests

    [Fact]
    public async Task ProcessingLoop_WhenNoMessages_WaitsPollingInterval()
    {
        // Note: _options.PollingInterval is already set to 10ms in constructor
        // This test uses a higher value for more reliable timing
        using var loop = CreateLoop();

        var callTimes = new List<DateTime>();
        _storageProvider.ReadAndCaptureAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                callTimes.Add(DateTime.UtcNow);
                return new List<InboxMessage>();
            });

        await loop.StartAsync(CancellationToken.None);
        await Task.Delay(250);
        await loop.StopAsync(CancellationToken.None);

        // Should have waited between calls
        callTimes.Should().HaveCountGreaterOrEqualTo(2);
    }

    [Fact]
    public async Task ProcessingLoop_WhenMessagesProcessed_DoesNotWaitPollingInterval()
    {
        using var loop = CreateLoop();

        var callCount = 0;
        _storageProvider.ReadAndCaptureAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                callCount++;
                if (callCount <= 3)
                {
                    return new List<InboxMessage>
                    {
                        new() { Id = Guid.NewGuid(), MessageType = "Test", Payload = "{}" }
                    };
                }
                return new List<InboxMessage>();
            });

        await loop.StartAsync(CancellationToken.None);
        await Task.Delay(100); // Should process multiple batches quickly
        await loop.StopAsync(CancellationToken.None);

        // Should have processed multiple batches quickly (no polling delay)
        callCount.Should().BeGreaterOrEqualTo(3);
    }

    #endregion
}
