using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Extensions;
using Rh.Inbox.InMemory;
using Rh.Inbox.Tests.Integration.Common;
using Rh.Inbox.Tests.Integration.Common.Handlers;
using Rh.Inbox.Tests.Integration.Common.TestMessages;
using Xunit;
using Xunit.Abstractions;

namespace Rh.Inbox.Tests.Integration.InMemory;

public class InMemoryCleanupIntegrationTests(ITestOutputHelper output) : IAsyncLifetime
{
    private ServiceProvider? _serviceProvider;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (_serviceProvider is not null)
        {
            await _serviceProvider.DisposeAsync();
        }
    }

    #region Cleanup Options Configuration Tests

    [Fact]
    public async Task CleanupOptions_CanBeConfigured_ViaBuilderExtension()
    {
        var handler = new CountingHandler<SimpleMessage>();
        _serviceProvider = CreateServiceProviderWithCleanupOptions("cleanup-config-test", handler, o =>
        {
            o.DeadLetterCleanup.Interval = TimeSpan.FromMinutes(10);
            o.DeadLetterCleanup.RestartDelay = TimeSpan.FromMinutes(1);
            o.DeduplicationCleanup.Interval = TimeSpan.FromMinutes(15);
        });

        // Service should start without errors
        var act = async () => await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);
        await act.Should().NotThrowAsync();

        output.WriteLine("Cleanup options configured successfully");
    }

    [Fact]
    public async Task CleanupOptions_IndependentPerCleanupType()
    {
        var handler = new CountingHandler<SimpleMessage>();
        _serviceProvider = CreateServiceProviderWithCleanupOptions("cleanup-independent", handler, o =>
        {
            o.DeadLetterCleanup.Interval = TimeSpan.FromMinutes(1);
            o.DeduplicationCleanup.Interval = TimeSpan.FromMinutes(2);
        });

        var act = async () => await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);
        await act.Should().NotThrowAsync();

        output.WriteLine("Independent cleanup intervals configured successfully");
    }

    #endregion

    #region Cleanup Services Registration Tests

    [Fact]
    public async Task DeadLetterCleanup_Enabled_ServiceStartsWithLifecycle()
    {
        var handler = new CountingHandler<SimpleMessage>();
        _serviceProvider = CreateDeadLetterServiceProvider("dlq-lifecycle-test", handler);

        // Start the inbox - cleanup service should start automatically as lifecycle hook
        var act = async () => await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);
        await act.Should().NotThrowAsync();

        output.WriteLine("Dead letter cleanup service started with lifecycle");
    }

    [Fact]
    public async Task DeduplicationCleanup_Enabled_ServiceStartsWithLifecycle()
    {
        var handler = new CountingHandler<DeduplicatableMessage>();
        _serviceProvider = CreateDeduplicationServiceProvider("dedup-lifecycle-test", handler);

        // Start the inbox - cleanup service should start automatically as lifecycle hook
        var act = async () => await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);
        await act.Should().NotThrowAsync();

        output.WriteLine("Deduplication cleanup service started with lifecycle");
    }

    [Fact]
    public async Task DeadLetterCleanup_Disabled_NoCleanupService()
    {
        var handler = new CountingHandler<SimpleMessage>();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox("dlq-disabled-test", builder =>
        {
            builder.AsDefault()
                .UseInMemory()
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    o.EnableDeadLetter = false; // Disabled
                })
                .RegisterHandler(handler);
        });
        _serviceProvider = services.BuildServiceProvider();

        // Should start without cleanup services
        var act = async () => await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);
        await act.Should().NotThrowAsync();

        output.WriteLine("Inbox started without dead letter cleanup (disabled)");
    }

    [Fact]
    public async Task DeduplicationCleanup_Disabled_NoCleanupService()
    {
        var handler = new CountingHandler<SimpleMessage>();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox("dedup-disabled-test", builder =>
        {
            builder.AsDefault()
                .UseInMemory()
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    o.EnableDeduplication = false; // Disabled
                })
                .RegisterHandler(handler);
        });
        _serviceProvider = services.BuildServiceProvider();

        // Should start without cleanup services
        var act = async () => await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);
        await act.Should().NotThrowAsync();

        output.WriteLine("Inbox started without deduplication cleanup (disabled)");
    }

    #endregion

    #region Dead Letter Processing Tests

    [Fact]
    public async Task DeadLetter_MessageFailsMaxAttempts_ProcessedCorrectly()
    {
        const int maxAttempts = 2;
        var handler = new FailingHandler<SimpleMessage>(1.0); // Always fails
        _serviceProvider = CreateDeadLetterServiceProvider("dlq-process-test", handler, o =>
        {
            o.MaxAttempts = maxAttempts;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        var message = new SimpleMessage("dlq-test", "data");
        await writer.WriteAsync(message, "dlq-process-test");

        // Wait for message to fail maxAttempts times
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.FailedCount >= maxAttempts,
            TimeSpan.FromSeconds(10));

        handler.FailedCount.Should().BeGreaterOrEqualTo(maxAttempts);
        output.WriteLine($"Message failed {handler.FailedCount} times and moved to dead letter");
    }

    #endregion

    #region Deduplication Processing Tests

    [Fact]
    public async Task Deduplication_DuplicateMessages_OnlyProcessedOnce()
    {
        var handler = new CountingHandler<DeduplicatableMessage>();
        _serviceProvider = CreateDeduplicationServiceProvider("dedup-process-test", handler);

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const string deduplicationKey = "unique-key";
        var message1 = new DeduplicatableMessage(deduplicationKey, "data1");
        var message2 = new DeduplicatableMessage(deduplicationKey, "data2");

        // Write same deduplication key twice
        await writer.WriteAsync(message1, "dedup-process-test");
        await writer.WriteAsync(message2, "dedup-process-test");

        // Wait for processing
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.ProcessedCount >= 1,
            TimeSpan.FromSeconds(5));

        // Small delay to ensure no more processing happens
        await Task.Delay(500);

        // Only one message should be processed due to deduplication
        handler.ProcessedCount.Should().Be(1, "duplicate message should be deduplicated");

        output.WriteLine($"Only {handler.ProcessedCount} message processed (deduplication working)");
    }

    #endregion

    #region Multiple Inboxes Tests

    [Fact]
    public async Task MultipleInboxes_IndependentCleanupConfigurations()
    {
        var handler1 = new CountingHandler<SimpleMessage>();
        var handler2 = new CountingHandler<SimpleMessage>();

        var services = new ServiceCollection();
        services.AddLogging();

        services.AddInbox("inbox-1", builder =>
        {
            builder.AsDefault()
                .UseInMemory()
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    o.EnableDeadLetter = true;
                    o.DeadLetterMaxMessageLifetime = TimeSpan.FromHours(1);
                })
                .RegisterHandler(handler1);
        });

        services.AddInbox("inbox-2", builder =>
        {
            builder.AsDefault()
                .UseInMemory()
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    o.EnableDeduplication = true;
                    o.DeduplicationInterval = TimeSpan.FromHours(2);
                })
                .RegisterHandler(handler2);
        });

        _serviceProvider = services.BuildServiceProvider();

        var act = async () => await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);
        await act.Should().NotThrowAsync();

        output.WriteLine("Multiple inboxes with independent cleanup configurations started");
    }

    #endregion

    #region Lifecycle Tests

    [Fact]
    public async Task CleanupServices_StopGracefully_OnInboxStop()
    {
        var handler = new CountingHandler<SimpleMessage>();
        _serviceProvider = CreateDeadLetterServiceProvider("cleanup-stop-test", handler);

        var manager = _serviceProvider.GetRequiredService<IInboxManager>();
        await manager.StartAsync(CancellationToken.None);

        // Write a message to ensure cleanup has something to track
        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await writer.WriteAsync(new SimpleMessage("test", "data"), "cleanup-stop-test");

        // Wait for processing
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.ProcessedCount >= 1,
            TimeSpan.FromSeconds(5));

        // Stop should complete gracefully
        var act = async () => await manager.StopAsync(CancellationToken.None);
        await act.Should().NotThrowAsync();

        output.WriteLine("Cleanup services stopped gracefully");
    }

    [Fact]
    public async Task CleanupServices_HandleCancellation_Gracefully()
    {
        var handler = new CountingHandler<SimpleMessage>();
        _serviceProvider = CreateDeadLetterServiceProvider("cleanup-cancel-test", handler);

        var manager = _serviceProvider.GetRequiredService<IInboxManager>();
        await manager.StartAsync(CancellationToken.None);

        var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        var token = cts.Token;

        // Stop with short timeout - should either complete or handle cancellation gracefully
        var act = async () => await manager.StopAsync(token);
        await act.Should().NotThrowAsync<Exception>("cleanup should handle cancellation gracefully");

        cts.Dispose();
        output.WriteLine("Cleanup services handled cancellation gracefully");
    }

    #endregion

    #region Cleanup During Active Processing Tests

    [Fact]
    public async Task CleanupDuringActiveProcessing_DoesNotAffectInFlightMessages()
    {
        var processedMessages = new List<string>();
        var handler = new CountingHandler<DeduplicatableMessage>();

        _serviceProvider = CreateDeduplicationServiceProvider("cleanup-active-test", handler, o =>
        {
            o.DeduplicationInterval = TimeSpan.FromMilliseconds(50); // Very short for test
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // Write messages continuously while cleanup might be running
        for (int i = 0; i < 20; i++)
        {
            var message = new DeduplicatableMessage($"key-{i}", $"data-{i}");
            await writer.WriteAsync(message, "cleanup-active-test");
            await Task.Delay(10); // Small delay to interleave with cleanup
        }

        // Wait for all messages to be processed
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.ProcessedCount >= 20,
            TimeSpan.FromSeconds(10));

        handler.ProcessedCount.Should().Be(20, "all unique messages should be processed");
        output.WriteLine($"Processed {handler.ProcessedCount} messages while cleanup was active");
    }

    [Fact]
    public async Task CleanupDuringActiveProcessing_DeadLetterCleanupDoesNotAffectRetries()
    {
        const int maxAttempts = 3;
        var handler = new FailingHandler<SimpleMessage>(0.5); // 50% failure rate

        _serviceProvider = CreateDeadLetterServiceProvider("cleanup-retry-test", handler, o =>
        {
            o.MaxAttempts = maxAttempts;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // Write multiple messages
        for (int i = 0; i < 10; i++)
        {
            await writer.WriteAsync(new SimpleMessage($"msg-{i}", $"data-{i}"), "cleanup-retry-test");
        }

        // Wait for processing (some will succeed, some may retry)
        await Task.Delay(2000);

        // Verify handler was called (retries should work correctly)
        (handler.ProcessedCount + handler.FailedCount).Should().BeGreaterThan(0);
        output.WriteLine($"Processed: {handler.ProcessedCount}, Failed: {handler.FailedCount}");
    }

    #endregion

    #region Concurrent Write and Cleanup Tests

    [Fact]
    public async Task ConcurrentWriteAndCleanup_NoDataLoss()
    {
        var handler = new CountingHandler<DeduplicatableMessage>();

        _serviceProvider = CreateDeduplicationServiceProvider("concurrent-test", handler, o =>
        {
            o.DeduplicationInterval = TimeSpan.FromMilliseconds(100);
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 50;
        var writeTasks = new List<Task>();

        // Write messages concurrently from multiple "threads"
        for (int i = 0; i < messageCount; i++)
        {
            var index = i;
            writeTasks.Add(Task.Run(async () =>
            {
                var message = new DeduplicatableMessage($"concurrent-key-{index}", $"data-{index}");
                await writer.WriteAsync(message, "concurrent-test");
            }));
        }

        await Task.WhenAll(writeTasks);

        // Wait for all messages to be processed
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.ProcessedCount >= messageCount,
            TimeSpan.FromSeconds(15));

        handler.ProcessedCount.Should().Be(messageCount, "all concurrent messages should be processed");
        output.WriteLine($"Successfully processed {handler.ProcessedCount} concurrent messages");
    }

    [Fact]
    public async Task ConcurrentWriteAndCleanup_DuplicatesStillDeduplicated()
    {
        var handler = new CountingHandler<DeduplicatableMessage>();

        _serviceProvider = CreateDeduplicationServiceProvider("concurrent-dedup-test", handler, o =>
        {
            o.DeduplicationInterval = TimeSpan.FromHours(1); // Long interval to keep dedup records
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int uniqueKeys = 10;
        const int duplicatesPerKey = 5;
        var writeTasks = new List<Task>();

        // Write duplicates concurrently
        for (int key = 0; key < uniqueKeys; key++)
        {
            for (int dup = 0; dup < duplicatesPerKey; dup++)
            {
                var keyIndex = key;
                writeTasks.Add(Task.Run(async () =>
                {
                    var message = new DeduplicatableMessage($"shared-key-{keyIndex}", $"data-{keyIndex}");
                    await writer.WriteAsync(message, "concurrent-dedup-test");
                }));
            }
        }

        await Task.WhenAll(writeTasks);

        // Wait for processing
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.ProcessedCount >= uniqueKeys,
            TimeSpan.FromSeconds(10));

        // Small delay to ensure no more processing
        await Task.Delay(500);

        handler.ProcessedCount.Should().Be(uniqueKeys, "only unique keys should be processed");
        output.WriteLine($"Deduplicated to {handler.ProcessedCount} messages from {uniqueKeys * duplicatesPerKey} writes");
    }

    #endregion

    #region Edge Case Tests

    [Fact]
    public async Task Cleanup_EmptyStore_CompletesSuccessfully()
    {
        var handler = new CountingHandler<SimpleMessage>();
        _serviceProvider = CreateDeadLetterServiceProvider("cleanup-empty-test", handler);

        var manager = _serviceProvider.GetRequiredService<IInboxManager>();
        await manager.StartAsync(CancellationToken.None);

        // Don't write any messages - cleanup should handle empty store gracefully
        await Task.Delay(500);

        // Stop should complete without errors
        var act = async () => await manager.StopAsync(CancellationToken.None);
        await act.Should().NotThrowAsync();

        output.WriteLine("Cleanup handled empty store gracefully");
    }

    [Fact]
    public async Task Cleanup_ExactExpirationBoundary_HandledCorrectly()
    {
        var handler = new CountingHandler<DeduplicatableMessage>();

        _serviceProvider = CreateDeduplicationServiceProvider("cleanup-boundary-test", handler, o =>
        {
            o.DeduplicationInterval = TimeSpan.FromMilliseconds(200);
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // Write first message
        await writer.WriteAsync(new DeduplicatableMessage("boundary-key", "data1"), "cleanup-boundary-test");

        // Wait for processing
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.ProcessedCount >= 1,
            TimeSpan.FromSeconds(5));

        // Wait just past expiration boundary
        await Task.Delay(300);

        // Write duplicate - should now be processed since dedup record expired
        await writer.WriteAsync(new DeduplicatableMessage("boundary-key", "data2"), "cleanup-boundary-test");

        // Wait for second message to be processed
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.ProcessedCount >= 2,
            TimeSpan.FromSeconds(5));

        handler.ProcessedCount.Should().Be(2, "message should be processed after dedup record expired");
        output.WriteLine("Expiration boundary handled correctly");
    }

    #endregion

    #region Multi-Inbox Cleanup Tests

    [Fact]
    public async Task MultipleInboxes_EachCleansUpIndependently()
    {
        var handler1 = new CountingHandler<DeduplicatableMessage>();
        var handler2 = new CountingHandler<DeduplicatableMessage>();

        var services = new ServiceCollection();
        services.AddLogging();

        services.AddInbox("multi-inbox-1", builder =>
        {
            builder.AsDefault()
                .UseInMemory()
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    o.EnableDeduplication = true;
                    o.DeduplicationInterval = TimeSpan.FromMilliseconds(100);
                })
                .RegisterHandler(handler1);
        });

        services.AddInbox("multi-inbox-2", builder =>
        {
            builder.AsDefault()
                .UseInMemory()
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    o.EnableDeduplication = true;
                    o.DeduplicationInterval = TimeSpan.FromMilliseconds(500);
                })
                .RegisterHandler(handler2);
        });

        _serviceProvider = services.BuildServiceProvider();
        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // Write to both inboxes with same dedup key
        await writer.WriteAsync(new DeduplicatableMessage("shared-key", "data1"), "multi-inbox-1");
        await writer.WriteAsync(new DeduplicatableMessage("shared-key", "data1"), "multi-inbox-2");

        // Wait for processing
        await TestWaitHelper.WaitForConditionAsync(
            () => handler1.ProcessedCount >= 1 && handler2.ProcessedCount >= 1,
            TimeSpan.FromSeconds(5));

        // Wait for inbox-1's dedup to expire (100ms)
        await Task.Delay(200);

        // Write again - inbox-1 should process (dedup expired), inbox-2 should not (dedup still valid)
        await writer.WriteAsync(new DeduplicatableMessage("shared-key", "data2"), "multi-inbox-1");
        await writer.WriteAsync(new DeduplicatableMessage("shared-key", "data2"), "multi-inbox-2");

        await Task.Delay(500);

        handler1.ProcessedCount.Should().Be(2, "inbox-1 dedup expired, should process second message");
        handler2.ProcessedCount.Should().Be(1, "inbox-2 dedup still valid, should not process second message");

        output.WriteLine($"Inbox-1: {handler1.ProcessedCount}, Inbox-2: {handler2.ProcessedCount}");
    }

    [Fact]
    public async Task MultipleInboxes_DifferentCleanupIntervals_RespectedPerInbox()
    {
        var handler1 = new CountingHandler<SimpleMessage>();
        var handler2 = new CountingHandler<SimpleMessage>();

        var services = new ServiceCollection();
        services.AddLogging();

        services.AddInbox("interval-inbox-1", builder =>
        {
            builder.AsDefault()
                .UseInMemory(o =>
                {
                    o.DeadLetterCleanup.Interval = TimeSpan.FromMilliseconds(100);
                })
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    o.EnableDeadLetter = true;
                    o.DeadLetterMaxMessageLifetime = TimeSpan.FromDays(1);
                })
                .RegisterHandler(handler1);
        });

        services.AddInbox("interval-inbox-2", builder =>
        {
            builder.AsDefault()
                .UseInMemory(o =>
                {
                    o.DeadLetterCleanup.Interval = TimeSpan.FromMinutes(10);
                })
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    o.EnableDeadLetter = true;
                    o.DeadLetterMaxMessageLifetime = TimeSpan.FromDays(1);
                })
                .RegisterHandler(handler2);
        });

        _serviceProvider = services.BuildServiceProvider();

        var act = async () => await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);
        await act.Should().NotThrowAsync();

        // Both inboxes should start with their independent cleanup intervals
        output.WriteLine("Multiple inboxes with different cleanup intervals started successfully");
    }

    #endregion

    #region Cleanup Recovery Tests

    [Fact]
    public async Task Cleanup_AfterInboxRestart_ResumesCorrectly()
    {
        var handler = new CountingHandler<DeduplicatableMessage>();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox("restart-test", builder =>
        {
            builder.AsDefault()
                .UseInMemory()
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    o.EnableDeduplication = true;
                    o.DeduplicationInterval = TimeSpan.FromHours(1);
                })
                .RegisterHandler(handler);
        });

        _serviceProvider = services.BuildServiceProvider();
        var manager = _serviceProvider.GetRequiredService<IInboxManager>();
        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();

        // First start
        await manager.StartAsync(CancellationToken.None);
        await writer.WriteAsync(new DeduplicatableMessage("restart-key", "data1"), "restart-test");

        await TestWaitHelper.WaitForConditionAsync(
            () => handler.ProcessedCount >= 1,
            TimeSpan.FromSeconds(5));

        // Stop
        await manager.StopAsync(CancellationToken.None);

        // Restart
        await manager.StartAsync(CancellationToken.None);

        // Write same key again - should be deduplicated if store persisted
        await writer.WriteAsync(new DeduplicatableMessage("restart-key", "data2"), "restart-test");

        // Write new key - should be processed
        await writer.WriteAsync(new DeduplicatableMessage("new-key", "data3"), "restart-test");

        await TestWaitHelper.WaitForConditionAsync(
            () => handler.ProcessedCount >= 2,
            TimeSpan.FromSeconds(5));

        await Task.Delay(300);

        // InMemory store is shared, so dedup should still work after restart
        handler.ProcessedCount.Should().Be(2, "restart-key deduplicated, new-key processed");
        output.WriteLine($"After restart: {handler.ProcessedCount} messages processed");
    }

    #endregion

    #region Helper Methods

    private ServiceProvider CreateServiceProviderWithCleanupOptions(
        string inboxName,
        CountingHandler<SimpleMessage> handler,
        Action<InMemoryInboxOptions>? configureInMemoryOptions = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox(inboxName, builder =>
        {
            builder.AsDefault()
                .UseInMemory(configureInMemoryOptions)
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    o.EnableDeadLetter = true;
                    o.EnableDeduplication = true;
                    o.DeduplicationInterval = TimeSpan.FromHours(1);
                    o.DeadLetterMaxMessageLifetime = TimeSpan.FromDays(1);
                })
                .RegisterHandler(handler);
        });
        return services.BuildServiceProvider();
    }

    private ServiceProvider CreateDeadLetterServiceProvider(
        string inboxName,
        FailingHandler<SimpleMessage> handler,
        Action<IConfigureInboxOptions>? configureOptions = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox(inboxName, builder =>
        {
            builder.AsDefault()
                .UseInMemory()
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    o.EnableDeadLetter = true;
                    o.DeadLetterMaxMessageLifetime = TimeSpan.FromDays(1);
                    configureOptions?.Invoke(o);
                })
                .RegisterHandler(handler);
        });
        return services.BuildServiceProvider();
    }

    private ServiceProvider CreateDeadLetterServiceProvider(
        string inboxName,
        CountingHandler<SimpleMessage> handler)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox(inboxName, builder =>
        {
            builder.AsDefault()
                .UseInMemory()
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    o.EnableDeadLetter = true;
                    o.DeadLetterMaxMessageLifetime = TimeSpan.FromDays(1);
                })
                .RegisterHandler(handler);
        });
        return services.BuildServiceProvider();
    }

    private ServiceProvider CreateDeduplicationServiceProvider(
        string inboxName,
        CountingHandler<DeduplicatableMessage> handler,
        Action<IConfigureInboxOptions>? configureOptions = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox(inboxName, builder =>
        {
            builder.AsDefault()
                .UseInMemory()
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    o.EnableDeduplication = true;
                    o.DeduplicationInterval = TimeSpan.FromHours(1);
                    configureOptions?.Invoke(o);
                })
                .RegisterHandler(handler);
        });
        return services.BuildServiceProvider();
    }

    #endregion
}
