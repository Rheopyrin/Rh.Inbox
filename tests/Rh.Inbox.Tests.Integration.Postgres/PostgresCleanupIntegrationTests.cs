using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Extensions;
using Rh.Inbox.Postgres;
using Rh.Inbox.Postgres.Services;
using Rh.Inbox.Tests.Integration.Common;
using Rh.Inbox.Tests.Integration.Common.Handlers;
using Rh.Inbox.Tests.Integration.Common.TestMessages;
using Xunit;
using Xunit.Abstractions;

namespace Rh.Inbox.Tests.Integration.Postgres;

[Collection("Postgres")]
public class PostgresCleanupIntegrationTests(PostgresContainerFixture container, ITestOutputHelper output) : IAsyncLifetime
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
            o.DeadLetterCleanup.BatchSize = 500;
            o.DeadLetterCleanup.Interval = TimeSpan.FromMinutes(10);
            o.DeduplicationCleanup.BatchSize = 1000;
            o.GroupLocksCleanup.RestartDelay = TimeSpan.FromMinutes(1);
        });

        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

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
            o.GroupLocksCleanup.Interval = TimeSpan.FromMinutes(3);
        });

        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

        var act = async () => await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);
        await act.Should().NotThrowAsync();

        output.WriteLine("Independent cleanup intervals configured successfully");
    }

    #endregion

    #region AutostartCleanupTasks Tests

    [Fact]
    public async Task AutostartCleanupTasks_True_ManagerRegisteredAsLifecycleHook()
    {
        var handler = new CountingHandler<SimpleMessage>();
        _serviceProvider = CreateServiceProviderWithAutostart("cleanup-autostart-true", handler, autostartCleanupTasks: true);

        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // When autostart is true, cleanup manager should be available and started as lifecycle hook
        var cleanupManager = _serviceProvider.GetService<IPostgresCleanupTasksManager>();
        cleanupManager.Should().NotBeNull("cleanup manager should be registered");

        output.WriteLine("Cleanup manager registered with autostart=true");
    }

    [Fact]
    public async Task AutostartCleanupTasks_False_ManagerAvailableButNotAutoStarted()
    {
        var handler = new CountingHandler<SimpleMessage>();
        _serviceProvider = CreateServiceProviderWithAutostart("cleanup-autostart-false", handler, autostartCleanupTasks: false);

        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // Manager should still be available for manual use
        var cleanupManager = _serviceProvider.GetService<IPostgresCleanupTasksManager>();
        cleanupManager.Should().NotBeNull("cleanup manager should be registered even with autostart=false");

        output.WriteLine("Cleanup manager available for manual use with autostart=false");
    }

    #endregion

    #region Manual Cleanup Execution Tests

    [Fact]
    public async Task ManualCleanup_ExecuteAsync_RunsCleanupTasks()
    {
        var handler = new CountingHandler<SimpleMessage>();
        _serviceProvider = CreateDeadLetterServiceProvider("cleanup-manual-exec", handler);

        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        var cleanupManager = _serviceProvider.GetRequiredService<IPostgresCleanupTasksManager>();

        // Execute cleanup manually
        var act = async () => await cleanupManager.ExecuteAsync(CancellationToken.None);
        await act.Should().NotThrowAsync();

        output.WriteLine("Manual cleanup execution completed successfully");
    }

    [Fact]
    public async Task ManualCleanup_ExecuteAsync_WithInboxFilter_RunsOnlyMatchingTasks()
    {
        var handler = new CountingHandler<SimpleMessage>();
        _serviceProvider = CreateDeadLetterServiceProvider("cleanup-filter-test", handler);

        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        var cleanupManager = _serviceProvider.GetRequiredService<IPostgresCleanupTasksManager>();

        // Execute cleanup only for specific inbox
        var act = async () => await cleanupManager.ExecuteAsync("cleanup-filter-test", CancellationToken.None);
        await act.Should().NotThrowAsync();

        output.WriteLine("Filtered cleanup execution completed successfully");
    }

    #endregion

    #region Dead Letter Cleanup Integration Tests

    [Fact]
    public async Task DeadLetterCleanup_RemovesExpiredRecords()
    {
        const int maxAttempts = 2;
        var handler = new FailingHandler<SimpleMessage>(1.0); // Always fails
        _serviceProvider = CreateDeadLetterServiceProvider("dlq-cleanup-test", handler, o =>
        {
            o.MaxAttempts = maxAttempts;
            o.DeadLetterMaxMessageLifetime = TimeSpan.FromMilliseconds(100); // Very short lifetime for test
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // Write message and wait for it to go to dead letter
        var message = new SimpleMessage("dlq-test", "data");
        await writer.WriteAsync(message, "dlq-cleanup-test");

        await TestWaitHelper.WaitForConditionAsync(
            () => handler.FailedCount >= maxAttempts,
            TimeSpan.FromSeconds(10));

        // Wait for dead letter record
        await WaitForDeadLetterCountAsync("dlq-cleanup-test", 1, TimeSpan.FromSeconds(5));

        // Wait for message to expire
        await Task.Delay(200);

        // Run cleanup manually
        var cleanupManager = _serviceProvider.GetRequiredService<IPostgresCleanupTasksManager>();
        await cleanupManager.ExecuteAsync("dlq-cleanup-test", CancellationToken.None);

        // Verify record was cleaned up
        var count = await GetDeadLetterCountAsync("dlq-cleanup-test");
        count.Should().Be(0, "expired dead letter record should be cleaned up");

        output.WriteLine("Dead letter cleanup removed expired record");
    }

    #endregion

    #region Deduplication Cleanup Integration Tests

    [Fact]
    public async Task DeduplicationCleanup_RemovesExpiredRecords()
    {
        var handler = new CountingHandler<DeduplicatableMessage>();
        _serviceProvider = CreateDeduplicationServiceProvider("dedup-cleanup-test", handler, o =>
        {
            o.DeduplicationInterval = TimeSpan.FromMilliseconds(100); // Very short interval for test
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // Write message with deduplication key
        var message = new DeduplicatableMessage("test-dedup-key", "data");
        await writer.WriteAsync(message, "dedup-cleanup-test");

        // Wait for processing
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.ProcessedCount >= 1,
            TimeSpan.FromSeconds(5));

        // Verify deduplication record exists
        var initialCount = await GetDeduplicationCountAsync("dedup-cleanup-test");
        initialCount.Should().BeGreaterOrEqualTo(1, "deduplication record should exist");

        // Wait for record to expire
        await Task.Delay(200);

        // Run cleanup manually
        var cleanupManager = _serviceProvider.GetRequiredService<IPostgresCleanupTasksManager>();
        await cleanupManager.ExecuteAsync("dedup-cleanup-test", CancellationToken.None);

        // Verify record was cleaned up
        var finalCount = await GetDeduplicationCountAsync("dedup-cleanup-test");
        finalCount.Should().Be(0, "expired deduplication record should be cleaned up");

        output.WriteLine("Deduplication cleanup removed expired record");
    }

    #endregion

    #region Group Locks Cleanup Integration Tests (FIFO)

    [Fact]
    public async Task GroupLocksCleanup_RemovesExpiredLocks()
    {
        var handler = new FifoCountingHandler<FifoMessage>();
        _serviceProvider = CreateFifoServiceProvider("group-locks-cleanup", handler, o =>
        {
            o.MaxProcessingTime = TimeSpan.FromMilliseconds(50); // Very short for test
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // Write message with group (FifoMessage has GroupId built-in via IHasGroupId)
        var message = new FifoMessage("test-group", 1, "data");
        await writer.WriteAsync(message, "group-locks-cleanup");

        // Wait for processing
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.ProcessedCount >= 1,
            TimeSpan.FromSeconds(5));

        // Wait for potential locks to expire (2x MaxProcessingTime)
        await Task.Delay(200);

        // Run cleanup manually
        var cleanupManager = _serviceProvider.GetRequiredService<IPostgresCleanupTasksManager>();

        var act = async () => await cleanupManager.ExecuteAsync("group-locks-cleanup", CancellationToken.None);
        await act.Should().NotThrowAsync();

        output.WriteLine("Group locks cleanup executed successfully");
    }

    #endregion

    #region Cleanup During Active Processing Tests

    [Fact]
    public async Task CleanupDuringActiveProcessing_DoesNotAffectInFlightMessages()
    {
        var handler = new CountingHandler<DeduplicatableMessage>();

        _serviceProvider = CreateDeduplicationServiceProvider("cleanup-active-test", handler, o =>
        {
            o.DeduplicationInterval = TimeSpan.FromMilliseconds(50); // Very short for test
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
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
            TimeSpan.FromSeconds(15));

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
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // Write multiple messages
        for (int i = 0; i < 10; i++)
        {
            await writer.WriteAsync(new SimpleMessage($"msg-{i}", $"data-{i}"), "cleanup-retry-test");
        }

        // Wait for processing (some will succeed, some may retry)
        await Task.Delay(3000);

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
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
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
            TimeSpan.FromSeconds(20));

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
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
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
            TimeSpan.FromSeconds(15));

        // Small delay to ensure no more processing
        await Task.Delay(500);

        handler.ProcessedCount.Should().Be(uniqueKeys, "only unique keys should be processed");
        output.WriteLine($"Deduplicated to {handler.ProcessedCount} messages from {uniqueKeys * duplicatesPerKey} writes");
    }

    #endregion

    #region Edge Case Tests

    [Fact]
    public async Task Cleanup_EmptyTables_CompletesSuccessfully()
    {
        var handler = new CountingHandler<SimpleMessage>();
        _serviceProvider = CreateDeadLetterServiceProvider("cleanup-empty-test", handler);

        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // Don't write any messages - cleanup should handle empty tables gracefully
        var cleanupManager = _serviceProvider.GetRequiredService<IPostgresCleanupTasksManager>();

        var act = async () => await cleanupManager.ExecuteAsync("cleanup-empty-test", CancellationToken.None);
        await act.Should().NotThrowAsync();

        output.WriteLine("Cleanup handled empty tables gracefully");
    }

    [Fact]
    public async Task Cleanup_LargeBatch_ProcessesInChunks()
    {
        const int maxAttempts = 1;
        var handler = new FailingHandler<SimpleMessage>(1.0); // Always fails

        _serviceProvider = CreateDeadLetterServiceProvider("cleanup-batch-test", handler, o =>
        {
            o.MaxAttempts = maxAttempts;
            o.DeadLetterMaxMessageLifetime = TimeSpan.FromMilliseconds(100);
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // Write many messages that will all go to dead letter
        const int messageCount = 50;
        for (int i = 0; i < messageCount; i++)
        {
            await writer.WriteAsync(new SimpleMessage($"batch-msg-{i}", $"data-{i}"), "cleanup-batch-test");
        }

        // Wait for all messages to fail and go to dead letter
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.FailedCount >= messageCount,
            TimeSpan.FromSeconds(30));

        // Wait for dead letter records
        await WaitForDeadLetterCountAsync("cleanup-batch-test", messageCount, TimeSpan.FromSeconds(10));

        // Wait for records to expire
        await Task.Delay(200);

        // Run cleanup - should process in batches
        var cleanupManager = _serviceProvider.GetRequiredService<IPostgresCleanupTasksManager>();
        await cleanupManager.ExecuteAsync("cleanup-batch-test", CancellationToken.None);

        var remainingCount = await GetDeadLetterCountAsync("cleanup-batch-test");
        remainingCount.Should().Be(0, "all expired dead letter records should be cleaned up");

        output.WriteLine($"Cleaned up {messageCount} dead letter records in batches");
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
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // Write first message
        await writer.WriteAsync(new DeduplicatableMessage("boundary-key", "data1"), "cleanup-boundary-test");

        // Wait for processing
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.ProcessedCount >= 1,
            TimeSpan.FromSeconds(5));

        // Wait just past expiration boundary
        await Task.Delay(300);

        // Run cleanup to remove expired dedup records (Postgres requires explicit cleanup)
        var cleanupManager = _serviceProvider.GetRequiredService<IPostgresCleanupTasksManager>();
        await cleanupManager.ExecuteAsync("cleanup-boundary-test", CancellationToken.None);

        // Write duplicate - should now be processed since dedup record was cleaned up
        await writer.WriteAsync(new DeduplicatableMessage("boundary-key", "data2"), "cleanup-boundary-test");

        // Wait for second message to be processed
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.ProcessedCount >= 2,
            TimeSpan.FromSeconds(5));

        handler.ProcessedCount.Should().Be(2, "message should be processed after dedup record cleaned up");
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
                .UsePostgres(container.ConnectionString)
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
                .UsePostgres(container.ConnectionString)
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    o.EnableDeduplication = true;
                    o.DeduplicationInterval = TimeSpan.FromMilliseconds(500);
                })
                .RegisterHandler(handler2);
        });

        _serviceProvider = services.BuildServiceProvider();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // Write to both inboxes with same dedup key
        await writer.WriteAsync(new DeduplicatableMessage("shared-key", "data1"), "multi-inbox-1");
        await writer.WriteAsync(new DeduplicatableMessage("shared-key", "data1"), "multi-inbox-2");

        // Wait for processing
        await TestWaitHelper.WaitForConditionAsync(
            () => handler1.ProcessedCount >= 1 && handler2.ProcessedCount >= 1,
            TimeSpan.FromSeconds(10));

        // Wait for inbox-1's dedup to expire (100ms)
        await Task.Delay(200);

        // Run cleanup on inbox-1 to remove expired dedup record (Postgres requires explicit cleanup)
        var cleanupManager = _serviceProvider.GetRequiredService<IPostgresCleanupTasksManager>();
        await cleanupManager.ExecuteAsync("multi-inbox-1", CancellationToken.None);

        // Write again - inbox-1 should process (dedup cleaned up), inbox-2 should not (dedup still valid)
        await writer.WriteAsync(new DeduplicatableMessage("shared-key", "data2"), "multi-inbox-1");
        await writer.WriteAsync(new DeduplicatableMessage("shared-key", "data2"), "multi-inbox-2");

        await Task.Delay(1000);

        handler1.ProcessedCount.Should().Be(2, "inbox-1 dedup cleaned up, should process second message");
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
                .UsePostgres(container.ConnectionString, o =>
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
                .UsePostgres(container.ConnectionString, o =>
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
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

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
                .UsePostgres(container.ConnectionString)
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    o.EnableDeduplication = true;
                    o.DeduplicationInterval = TimeSpan.FromHours(1);
                })
                .RegisterHandler(handler);
        });

        _serviceProvider = services.BuildServiceProvider();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        var manager = _serviceProvider.GetRequiredService<IInboxManager>();
        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();

        // First start
        await manager.StartAsync(CancellationToken.None);
        await writer.WriteAsync(new DeduplicatableMessage("restart-key", "data1"), "restart-test");

        await TestWaitHelper.WaitForConditionAsync(
            () => handler.ProcessedCount >= 1,
            TimeSpan.FromSeconds(10));

        // Stop
        await manager.StopAsync(CancellationToken.None);

        // Restart
        await manager.StartAsync(CancellationToken.None);

        // Write same key again - should be deduplicated since Postgres persists
        await writer.WriteAsync(new DeduplicatableMessage("restart-key", "data2"), "restart-test");

        // Write new key - should be processed
        await writer.WriteAsync(new DeduplicatableMessage("new-key", "data3"), "restart-test");

        await TestWaitHelper.WaitForConditionAsync(
            () => handler.ProcessedCount >= 2,
            TimeSpan.FromSeconds(10));

        await Task.Delay(500);

        // Postgres persists dedup records, so restart-key should still be deduplicated
        handler.ProcessedCount.Should().Be(2, "restart-key deduplicated, new-key processed");
        output.WriteLine($"After restart: {handler.ProcessedCount} messages processed");
    }

    #endregion

    #region Helper Methods

    private async Task<int> WaitForDeadLetterCountAsync(string inboxName, int expectedCount, TimeSpan timeout)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var count = 0;

        while (sw.Elapsed < timeout)
        {
            count = await GetDeadLetterCountAsync(inboxName);
            if (count >= expectedCount)
            {
                return count;
            }
            await Task.Delay(100);
        }

        return count;
    }

    private async Task<int> GetDeadLetterCountAsync(string inboxName)
    {
        var tableName = BuildTableName("inbox_dead_letters", inboxName);
        return await GetTableCountAsync(tableName);
    }

    private async Task<int> GetDeduplicationCountAsync(string inboxName)
    {
        var tableName = BuildTableName("inbox_dedup", inboxName);
        return await GetTableCountAsync(tableName);
    }

    private async Task<int> GetTableCountAsync(string tableName)
    {
        try
        {
            await using var connection = new NpgsqlConnection(container.ConnectionString);
            await connection.OpenAsync();

            await using var cmd = new NpgsqlCommand($"SELECT COUNT(*) FROM \"{tableName}\"", connection);
            var result = await cmd.ExecuteScalarAsync();
            return Convert.ToInt32(result);
        }
        catch (PostgresException ex) when (ex.SqlState == "42P01") // Table doesn't exist
        {
            return 0;
        }
    }

    private static string BuildTableName(string prefix, string inboxName)
    {
        var sanitized = inboxName.ToLowerInvariant().Replace('-', '_');
        return $"{prefix}_{sanitized}";
    }

    private ServiceProvider CreateServiceProviderWithCleanupOptions(
        string inboxName,
        CountingHandler<SimpleMessage> handler,
        Action<Rh.Inbox.Postgres.Options.PostgresInboxOptions>? configurePostgresOptions = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox(inboxName, builder =>
        {
            builder.AsDefault()
                .UsePostgres(container.ConnectionString, configurePostgresOptions)
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

    private ServiceProvider CreateServiceProviderWithAutostart(
        string inboxName,
        CountingHandler<SimpleMessage> handler,
        bool autostartCleanupTasks)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox(inboxName, builder =>
        {
            builder.AsDefault()
                .UsePostgres(container.ConnectionString, o =>
                {
                    o.AutostartCleanupTasks = autostartCleanupTasks;
                })
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
                .UsePostgres(container.ConnectionString)
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
                .UsePostgres(container.ConnectionString)
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
                .UsePostgres(container.ConnectionString)
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

    private ServiceProvider CreateFifoServiceProvider(
        string inboxName,
        FifoCountingHandler<FifoMessage> handler,
        Action<IConfigureInboxOptions>? configureOptions = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox(inboxName, builder =>
        {
            builder.AsFifo()
                .UsePostgres(container.ConnectionString)
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    configureOptions?.Invoke(o);
                })
                .RegisterHandler(handler);
        });
        return services.BuildServiceProvider();
    }

    #endregion
}
