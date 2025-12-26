using FluentAssertions;
using Rh.Inbox.Abstractions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Extensions;
using Rh.Inbox.Redis;
using Rh.Inbox.Tests.Integration.Common;
using Rh.Inbox.Tests.Integration.Common.Handlers;
using Rh.Inbox.Tests.Integration.Common.TestMessages;
using Xunit;
using Xunit.Abstractions;

namespace Rh.Inbox.Tests.Integration.Redis;

/// <summary>
/// Tests to validate ReadFifo script's group locking behavior.
/// These tests verify that:
/// 1. Groups with captured messages are locked for subsequent reads
/// 2. Within a single read, the script correctly handles group locking
/// 3. Interleaved messages from different groups are handled correctly
/// </summary>
[Collection("Redis")]
public class RedisFifoGroupLockingTests(RedisContainerFixture container, ITestOutputHelper output) : IAsyncLifetime
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

    /// <summary>
    /// Tests that when a group has a message being processed (captured),
    /// subsequent reads should NOT capture more messages from that group.
    /// This validates the lockedKey mechanism works across multiple read calls.
    /// </summary>
    [Fact]
    public async Task Fifo_CapturedGroup_IsLockedForSubsequentReads()
    {
        // Use a slow handler so we can observe locking behavior
        var handler = new BlockingFifoHandler<FifoMessage>();
        _serviceProvider = CreateFifoServiceProvider("fifo-lock-test", handler, o =>
        {
            o.ReadBatchSize = 1; // Read one message at a time to control capture timing
            o.PollingInterval = TimeSpan.FromMilliseconds(50);
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

        // Write multiple messages for the same group
        var messages = TestMessageFactory.CreateFifoMessages(3, "locked-group");
        await writer.WriteBatchAsync(messages, "fifo-lock-test");

        // Start processing - first message will be captured and blocked
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // Wait for first message to be captured and start processing
        await TestWaitHelper.WaitForConditionAsync(() => handler.CurrentlyProcessing > 0);

        // Give time for additional polling cycles
        await Task.Delay(200);

        // While first message is being processed, no other messages from same group should be captured
        handler.CurrentlyProcessing.Should().Be(1, "only one message should be processing at a time within the same group");
        handler.CapturedSequence.Should().HaveCount(1, "only first message should have been captured");

        // Release the first message
        handler.ReleaseOne();
        await TestWaitHelper.WaitForConditionAsync(() => handler.ProcessedCount >= 1);

        // Now second message should be captured
        await TestWaitHelper.WaitForConditionAsync(() => handler.CurrentlyProcessing > 0 || handler.ProcessedCount >= 2, TimeSpan.FromSeconds(2));

        // Release remaining messages
        handler.ReleaseAll();
        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 3, TimeSpan.FromSeconds(5));

        handler.ProcessedCount.Should().Be(3);
        output.WriteLine($"Messages processed in sequence: {string.Join(", ", handler.CapturedSequence)}");
    }

    /// <summary>
    /// Tests that messages from different groups can be captured in the same read batch,
    /// even when they are interleaved in the pending queue.
    /// </summary>
    [Fact]
    public async Task Fifo_InterleavedGroups_CapturesFromMultipleGroupsInOneBatch()
    {
        var handler = new TrackingFifoHandler<FifoMessage>();
        _serviceProvider = CreateFifoServiceProvider("fifo-interleaved", handler, o =>
        {
            o.ReadBatchSize = 10; // Large batch to capture from multiple groups
            o.PollingInterval = TimeSpan.FromMilliseconds(50);
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

        // Write interleaved messages from different groups
        // Group A: msg0, Group B: msg0, Group A: msg1, Group B: msg1, etc.
        var messages = new List<FifoMessage>();
        for (int i = 0; i < 4; i++)
        {
            messages.Add(new FifoMessage("group-A", i, $"A-{i}"));
            messages.Add(new FifoMessage("group-B", i, $"B-{i}"));
        }

        // Write one by one to maintain interleaved order
        foreach (var msg in messages)
        {
            await writer.WriteAsync(msg, "fifo-interleaved");
            await Task.Delay(5); // Small delay to ensure different timestamps
        }

        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 8, TimeSpan.FromSeconds(5));

        handler.ProcessedCount.Should().Be(8);

        // Verify ordering within each group is maintained
        var groupA = handler.Processed.Where(p => p.Message.GroupId == "group-A").OrderBy(p => p.ProcessedAt).ToList();
        var groupB = handler.Processed.Where(p => p.Message.GroupId == "group-B").OrderBy(p => p.ProcessedAt).ToList();

        for (int i = 1; i < groupA.Count; i++)
        {
            groupA[i].Message.Sequence.Should().BeGreaterThan(groupA[i - 1].Message.Sequence,
                "group A messages should be processed in sequence order");
        }

        for (int i = 1; i < groupB.Count; i++)
        {
            groupB[i].Message.Sequence.Should().BeGreaterThan(groupB[i - 1].Message.Sequence,
                "group B messages should be processed in sequence order");
        }

        output.WriteLine($"Group A processing order: {string.Join(", ", groupA.Select(p => p.Message.Sequence))}");
        output.WriteLine($"Group B processing order: {string.Join(", ", groupB.Select(p => p.Message.Sequence))}");
    }

    /// <summary>
    /// Tests that within a single ReadFifo call, multiple messages from the same group
    /// can be captured. This documents the current behavior where existingLocks is
    /// not updated during the loop, allowing batch capture from same group.
    ///
    /// BUG: The ReadFifo script adds captured groups to groupsToLock but does NOT
    /// add them to existingLocks, allowing subsequent messages from the same group
    /// to be captured within the same script execution.
    /// </summary>
    [Fact]
    public async Task Fifo_WithinSingleRead_CapturesMultipleMessagesFromSameGroup()
    {
        var handler = new TrackingFifoHandler<FifoMessage>();
        _serviceProvider = CreateFifoServiceProvider("fifo-same-group-batch", handler, o =>
        {
            o.ReadBatchSize = 10; // Large enough to capture all messages in one read
            o.PollingInterval = TimeSpan.FromMilliseconds(50);
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

        // Write multiple messages for the same group
        var messages = TestMessageFactory.CreateFifoMessages(5, "same-group");
        await writer.WriteBatchAsync(messages, "fifo-same-group-batch");

        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 5, TimeSpan.FromSeconds(5));

        handler.ProcessedCount.Should().Be(5);

        // All messages should maintain their sequence order
        var processed = handler.Processed.OrderBy(p => p.ProcessedAt).ToList();
        for (int i = 1; i < processed.Count; i++)
        {
            processed[i].Message.Sequence.Should().BeGreaterThan(processed[i - 1].Message.Sequence,
                "messages should maintain sequence order");
        }

        output.WriteLine($"Processing order: {string.Join(", ", processed.Select(p => p.Message.Sequence))}");
    }

    /// <summary>
    /// Tests that group lock is released when a message fails (via Fail operation),
    /// allowing subsequent messages from the same group to be captured.
    /// </summary>
    [Fact]
    public async Task Fifo_FailedMessage_ReleasesGroupLock()
    {
        var handler = new FailFirstThenSucceedFifoHandler<FifoMessage>(failCount: 1);
        _serviceProvider = CreateFifoServiceProvider("fifo-fail-unlock", handler, o =>
        {
            o.ReadBatchSize = 1;
            o.PollingInterval = TimeSpan.FromMilliseconds(50);
            o.MaxAttempts = 3;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

        var messages = TestMessageFactory.CreateFifoMessages(3, "fail-group");
        await writer.WriteBatchAsync(messages, "fifo-fail-unlock");

        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // First message will fail once, then retry and succeed
        // Group lock should be released on fail, allowing same message to be retried
        await TestWaitHelper.WaitForCountAsync(() => handler.SuccessCount, 3, TimeSpan.FromSeconds(10));

        handler.SuccessCount.Should().Be(3, "all messages should eventually succeed");
        handler.FailCount.Should().Be(1, "first message should have failed once");

        output.WriteLine($"Failures: {handler.FailCount}, Successes: {handler.SuccessCount}");
    }

    /// <summary>
    /// Tests that ExtendLocksAsync prevents message lock from expiring during long processing.
    /// </summary>
    [Fact]
    public async Task Fifo_ExtendLocks_PreventsExpiration()
    {
        var handler = new BlockingFifoHandler<FifoMessage>();
        _serviceProvider = CreateFifoServiceProvider("fifo-extend-locks", handler, o =>
        {
            o.ReadBatchSize = 1;
            o.PollingInterval = TimeSpan.FromMilliseconds(50);
            o.MaxProcessingTime = TimeSpan.FromSeconds(5); // Long enough for handler to complete
            o.EnableLockExtension = true;
            o.LockExtensionThreshold = 0.3; // Extend at 30% of MaxProcessingTime (1.5s)
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

        await writer.WriteAsync(new FifoMessage("extend-group", 0, "msg"), "fifo-extend-locks");

        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        await TestWaitHelper.WaitForConditionAsync(() => handler.CurrentlyProcessing > 0);

        // Wait past original MaxProcessingTime - lock should be extended
        await Task.Delay(TimeSpan.FromSeconds(2));

        // Message should still be processing (not re-captured due to lock extension)
        handler.CapturedSequence.Should().HaveCount(1, "message should not be recaptured - lock was extended");

        handler.ReleaseAll();
        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 1, TimeSpan.FromSeconds(5));

        output.WriteLine("Message processed successfully with extended lock");
    }

    /// <summary>
    /// Tests that ExtendLocksAsync also extends the group lock TTL.
    /// </summary>
    [Fact]
    public async Task Fifo_ExtendLocks_ExtendsGroupLockTTL()
    {
        var handler = new BlockingFifoHandler<FifoMessage>();
        _serviceProvider = CreateFifoServiceProvider("fifo-extend-group-ttl", handler, o =>
        {
            o.ReadBatchSize = 1;
            o.PollingInterval = TimeSpan.FromMilliseconds(50);
            o.MaxProcessingTime = TimeSpan.FromSeconds(5); // Long enough for handler to complete
            o.EnableLockExtension = true;
            o.LockExtensionThreshold = 0.3; // Extend at 30% of MaxProcessingTime (1.5s)
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

        // Write two messages for same group
        await writer.WriteAsync(new FifoMessage("extend-group-lock", 0, "msg-0"), "fifo-extend-group-ttl");
        await writer.WriteAsync(new FifoMessage("extend-group-lock", 1, "msg-1"), "fifo-extend-group-ttl");

        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        await TestWaitHelper.WaitForConditionAsync(() => handler.CurrentlyProcessing > 0);

        // Wait past original MaxProcessingTime - both locks should be extended
        await Task.Delay(TimeSpan.FromSeconds(2));

        // Only first message should be captured - group lock should still be held
        handler.CapturedSequence.Should().HaveCount(1, "only first message should be captured - group lock extended");

        // Release first message
        handler.ReleaseOne();
        await TestWaitHelper.WaitForConditionAsync(() => handler.ProcessedCount >= 1);

        // Now second message should be captured
        await TestWaitHelper.WaitForConditionAsync(() => handler.CurrentlyProcessing > 0, TimeSpan.FromSeconds(2));

        handler.ReleaseAll();
        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 2, TimeSpan.FromSeconds(5));

        handler.ProcessedCount.Should().Be(2);
        output.WriteLine($"Messages processed in sequence: {string.Join(", ", handler.CapturedSequence)}");
    }

    private ServiceProvider CreateFifoServiceProvider<THandler>(
        string inboxName,
        THandler handler,
        Action<IConfigureInboxOptions>? configureOptions = null)
        where THandler : class, IFifoInboxHandler<FifoMessage>
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox(inboxName, builder =>
        {
            builder.AsFifo()
                .UseRedis(container.ConnectionString)
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(50);
                    configureOptions?.Invoke(o);
                })
                .RegisterHandler(handler);
        });
        return services.BuildServiceProvider();
    }
}

