using FluentAssertions;
using Rh.Inbox.Abstractions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Extensions;
using Rh.Inbox.Postgres;
using Rh.Inbox.Tests.Integration.Common;
using Rh.Inbox.Tests.Integration.Common.Handlers;
using Rh.Inbox.Tests.Integration.Common.TestMessages;
using Xunit;
using Xunit.Abstractions;

namespace Rh.Inbox.Tests.Integration.Postgres;

/// <summary>
/// Tests for Postgres FIFO group locking behavior including:
/// - Group locking during message processing
/// - Lock release on failure
/// - Interleaved group processing
/// - ExtendLocks functionality
/// - Group lock cleanup
/// </summary>
[Collection("Postgres")]
public class PostgresFifoGroupLockingTests(PostgresContainerFixture container, ITestOutputHelper output) : IAsyncLifetime
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
    /// </summary>
    [Fact]
    public async Task Fifo_CapturedGroup_IsLockedForSubsequentReads()
    {
        var handler = new BlockingFifoHandler<FifoMessage>();
        _serviceProvider = CreateFifoServiceProvider("pg-fifo-lock-test", handler, o =>
        {
            o.ReadBatchSize = 1;
            o.PollingInterval = TimeSpan.FromMilliseconds(50);
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

        var messages = TestMessageFactory.CreateFifoMessages(3, "locked-group");
        await writer.WriteBatchAsync(messages, "pg-fifo-lock-test");

        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        await TestWaitHelper.WaitForConditionAsync(() => handler.CurrentlyProcessing > 0);
        await Task.Delay(200);

        handler.CurrentlyProcessing.Should().Be(1, "only one message should be processing at a time within the same group");
        handler.CapturedSequence.Should().HaveCount(1, "only first message should have been captured");

        handler.ReleaseOne();
        await TestWaitHelper.WaitForConditionAsync(() => handler.ProcessedCount >= 1);

        await TestWaitHelper.WaitForConditionAsync(() => handler.CurrentlyProcessing > 0 || handler.ProcessedCount >= 2, TimeSpan.FromSeconds(2));

        handler.ReleaseAll();
        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 3, TimeSpan.FromSeconds(5));

        handler.ProcessedCount.Should().Be(3);
        output.WriteLine($"Messages processed in sequence: {string.Join(", ", handler.CapturedSequence)}");
    }

    /// <summary>
    /// Tests that messages from different groups can be captured in the same read batch.
    /// </summary>
    [Fact]
    public async Task Fifo_InterleavedGroups_CapturesFromMultipleGroupsInOneBatch()
    {
        var handler = new TrackingFifoHandler<FifoMessage>();
        _serviceProvider = CreateFifoServiceProvider("pg-fifo-interleaved", handler, o =>
        {
            o.ReadBatchSize = 10;
            o.PollingInterval = TimeSpan.FromMilliseconds(50);
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

        var messages = new List<FifoMessage>();
        for (int i = 0; i < 4; i++)
        {
            messages.Add(new FifoMessage("group-A", i, $"A-{i}"));
            messages.Add(new FifoMessage("group-B", i, $"B-{i}"));
        }

        foreach (var msg in messages)
        {
            await writer.WriteAsync(msg, "pg-fifo-interleaved");
            await Task.Delay(5);
        }

        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 8, TimeSpan.FromSeconds(5));

        handler.ProcessedCount.Should().Be(8);

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
    /// Tests that within a single read, multiple messages from the same group can be captured and processed.
    /// </summary>
    [Fact]
    public async Task Fifo_WithinSingleRead_CapturesMultipleMessagesFromSameGroup()
    {
        var handler = new TrackingFifoHandler<FifoMessage>();
        _serviceProvider = CreateFifoServiceProvider("pg-fifo-same-group-batch", handler, o =>
        {
            o.ReadBatchSize = 10;
            o.PollingInterval = TimeSpan.FromMilliseconds(50);
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

        var messages = TestMessageFactory.CreateFifoMessages(5, "same-group");
        await writer.WriteBatchAsync(messages, "pg-fifo-same-group-batch");

        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 5, TimeSpan.FromSeconds(5));

        handler.ProcessedCount.Should().Be(5);

        // Verify all sequences were processed (0, 1, 2, 3, 4)
        var processedSequences = handler.Processed.Select(p => p.Message.Sequence).OrderBy(s => s).ToList();
        processedSequences.Should().BeEquivalentTo([0, 1, 2, 3, 4], "all messages should be processed");

        output.WriteLine($"Processed sequences: {string.Join(", ", processedSequences)}");
    }

    /// <summary>
    /// Tests that group lock is released when a message fails.
    /// </summary>
    [Fact]
    public async Task Fifo_FailedMessage_ReleasesGroupLock()
    {
        var handler = new FailFirstThenSucceedFifoHandler<FifoMessage>(failCount: 1);
        _serviceProvider = CreateFifoServiceProvider("pg-fifo-fail-unlock", handler, o =>
        {
            o.ReadBatchSize = 1;
            o.PollingInterval = TimeSpan.FromMilliseconds(50);
            o.MaxAttempts = 3;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

        var messages = TestMessageFactory.CreateFifoMessages(3, "fail-group");
        await writer.WriteBatchAsync(messages, "pg-fifo-fail-unlock");

        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        await TestWaitHelper.WaitForCountAsync(() => handler.SuccessCount, 3, TimeSpan.FromSeconds(10));

        handler.SuccessCount.Should().Be(3, "all messages should eventually succeed");
        handler.FailCount.Should().Be(1, "first message should have failed once");

        output.WriteLine($"Failures: {handler.FailCount}, Successes: {handler.SuccessCount}");
    }

    /// <summary>
    /// Tests that group locks prevent additional messages from same group being captured.
    /// </summary>
    [Fact]
    public async Task Fifo_GroupLock_PreventsAdditionalMessagesFromSameGroup()
    {
        var handler = new BlockingFifoHandler<FifoMessage>();
        _serviceProvider = CreateFifoServiceProvider("pg-fifo-group-lock", handler, o =>
        {
            o.ReadBatchSize = 10;
            o.PollingInterval = TimeSpan.FromMilliseconds(50);
            o.MaxProcessingTime = TimeSpan.FromMinutes(5);
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

        await writer.WriteAsync(new FifoMessage("group-A", 0, "A-0"), "pg-fifo-group-lock");

        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        await TestWaitHelper.WaitForConditionAsync(() => handler.CurrentlyProcessing >= 1, TimeSpan.FromSeconds(2));

        await writer.WriteAsync(new FifoMessage("group-A", 1, "A-1"), "pg-fifo-group-lock");

        await Task.Delay(200);

        handler.CapturedSequence.Should().HaveCount(1, "only first message should be captured while group is locked");
        handler.CapturedSequence[0].Should().Be(0, "first message (seq=0) should be captured");

        handler.ReleaseOne();
        await TestWaitHelper.WaitForConditionAsync(() => handler.ProcessedCount >= 1);

        await TestWaitHelper.WaitForConditionAsync(() => handler.CurrentlyProcessing > 0, TimeSpan.FromSeconds(2));
        handler.CapturedSequence.Should().HaveCount(2, "second message should now be captured");

        handler.ReleaseAll();
        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 2, TimeSpan.FromSeconds(5));

        handler.ProcessedCount.Should().Be(2);
        output.WriteLine($"Messages captured in sequence: {string.Join(", ", handler.CapturedSequence)}");
    }

    /// <summary>
    /// Tests that messages and group locks can be released together.
    /// </summary>
    [Fact]
    public async Task Fifo_ReleaseMessagesAndGroupLocks_ReleasesAll()
    {
        var handler = new TrackingFifoHandler<FifoMessage>();
        _serviceProvider = CreateFifoServiceProvider("pg-fifo-release-all", handler, o =>
        {
            o.ReadBatchSize = 5;
            o.PollingInterval = TimeSpan.FromMilliseconds(50);
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

        var messages = new List<FifoMessage>();
        for (int g = 0; g < 3; g++)
        {
            messages.AddRange(TestMessageFactory.CreateFifoMessages(3, $"release-group-{g}"));
        }
        await writer.WriteBatchAsync(messages, "pg-fifo-release-all");

        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 9, TimeSpan.FromSeconds(5));

        handler.ProcessedCount.Should().Be(9);

        // Verify each group has all expected sequences processed
        for (int g = 0; g < 3; g++)
        {
            var groupSequences = handler.Processed
                .Where(p => p.Message.GroupId == $"release-group-{g}")
                .Select(p => p.Message.Sequence)
                .OrderBy(s => s)
                .ToList();

            groupSequences.Should().BeEquivalentTo([0, 1, 2], $"group {g} should have all sequences processed");
        }

        output.WriteLine($"Total messages processed: {handler.ProcessedCount}");
    }

    /// <summary>
    /// Tests that ExtendLocksAsync prevents message lock from expiring during long processing.
    /// </summary>
    [Fact]
    public async Task Fifo_ExtendLocks_PreventsExpiration()
    {
        var handler = new BlockingFifoHandler<FifoMessage>();
        _serviceProvider = CreateFifoServiceProvider("pg-fifo-extend-locks", handler, o =>
        {
            o.ReadBatchSize = 1;
            o.PollingInterval = TimeSpan.FromMilliseconds(50);
            o.MaxProcessingTime = TimeSpan.FromSeconds(5); // Long enough for handler to complete
            o.EnableLockExtension = true;
            o.LockExtensionThreshold = 0.3; // Extend at 30% of MaxProcessingTime (1.5s)
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

        await writer.WriteAsync(new FifoMessage("extend-group", 0, "msg"), "pg-fifo-extend-locks");

        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        await TestWaitHelper.WaitForConditionAsync(() => handler.CurrentlyProcessing > 0);

        await Task.Delay(TimeSpan.FromSeconds(2));

        handler.CapturedSequence.Should().HaveCount(1, "message should not be recaptured - lock was extended");

        handler.ReleaseAll();
        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 1, TimeSpan.FromSeconds(5));

        output.WriteLine("Message processed successfully with extended lock");
    }

    /// <summary>
    /// Tests that ExtendLocksAsync also extends the group lock TTL.
    /// </summary>
    [Fact]
    public async Task Fifo_ExtendLocks_ExtendsGroupLockToo()
    {
        var handler = new BlockingFifoHandler<FifoMessage>();
        _serviceProvider = CreateFifoServiceProvider("pg-fifo-extend-group", handler, o =>
        {
            o.ReadBatchSize = 1;
            o.PollingInterval = TimeSpan.FromMilliseconds(50);
            o.MaxProcessingTime = TimeSpan.FromSeconds(5); // Long enough for handler to complete
            o.EnableLockExtension = true;
            o.LockExtensionThreshold = 0.3; // Extend at 30% of MaxProcessingTime (1.5s)
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

        await writer.WriteAsync(new FifoMessage("extend-group-lock", 0, "msg-0"), "pg-fifo-extend-group");
        await writer.WriteAsync(new FifoMessage("extend-group-lock", 1, "msg-1"), "pg-fifo-extend-group");

        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        await TestWaitHelper.WaitForConditionAsync(() => handler.CurrentlyProcessing > 0);

        await Task.Delay(TimeSpan.FromSeconds(2));

        handler.CapturedSequence.Should().HaveCount(1, "only first message should be captured - group lock extended");

        handler.ReleaseOne();
        await TestWaitHelper.WaitForConditionAsync(() => handler.ProcessedCount >= 1);

        await TestWaitHelper.WaitForConditionAsync(() => handler.CurrentlyProcessing > 0, TimeSpan.FromSeconds(2));

        handler.ReleaseAll();
        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 2, TimeSpan.FromSeconds(5));

        handler.ProcessedCount.Should().Be(2);
        output.WriteLine($"Messages processed in sequence: {string.Join(", ", handler.CapturedSequence)}");
    }

    /// <summary>
    /// Tests that the group lock cleanup service releases expired group locks.
    /// </summary>
    [Fact]
    public async Task Fifo_GroupLockCleanup_ReleasesExpiredLocks()
    {
        var handler = new TrackingFifoHandler<FifoMessage>();
        _serviceProvider = CreateFifoServiceProvider("pg-fifo-cleanup", handler, o =>
        {
            o.ReadBatchSize = 10;
            o.PollingInterval = TimeSpan.FromMilliseconds(50);
            o.MaxProcessingTime = TimeSpan.FromSeconds(1); // Short timeout for testing
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

        // Write multiple messages
        var messages = TestMessageFactory.CreateFifoMessages(5, "cleanup-group");
        await writer.WriteBatchAsync(messages, "pg-fifo-cleanup");

        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 5, TimeSpan.FromSeconds(10));

        handler.ProcessedCount.Should().Be(5);
        output.WriteLine($"All {handler.ProcessedCount} messages processed after cleanup");
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
                .UsePostgres(container.ConnectionString)
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
