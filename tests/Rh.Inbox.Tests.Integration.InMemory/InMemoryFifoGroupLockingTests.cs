using FluentAssertions;
using Rh.Inbox.Abstractions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Extensions;
using Rh.Inbox.InMemory;
using Rh.Inbox.Tests.Integration.Common;
using Rh.Inbox.Tests.Integration.Common.Handlers;
using Rh.Inbox.Tests.Integration.Common.TestMessages;
using Xunit;
using Xunit.Abstractions;

namespace Rh.Inbox.Tests.Integration.InMemory;

/// <summary>
/// Tests for InMemory FIFO group locking behavior including:
/// - Group locking during message processing
/// - Lock release on failure
/// - Explicit API: ReleaseGroupLocks, ReleaseMessagesAndGroupLocks
/// - ExtendLocks functionality
/// </summary>
public class InMemoryFifoGroupLockingTests(ITestOutputHelper output) : IAsyncLifetime
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
        _serviceProvider = CreateFifoServiceProvider("inmem-fifo-lock-test", handler, o =>
        {
            o.ReadBatchSize = 1;
            o.PollingInterval = TimeSpan.FromMilliseconds(50);
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        var messages = TestMessageFactory.CreateFifoMessages(3, "locked-group");
        await writer.WriteBatchAsync(messages, "inmem-fifo-lock-test");

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
    /// Tests that group lock is released when a message fails (via Fail operation),
    /// allowing subsequent messages from the same group to be captured.
    /// </summary>
    [Fact]
    public async Task Fifo_FailedMessage_ReleasesGroupLock()
    {
        var handler = new FailFirstThenSucceedFifoHandler<FifoMessage>(failCount: 1);
        _serviceProvider = CreateFifoServiceProvider("inmem-fifo-fail-unlock", handler, o =>
        {
            o.ReadBatchSize = 1;
            o.PollingInterval = TimeSpan.FromMilliseconds(50);
            o.MaxAttempts = 3;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

        var messages = TestMessageFactory.CreateFifoMessages(3, "fail-group");
        await writer.WriteBatchAsync(messages, "inmem-fifo-fail-unlock");

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
        _serviceProvider = CreateFifoServiceProvider("inmem-fifo-release-groups", handler, o =>
        {
            o.ReadBatchSize = 10;
            o.PollingInterval = TimeSpan.FromMilliseconds(50);
            o.MaxProcessingTime = TimeSpan.FromMinutes(5);
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

        // Write first message for group-A
        await writer.WriteAsync(new FifoMessage("group-A", 0, "A-0"), "inmem-fifo-release-groups");

        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // Wait for first message to be captured
        await TestWaitHelper.WaitForConditionAsync(() => handler.CurrentlyProcessing >= 1, TimeSpan.FromSeconds(2));

        // Now write second message for same group
        await writer.WriteAsync(new FifoMessage("group-A", 1, "A-1"), "inmem-fifo-release-groups");

        // Give time for polling
        await Task.Delay(200);

        // Group-A should be locked, so A-1 shouldn't be captured yet
        handler.CapturedSequence.Should().HaveCount(1, "only first message should be captured while group is locked");
        handler.CapturedSequence[0].Should().Be(0, "first message (seq=0) should be captured");

        // Release first message
        handler.ReleaseOne();
        await TestWaitHelper.WaitForConditionAsync(() => handler.ProcessedCount >= 1);

        // Now second message should be captured
        await TestWaitHelper.WaitForConditionAsync(() => handler.CurrentlyProcessing > 0, TimeSpan.FromSeconds(2));
        handler.CapturedSequence.Should().HaveCount(2, "second message should now be captured");

        // Release second message
        handler.ReleaseAll();
        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 2, TimeSpan.FromSeconds(5));

        handler.ProcessedCount.Should().Be(2);
        output.WriteLine($"Messages captured in sequence: {string.Join(", ", handler.CapturedSequence)}");
    }

    /// <summary>
    /// Tests that ReleaseMessagesAndGroupLocksAsync releases all locks.
    /// </summary>
    [Fact]
    public async Task Fifo_ReleaseMessagesAndGroupLocks_ReleasesAll()
    {
        var handler = new TrackingFifoHandler<FifoMessage>();
        _serviceProvider = CreateFifoServiceProvider("inmem-fifo-release-all", handler, o =>
        {
            o.ReadBatchSize = 5;
            o.PollingInterval = TimeSpan.FromMilliseconds(50);
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

        // Write messages from multiple groups
        var messages = new List<FifoMessage>();
        for (int g = 0; g < 3; g++)
        {
            messages.AddRange(TestMessageFactory.CreateFifoMessages(3, $"release-group-{g}"));
        }
        await writer.WriteBatchAsync(messages, "inmem-fifo-release-all");

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
        _serviceProvider = CreateFifoServiceProvider("inmem-fifo-extend-locks", handler, o =>
        {
            o.ReadBatchSize = 1;
            o.PollingInterval = TimeSpan.FromMilliseconds(50);
            o.MaxProcessingTime = TimeSpan.FromSeconds(1); // Short timeout
            o.EnableLockExtension = true; // Enable automatic lock extension
            o.LockExtensionThreshold = 0.5; // Extend at 50% of MaxProcessingTime
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

        await writer.WriteAsync(new FifoMessage("extend-group", 0, "msg"), "inmem-fifo-extend-locks");

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
    public async Task Fifo_ExtendLocks_ExtendsGroupLockToo()
    {
        var handler = new BlockingFifoHandler<FifoMessage>();
        _serviceProvider = CreateFifoServiceProvider("inmem-fifo-extend-group", handler, o =>
        {
            o.ReadBatchSize = 1;
            o.PollingInterval = TimeSpan.FromMilliseconds(50);
            o.MaxProcessingTime = TimeSpan.FromSeconds(1);
            o.EnableLockExtension = true;
            o.LockExtensionThreshold = 0.5;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();

        // Write two messages for same group
        await writer.WriteAsync(new FifoMessage("extend-group-lock", 0, "msg-0"), "inmem-fifo-extend-group");
        await writer.WriteAsync(new FifoMessage("extend-group-lock", 1, "msg-1"), "inmem-fifo-extend-group");

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
                .UseInMemory()
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
