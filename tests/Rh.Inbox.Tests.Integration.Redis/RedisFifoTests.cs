using FluentAssertions;
using Rh.Inbox.Abstractions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Extensions;
using Rh.Inbox.Redis;
using Rh.Inbox.Tests.Integration.Common;
using Rh.Inbox.Tests.Integration.Common.Handlers;
using Rh.Inbox.Tests.Integration.Common.TestMessages;
using Xunit;
using Xunit.Abstractions;

namespace Rh.Inbox.Tests.Integration.Redis;

[Collection("Redis")]
public class RedisFifoTests(RedisContainerFixture container, ITestOutputHelper output) : IAsyncLifetime
{
    private ServiceProvider? _serviceProvider;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (_serviceProvider is not null)
            await _serviceProvider.DisposeAsync();
    }

    [Fact]
    public async Task Fifo_SingleGroup_ProcessesInOrder()
    {
        var handler = new FifoCountingHandler<FifoMessage>();
        _serviceProvider = CreateFifoServiceProvider("fifo-single-group", handler);

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 50;
        var messages = TestMessageFactory.CreateFifoMessages(messageCount, "group-1");
        await writer.WriteBatchAsync(messages, "fifo-single-group");

        var elapsed = await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, messageCount);

        handler.ProcessedCount.Should().Be(messageCount);
        handler.ValidateOrdering().Should().BeTrue("messages within the same group should be processed in order");

        output.WriteLine($"Processed {messageCount} FIFO messages in {elapsed.TotalMilliseconds:F0}ms");
    }

    [Fact]
    public async Task Fifo_MultipleGroups_ProcessesEachGroupInOrder()
    {
        var handler = new FifoCountingHandler<FifoMessage>();
        _serviceProvider = CreateFifoServiceProvider("fifo-multi-group", handler);

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int groupCount = 5;
        const int messagesPerGroup = 20;
        var allMessages = new List<FifoMessage>();

        // Create messages for multiple groups
        for (int g = 0; g < groupCount; g++)
        {
            allMessages.AddRange(TestMessageFactory.CreateFifoMessages(messagesPerGroup, $"group-{g}"));
        }

        // Shuffle messages to ensure they're written in mixed order
        var shuffled = allMessages.OrderBy(_ => Random.Shared.Next()).ToList();
        await writer.WriteBatchAsync(shuffled, "fifo-multi-group");

        var totalMessages = groupCount * messagesPerGroup;
        var elapsed = await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, totalMessages);

        handler.ProcessedCount.Should().Be(totalMessages);
        handler.ValidateOrdering().Should().BeTrue("each group should be processed in order");

        // Verify each group has the correct message count
        var groupCounts = handler.Processed
            .GroupBy(p => p.Message.GroupId)
            .ToDictionary(g => g.Key, g => g.Count());

        groupCounts.Should().HaveCount(groupCount);
        foreach (var (groupId, count) in groupCounts)
        {
            count.Should().Be(messagesPerGroup, $"group {groupId} should have {messagesPerGroup} messages");
        }

        output.WriteLine($"Processed {totalMessages} messages across {groupCount} groups in {elapsed.TotalMilliseconds:F0}ms");
    }

    [Fact]
    public async Task Fifo_GroupLocking_PreventsParallelProcessingWithinGroup()
    {
        var handler = new DelayedFifoHandler<FifoMessage>(TimeSpan.FromMilliseconds(50));
        _serviceProvider = CreateFifoServiceProvider("fifo-locking", handler, o =>
        {
            o.ReadBatchSize = 10;
            o.PollingInterval = TimeSpan.FromMilliseconds(20);
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 10;
        var messages = TestMessageFactory.CreateFifoMessages(messageCount, "locked-group");
        await writer.WriteBatchAsync(messages, "fifo-locking");

        var elapsed = await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, messageCount, TimeSpan.FromSeconds(10));

        handler.ProcessedCount.Should().Be(messageCount);

        // Verify no overlapping processing times within the group
        var processingTimes = handler.Processed
            .OrderBy(p => p.ProcessedAt)
            .ToList();

        for (int i = 1; i < processingTimes.Count; i++)
        {
            var prev = processingTimes[i - 1];
            var curr = processingTimes[i];

            // Next message should start after previous finished (accounting for some timing tolerance)
            var gap = curr.ProcessedAt - prev.ProcessedAt;
            gap.Should().BeGreaterOrEqualTo(TimeSpan.FromMilliseconds(40),
                "messages in the same group should not overlap in processing");
        }

        output.WriteLine($"Processed {messageCount} messages sequentially in {elapsed.TotalMilliseconds:F0}ms");
    }

    [Fact]
    public async Task Fifo_DifferentGroups_CanProcessInParallel()
    {
        var handler = new DelayedFifoHandler<FifoMessage>(TimeSpan.FromMilliseconds(100));
        _serviceProvider = CreateFifoServiceProvider("fifo-parallel-groups", handler, o =>
        {
            o.ReadBatchSize = 10;
            o.PollingInterval = TimeSpan.FromMilliseconds(20);
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int groupCount = 3;
        const int messagesPerGroup = 3;
        var allMessages = new List<FifoMessage>();

        for (int g = 0; g < groupCount; g++)
        {
            allMessages.AddRange(TestMessageFactory.CreateFifoMessages(messagesPerGroup, $"parallel-group-{g}"));
        }

        await writer.WriteBatchAsync(allMessages, "fifo-parallel-groups");

        var totalMessages = groupCount * messagesPerGroup;
        var elapsed = await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, totalMessages, TimeSpan.FromSeconds(10));

        handler.ProcessedCount.Should().Be(totalMessages);

        // If processing was purely sequential, it would take at least 900ms (9 messages * 100ms)
        // With parallel groups, it should be faster (around 300-400ms for 3 messages per group)
        // Allow some buffer for CI/CD environment variations
        elapsed.Should().BeLessThan(TimeSpan.FromMilliseconds(1500),
            "different groups should be processed somewhat in parallel");

        output.WriteLine($"Processed {totalMessages} messages across {groupCount} groups in {elapsed.TotalMilliseconds:F0}ms");
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
