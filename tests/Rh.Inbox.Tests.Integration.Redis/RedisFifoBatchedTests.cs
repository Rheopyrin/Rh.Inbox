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

[Collection("Redis")]
public class RedisFifoBatchedTests(RedisContainerFixture container, ITestOutputHelper output) : IAsyncLifetime
{
    private ServiceProvider? _serviceProvider;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (_serviceProvider is not null)
            await _serviceProvider.DisposeAsync();
    }

    [Fact]
    public async Task FifoBatched_SingleGroup_ProcessesInOrderAsBatch()
    {
        var handler = new FifoBatchedCountingHandler<FifoMessage>();
        _serviceProvider = CreateFifoBatchedServiceProvider("fifo-batched-single", handler, o =>
        {
            o.ReadBatchSize = 100;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 50;
        var messages = TestMessageFactory.CreateFifoMessages(messageCount, "batch-group-1");
        await writer.WriteBatchAsync(messages, "fifo-batched-single");

        var elapsed = await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, messageCount);

        handler.ProcessedCount.Should().Be(messageCount);
        handler.ValidateGroupOrdering().Should().BeTrue("messages within the same group should be processed in order");

        output.WriteLine($"Processed {messageCount} FIFO batched messages in {elapsed.TotalMilliseconds:F0}ms");
        output.WriteLine($"Total batches received: {handler.GroupsProcessed}");
    }

    [Fact]
    public async Task FifoBatched_MultipleGroups_EachGroupInSeparateBatch()
    {
        var handler = new FifoBatchedCountingHandler<FifoMessage>();
        _serviceProvider = CreateFifoBatchedServiceProvider("fifo-batched-multi", handler, o =>
        {
            o.ReadBatchSize = 100;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int groupCount = 5;
        const int messagesPerGroup = 10;
        var allMessages = new List<FifoMessage>();

        for (int g = 0; g < groupCount; g++)
        {
            allMessages.AddRange(TestMessageFactory.CreateFifoMessages(messagesPerGroup, $"batch-group-{g}"));
        }

        // Write messages in order (not shuffled) to ensure FIFO ordering is maintained
        // Note: Shuffling would break Sequence order because FIFO guarantees write order, not external sequence
        await writer.WriteBatchAsync(allMessages, "fifo-batched-multi");

        var totalMessages = groupCount * messagesPerGroup;
        var elapsed = await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, totalMessages);

        handler.ProcessedCount.Should().Be(totalMessages);
        handler.ValidateGroupOrdering().Should().BeTrue("each group should be processed in order");

        // Verify each batch contains messages from only one group
        foreach (var group in handler.ProcessedGroups)
        {
            // Each processed group should have messages from the same group
            var groupsInBatch = group.Messages.Select(m => m.GroupId).Distinct().ToList();
            groupsInBatch.Should().HaveCount(1, "each batch should contain messages from only one group");
        }

        output.WriteLine($"Processed {totalMessages} messages across {groupCount} groups in {handler.GroupsProcessed} batches");
        output.WriteLine($"Time: {elapsed.TotalMilliseconds:F0}ms");
    }

    [Fact]
    public async Task FifoBatched_LargeGroup_ProcessedInMultipleBatches()
    {
        var handler = new FifoBatchedCountingHandler<FifoMessage>();
        const int batchSize = 10;
        _serviceProvider = CreateFifoBatchedServiceProvider("fifo-batched-large", handler, o =>
        {
            o.ReadBatchSize = batchSize;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 35; // Will require at least 4 batches
        var messages = TestMessageFactory.CreateFifoMessages(messageCount, "large-group");
        await writer.WriteBatchAsync(messages, "fifo-batched-large");

        var elapsed = await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, messageCount);

        handler.ProcessedCount.Should().Be(messageCount);
        handler.ValidateGroupOrdering().Should().BeTrue("messages should maintain order across batches");
        handler.GroupsProcessed.Should().BeGreaterOrEqualTo(4, "should require multiple batches");

        // Verify batch sizes
        foreach (var group in handler.ProcessedGroups)
        {
            group.Messages.Count.Should().BeLessOrEqualTo(batchSize, "each batch should respect ReadBatchSize");
        }

        output.WriteLine($"Processed {messageCount} messages in {handler.GroupsProcessed} batches");
        output.WriteLine($"Time: {elapsed.TotalMilliseconds:F0}ms");
    }

    [Fact]
    public async Task FifoBatched_GroupsProcessedConcurrently()
    {
        var handler = new DelayedFifoBatchedHandler<FifoMessage>(TimeSpan.FromMilliseconds(100));
        _serviceProvider = CreateFifoBatchedServiceProvider("fifo-batched-concurrent", handler, o =>
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
            allMessages.AddRange(TestMessageFactory.CreateFifoMessages(messagesPerGroup, $"concurrent-group-{g}"));
        }

        await writer.WriteBatchAsync(allMessages, "fifo-batched-concurrent");

        var totalMessages = groupCount * messagesPerGroup;
        var elapsed = await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, totalMessages, TimeSpan.FromSeconds(10));

        handler.ProcessedCount.Should().Be(totalMessages);

        // If processing was purely sequential, it would take at least 900ms (9 messages * 100ms)
        // With parallel groups, it should be faster
        elapsed.Should().BeLessThan(TimeSpan.FromMilliseconds(1500),
            "different groups should be processed somewhat in parallel");

        output.WriteLine($"Processed {totalMessages} messages across {groupCount} groups in {elapsed.TotalMilliseconds:F0}ms");
    }

    private ServiceProvider CreateFifoBatchedServiceProvider<THandler>(
        string inboxName,
        THandler handler,
        Action<IConfigureInboxOptions>? configureOptions = null)
        where THandler : class, IFifoBatchedInboxHandler<FifoMessage>
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox(inboxName, builder =>
        {
            builder.AsFifoBatched()
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

file class DelayedFifoBatchedHandler<TMessage> : IFifoBatchedInboxHandler<TMessage>
    where TMessage : class, IHasGroupId
{
    private readonly TimeSpan _delay;
    private int _processedCount;

    public DelayedFifoBatchedHandler(TimeSpan delay)
    {
        _delay = delay;
    }

    public int ProcessedCount => _processedCount;

    public async Task<IReadOnlyList<InboxMessageResult>> HandleAsync(
        string groupId,
        IReadOnlyList<InboxMessageEnvelope<TMessage>> messages,
        CancellationToken token)
    {
        var results = new List<InboxMessageResult>();

        foreach (var message in messages)
        {
            await Task.Delay(_delay, token);
            Interlocked.Increment(ref _processedCount);
            results.Add(new InboxMessageResult(message.Id, InboxHandleResult.Success));
        }

        return results;
    }
}
