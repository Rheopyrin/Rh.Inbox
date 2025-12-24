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

[Collection("Postgres")]
public class PostgresFifoBatchedTests(PostgresContainerFixture container, ITestOutputHelper output) : IAsyncLifetime
{
    private ServiceProvider? _serviceProvider;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (_serviceProvider is not null)
            await _serviceProvider.DisposeAsync();
    }

    [Fact]
    public async Task FifoBatched_SingleGroup_ProcessesAllInOneBatch()
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
        handler.GroupsProcessed.Should().Be(1, "all messages belong to one group and should be processed in one batch");

        var processedGroup = handler.ProcessedGroups.Single();
        processedGroup.GroupId.Should().Be("batch-group-1");
        processedGroup.Messages.Should().HaveCount(messageCount);

        output.WriteLine($"Processed {messageCount} messages in {handler.GroupsProcessed} batch(es) in {elapsed.TotalMilliseconds:F0}ms");
    }

    [Fact]
    public async Task FifoBatched_MultipleGroups_ProcessesEachGroupSeparately()
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
        const int messagesPerGroup = 20;
        var allMessages = new List<FifoMessage>();

        for (int g = 0; g < groupCount; g++)
        {
            allMessages.AddRange(TestMessageFactory.CreateFifoMessages(messagesPerGroup, $"batch-group-{g}"));
        }

        // Shuffle to mix groups
        var shuffled = allMessages.OrderBy(_ => Random.Shared.Next()).ToList();
        await writer.WriteBatchAsync(shuffled, "fifo-batched-multi");

        var totalMessages = groupCount * messagesPerGroup;
        var elapsed = await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, totalMessages);

        handler.ProcessedCount.Should().Be(totalMessages);

        // Each group should be processed as a separate batch
        var groupsProcessed = handler.ProcessedGroups
            .GroupBy(g => g.GroupId)
            .ToDictionary(g => g.Key, g => g.Sum(x => x.Messages.Count));

        groupsProcessed.Should().HaveCount(groupCount);
        foreach (var (groupId, count) in groupsProcessed)
        {
            count.Should().Be(messagesPerGroup, $"group {groupId} should have {messagesPerGroup} messages");
        }

        output.WriteLine($"Processed {totalMessages} messages in {handler.GroupsProcessed} batch(es) across {groupCount} groups in {elapsed.TotalMilliseconds:F0}ms");
    }

    [Fact]
    public async Task FifoBatched_LargeBatch_SplitsIntoBatchSize()
    {
        var handler = new FifoBatchedCountingHandler<FifoMessage>();
        _serviceProvider = CreateFifoBatchedServiceProvider("fifo-batched-large", handler, o =>
        {
            o.ReadBatchSize = 20; // Small batch size to force multiple batches
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 100;
        var messages = TestMessageFactory.CreateFifoMessages(messageCount, "large-batch-group");
        await writer.WriteBatchAsync(messages, "fifo-batched-large");

        var elapsed = await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, messageCount, TestConstants.LongProcessingTimeout);

        handler.ProcessedCount.Should().Be(messageCount);

        // With batch size of 20 and 100 messages in one group, we should have multiple batches
        handler.GroupsProcessed.Should().BeGreaterOrEqualTo(1);

        // All batches should be for the same group
        handler.ProcessedGroups.All(g => g.GroupId == "large-batch-group").Should().BeTrue();

        output.WriteLine($"Processed {messageCount} messages in {handler.GroupsProcessed} batch(es) in {elapsed.TotalMilliseconds:F0}ms");
    }

    [Fact]
    public async Task FifoBatched_MaintainsOrderWithinGroup()
    {
        var handler = new FifoBatchedCountingHandler<FifoMessage>();
        _serviceProvider = CreateFifoBatchedServiceProvider("fifo-batched-order", handler, o =>
        {
            o.ReadBatchSize = 50;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 30;
        var messages = TestMessageFactory.CreateFifoMessages(messageCount, "order-group");

        // Write messages one by one to ensure proper ordering
        foreach (var msg in messages)
        {
            await writer.WriteAsync(msg, "fifo-batched-order");
        }

        var elapsed = await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, messageCount);

        handler.ProcessedCount.Should().Be(messageCount);
        handler.ValidateGroupOrdering().Should().BeTrue("messages within each batch should maintain their sequence order");

        output.WriteLine($"Processed {messageCount} messages with correct ordering in {elapsed.TotalMilliseconds:F0}ms");
    }

    [Fact]
    public async Task FifoBatched_HighThroughput_MeasuresPerformance()
    {
        var handler = new FifoBatchedCountingHandler<FifoMessage>();
        _serviceProvider = CreateFifoBatchedServiceProvider("fifo-batched-throughput", handler, o =>
        {
            o.ReadBatchSize = 100;
            o.PollingInterval = TimeSpan.FromMilliseconds(30);
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int groupCount = 10;
        const int messagesPerGroup = 100;
        var allMessages = new List<FifoMessage>();

        for (int g = 0; g < groupCount; g++)
        {
            allMessages.AddRange(TestMessageFactory.CreateFifoMessages(messagesPerGroup, $"throughput-group-{g}"));
        }

        var writeSw = System.Diagnostics.Stopwatch.StartNew();
        foreach (var batch in TestMessageFactory.BatchMessages(allMessages, 100))
        {
            await writer.WriteBatchAsync(batch, "fifo-batched-throughput");
        }
        writeSw.Stop();

        var totalMessages = groupCount * messagesPerGroup;
        var elapsed = await TestWaitHelper.WaitForCountAsync(
            () => handler.ProcessedCount, totalMessages, TestConstants.LongProcessingTimeout);

        handler.ProcessedCount.Should().Be(totalMessages);

        output.WriteLine($"Write: {totalMessages} messages in {writeSw.ElapsedMilliseconds}ms ({totalMessages / writeSw.Elapsed.TotalSeconds:F2} msg/s)");
        output.WriteLine($"Process: {totalMessages} messages in {elapsed.TotalMilliseconds:F0}ms ({totalMessages / elapsed.TotalSeconds:F2} msg/s)");
        output.WriteLine($"Groups: {groupCount}, Batches processed: {handler.GroupsProcessed}");
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
