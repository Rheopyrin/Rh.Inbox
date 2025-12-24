using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Extensions;
using Rh.Inbox.InMemory;
using Rh.Inbox.Tests.Integration.Common;
using Rh.Inbox.Tests.Integration.Common.Handlers;
using Rh.Inbox.Tests.Integration.Common.TestMessages;
using Xunit;
using Xunit.Abstractions;

namespace Rh.Inbox.Tests.Integration.InMemory;

public class InMemoryMultiInboxTests(ITestOutputHelper output) : IAsyncLifetime
{
    private ServiceProvider? _serviceProvider;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (_serviceProvider is not null)
            await _serviceProvider.DisposeAsync();
    }

    [Fact]
    public async Task MultiInbox_FourInboxesInSingleHost_AllProcessIndependently()
    {
        const int messagesPerInbox = 200;
        var handlers = Enumerable.Range(1, 4)
            .Select(i => new CountingHandler<SimpleMessage>($"inbox{i}"))
            .ToArray();

        _serviceProvider = CreateMultiInboxServiceProvider(handlers);

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        for (int i = 0; i < messagesPerInbox; i++)
        {
            for (int j = 0; j < 4; j++)
            {
                await writer.WriteAsync(new SimpleMessage($"msg{j + 1}-{i}", "data"), $"inbox{j + 1}");
            }
        }

        var elapsed = await TestWaitHelper.WaitForAllAsync(
            handlers.Select<CountingHandler<SimpleMessage>, Func<int>>(h => () => h.ProcessedCount).ToList(),
            messagesPerInbox);

        foreach (var handler in handlers)
        {
            handler.ProcessedCount.Should().Be(messagesPerInbox);
        }

        var totalMessages = messagesPerInbox * 4;
        output.WriteLine($"Processed {totalMessages} messages across 4 inboxes in {elapsed.TotalMilliseconds:F0}ms ({totalMessages / elapsed.TotalSeconds:F2} msg/s)");
    }

    [Fact]
    public async Task MultiInbox_MixedInboxTypes_AllProcessCorrectly()
    {
        const int messagesPerInbox = 100;
        var defaultHandler = new CountingHandler<SimpleMessage>("default");
        var batchedHandler = new BatchedCountingHandler<SimpleMessage>("batched");

        _serviceProvider = CreateMixedInboxServiceProvider(defaultHandler, batchedHandler);

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        await writer.WriteBatchAsync(TestMessageFactory.CreateSimpleMessages(messagesPerInbox, "default"), "default-inbox");
        await writer.WriteBatchAsync(TestMessageFactory.CreateSimpleMessages(messagesPerInbox, "batched"), "batched-inbox");

        var elapsed = await TestWaitHelper.WaitForAllAsync(
            [() => defaultHandler.ProcessedCount, () => batchedHandler.ProcessedCount],
            messagesPerInbox);

        defaultHandler.ProcessedCount.Should().Be(messagesPerInbox);
        batchedHandler.ProcessedCount.Should().Be(messagesPerInbox);
        output.WriteLine($"Default: {defaultHandler.ProcessedCount}, Batched: {batchedHandler.ProcessedCount} in {elapsed.TotalMilliseconds:F0}ms");
    }

    private ServiceProvider CreateMultiInboxServiceProvider(CountingHandler<SimpleMessage>[] handlers)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        for (int i = 0; i < handlers.Length; i++)
        {
            var handler = handlers[i];
            var inboxName = $"inbox{i + 1}";
            services.AddInbox(inboxName, builder =>
            {
                builder.AsDefault()
                    .UseInMemory()
                    .ConfigureOptions(o => o.PollingInterval = TimeSpan.FromMilliseconds(20))
                    .RegisterHandler(handler);
            });
        }
        return services.BuildServiceProvider();
    }

    private ServiceProvider CreateMixedInboxServiceProvider(
        CountingHandler<SimpleMessage> defaultHandler,
        BatchedCountingHandler<SimpleMessage> batchedHandler)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox("default-inbox", builder =>
        {
            builder.AsDefault()
                .UseInMemory()
                .ConfigureOptions(o => o.PollingInterval = TimeSpan.FromMilliseconds(20))
                .RegisterHandler(defaultHandler);
        });
        services.AddInbox("batched-inbox", builder =>
        {
            builder.AsBatched()
                .UseInMemory()
                .ConfigureOptions(o => o.PollingInterval = TimeSpan.FromMilliseconds(20))
                .RegisterHandler(batchedHandler);
        });
        return services.BuildServiceProvider();
    }
}
