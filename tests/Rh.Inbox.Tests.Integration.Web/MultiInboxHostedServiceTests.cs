using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Extensions;
using Rh.Inbox.InMemory;
using Rh.Inbox.Tests.Integration.Common;
using Rh.Inbox.Tests.Integration.Common.Handlers;
using Rh.Inbox.Tests.Integration.Common.TestMessages;
using Rh.Inbox.Web;
using Xunit;
using Xunit.Abstractions;

namespace Rh.Inbox.Tests.Integration.Web;

public class MultiInboxHostedServiceTests(ITestOutputHelper output) : IAsyncLifetime
{
    private IHost? _host;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (_host is not null)
        {
            await _host.StopAsync();
            _host.Dispose();
        }
    }

    [Fact]
    public async Task MultiInbox_AllInboxesStartWithHost()
    {
        var handler1 = new CountingHandler<SimpleMessage>();
        var handler2 = new CountingHandler<SimpleMessage>();
        var handler3 = new CountingHandler<SimpleMessage>();

        _host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddLogging();

                services.AddInbox("inbox-1", builder =>
                {
                    builder.AsDefault()
                        .UseInMemory()
                        .ConfigureOptions(o => o.PollingInterval = TimeSpan.FromMilliseconds(50))
                        .RegisterHandler(handler1);
                });

                services.AddInbox("inbox-2", builder =>
                {
                    builder.AsDefault()
                        .UseInMemory()
                        .ConfigureOptions(o => o.PollingInterval = TimeSpan.FromMilliseconds(50))
                        .RegisterHandler(handler2);
                });

                services.AddInbox("inbox-3", builder =>
                {
                    builder.AsDefault()
                        .UseInMemory()
                        .ConfigureOptions(o => o.PollingInterval = TimeSpan.FromMilliseconds(50))
                        .RegisterHandler(handler3);
                });

                services.RunInboxAsHostedService();
            })
            .Build();

        // Start host first - this starts all inboxes
        await _host.StartAsync();

        using var scope = _host.Services.CreateScope();
        var writer = scope.ServiceProvider.GetRequiredService<IInboxWriter>();

        // Write messages to all inboxes after starting
        await writer.WriteBatchAsync(TestMessageFactory.CreateSimpleMessages(5), "inbox-1");
        await writer.WriteBatchAsync(TestMessageFactory.CreateSimpleMessages(5), "inbox-2");
        await writer.WriteBatchAsync(TestMessageFactory.CreateSimpleMessages(5), "inbox-3");

        // Wait for all inboxes to process
        await TestWaitHelper.WaitForCountAsync(() => handler1.ProcessedCount, 5);
        await TestWaitHelper.WaitForCountAsync(() => handler2.ProcessedCount, 5);
        await TestWaitHelper.WaitForCountAsync(() => handler3.ProcessedCount, 5);

        handler1.ProcessedCount.Should().Be(5);
        handler2.ProcessedCount.Should().Be(5);
        handler3.ProcessedCount.Should().Be(5);

        output.WriteLine("All 3 inboxes started and processed messages with host");
    }

    [Fact]
    public async Task MultiInbox_AllInboxesStopWithHost()
    {
        var handler1 = new DelayedHandler<SimpleMessage>(TimeSpan.FromMilliseconds(50));
        var handler2 = new DelayedHandler<SimpleMessage>(TimeSpan.FromMilliseconds(50));

        _host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddLogging();

                services.AddInbox("stop-inbox-1", builder =>
                {
                    builder.AsDefault()
                        .UseInMemory()
                        .ConfigureOptions(o => o.PollingInterval = TimeSpan.FromMilliseconds(50))
                        .RegisterHandler(handler1);
                });

                services.AddInbox("stop-inbox-2", builder =>
                {
                    builder.AsDefault()
                        .UseInMemory()
                        .ConfigureOptions(o => o.PollingInterval = TimeSpan.FromMilliseconds(50))
                        .RegisterHandler(handler2);
                });

                services.RunInboxAsHostedService();
            })
            .Build();

        await _host.StartAsync();

        using var scope = _host.Services.CreateScope();
        var writer = scope.ServiceProvider.GetRequiredService<IInboxWriter>();

        // Write some messages
        await writer.WriteBatchAsync(TestMessageFactory.CreateSimpleMessages(10), "stop-inbox-1");
        await writer.WriteBatchAsync(TestMessageFactory.CreateSimpleMessages(10), "stop-inbox-2");

        // Wait for some processing to start
        await TestWaitHelper.WaitForCountAsync(() => handler1.ProcessedCount, 2, TimeSpan.FromSeconds(5));
        await TestWaitHelper.WaitForCountAsync(() => handler2.ProcessedCount, 2, TimeSpan.FromSeconds(5));

        // Stop host while messages are still being processed
        await _host.StopAsync();

        var count1AfterStop = handler1.ProcessedCount;
        var count2AfterStop = handler2.ProcessedCount;

        // Wait and verify no more processing
        await Task.Delay(300);

        // Counts should not have increased significantly after stop
        handler1.ProcessedCount.Should().BeInRange(count1AfterStop, count1AfterStop + 1,
            "inbox-1 should stop processing after host stops");
        handler2.ProcessedCount.Should().BeInRange(count2AfterStop, count2AfterStop + 1,
            "inbox-2 should stop processing after host stops");

        output.WriteLine($"Both inboxes stopped. Inbox1: {count1AfterStop}, Inbox2: {count2AfterStop}");
    }

    [Fact]
    public async Task MultiInbox_MixedInboxTypes_AllWorkWithHostedService()
    {
        var defaultHandler = new CountingHandler<SimpleMessage>();
        var batchedHandler = new BatchedCountingHandler<SimpleMessage>();
        var fifoHandler = new FifoCountingHandler<FifoMessage>();

        _host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddLogging();

                services.AddInbox("default-type", builder =>
                {
                    builder.AsDefault()
                        .UseInMemory()
                        .ConfigureOptions(o => o.PollingInterval = TimeSpan.FromMilliseconds(50))
                        .RegisterHandler(defaultHandler);
                });

                services.AddInbox("batched-type", builder =>
                {
                    builder.AsBatched()
                        .UseInMemory()
                        .ConfigureOptions(o =>
                        {
                            o.PollingInterval = TimeSpan.FromMilliseconds(50);
                            o.ReadBatchSize = 10;
                        })
                        .RegisterHandler(batchedHandler);
                });

                services.AddInbox("fifo-type", builder =>
                {
                    builder.AsFifo()
                        .UseInMemory()
                        .ConfigureOptions(o => o.PollingInterval = TimeSpan.FromMilliseconds(50))
                        .RegisterHandler(fifoHandler);
                });

                services.RunInboxAsHostedService();
            })
            .Build();

        await _host.StartAsync();

        using var scope = _host.Services.CreateScope();
        var writer = scope.ServiceProvider.GetRequiredService<IInboxWriter>();

        // Write to all inbox types
        await writer.WriteBatchAsync(TestMessageFactory.CreateSimpleMessages(10), "default-type");
        await writer.WriteBatchAsync(TestMessageFactory.CreateSimpleMessages(10), "batched-type");
        await writer.WriteBatchAsync(TestMessageFactory.CreateFifoMessages(10, "group-1"), "fifo-type");

        // Wait for all to complete
        await TestWaitHelper.WaitForCountAsync(() => defaultHandler.ProcessedCount, 10);
        await TestWaitHelper.WaitForCountAsync(() => batchedHandler.ProcessedCount, 10);
        await TestWaitHelper.WaitForCountAsync(() => fifoHandler.ProcessedCount, 10);

        defaultHandler.ProcessedCount.Should().Be(10);
        batchedHandler.ProcessedCount.Should().Be(10);
        fifoHandler.ProcessedCount.Should().Be(10);
        fifoHandler.ValidateOrdering().Should().BeTrue("FIFO order should be maintained");

        output.WriteLine("All inbox types (default, batched, FIFO) work with hosted service");
    }

    [Fact]
    public async Task MultiInbox_IndependentProcessing_NoInterference()
    {
        var slowHandler = new DelayedHandler<SimpleMessage>(TimeSpan.FromMilliseconds(100));
        var fastHandler = new CountingHandler<SimpleMessage>();

        _host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddLogging();

                services.AddInbox("slow-inbox", builder =>
                {
                    builder.AsDefault()
                        .UseInMemory()
                        .ConfigureOptions(o => o.PollingInterval = TimeSpan.FromMilliseconds(50))
                        .RegisterHandler(slowHandler);
                });

                services.AddInbox("fast-inbox", builder =>
                {
                    builder.AsDefault()
                        .UseInMemory()
                        .ConfigureOptions(o => o.PollingInterval = TimeSpan.FromMilliseconds(50))
                        .RegisterHandler(fastHandler);
                });

                services.RunInboxAsHostedService();
            })
            .Build();

        await _host.StartAsync();

        using var scope = _host.Services.CreateScope();
        var writer = scope.ServiceProvider.GetRequiredService<IInboxWriter>();

        // Write to both inboxes at the same time
        await writer.WriteBatchAsync(TestMessageFactory.CreateSimpleMessages(10), "slow-inbox");
        await writer.WriteBatchAsync(TestMessageFactory.CreateSimpleMessages(50), "fast-inbox");

        // Fast inbox should complete quickly
        var fastElapsed = await TestWaitHelper.WaitForCountAsync(() => fastHandler.ProcessedCount, 50);

        fastHandler.ProcessedCount.Should().Be(50);

        // Slow inbox should still be processing (10 messages * 100ms = ~1000ms)
        // Fast inbox should have finished much quicker
        fastElapsed.Should().BeLessThan(TimeSpan.FromMilliseconds(500),
            "fast inbox should not be blocked by slow inbox");

        // Wait for slow inbox to finish
        await TestWaitHelper.WaitForCountAsync(() => slowHandler.ProcessedCount, 10, TimeSpan.FromSeconds(5));

        slowHandler.ProcessedCount.Should().Be(10);

        output.WriteLine($"Fast inbox: {fastHandler.ProcessedCount} in {fastElapsed.TotalMilliseconds:F0}ms");
        output.WriteLine($"Slow inbox: {slowHandler.ProcessedCount} (expected ~1000ms)");
    }

    [Fact]
    public async Task MultiInbox_HighVolume_AllProcessedCorrectly()
    {
        var handlers = new List<CountingHandler<SimpleMessage>>();
        const int inboxCount = 5;
        const int messagesPerInbox = 100;

        var hostBuilder = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddLogging();

                for (int i = 0; i < inboxCount; i++)
                {
                    var handler = new CountingHandler<SimpleMessage>();
                    handlers.Add(handler);

                    var inboxName = $"volume-inbox-{i}";
                    services.AddInbox(inboxName, builder =>
                    {
                        builder.AsDefault()
                            .UseInMemory()
                            .ConfigureOptions(o => o.PollingInterval = TimeSpan.FromMilliseconds(20))
                            .RegisterHandler(handler);
                    });
                }

                services.RunInboxAsHostedService();
            });

        _host = hostBuilder.Build();
        await _host.StartAsync();

        using var scope = _host.Services.CreateScope();
        var writer = scope.ServiceProvider.GetRequiredService<IInboxWriter>();

        // Write to all inboxes
        var writeTasks = new List<Task>();
        for (int i = 0; i < inboxCount; i++)
        {
            var messages = TestMessageFactory.CreateSimpleMessages(messagesPerInbox);
            var inboxName = $"volume-inbox-{i}";
            writeTasks.Add(writer.WriteBatchAsync(messages, inboxName));
        }
        await Task.WhenAll(writeTasks);

        // Wait for all to complete
        var waitTasks = handlers.Select(h =>
            TestWaitHelper.WaitForCountAsync(() => h.ProcessedCount, messagesPerInbox));
        await Task.WhenAll(waitTasks);

        // Verify all processed
        for (int i = 0; i < inboxCount; i++)
        {
            handlers[i].ProcessedCount.Should().Be(messagesPerInbox,
                $"inbox-{i} should process all {messagesPerInbox} messages");
        }

        var totalProcessed = handlers.Sum(h => h.ProcessedCount);
        output.WriteLine($"Processed {totalProcessed} messages across {inboxCount} inboxes");
    }
}
