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

public class HostedServiceLifecycleTests(ITestOutputHelper output) : IAsyncLifetime
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
    public async Task HostedService_WhenHostStarts_InboxStartsProcessing()
    {
        var handler = new CountingHandler<SimpleMessage>();

        _host = CreateHost("hosted-start", handler);

        // Start the host first - this starts the inbox
        await _host.StartAsync();

        // Now write messages
        using var scope = _host.Services.CreateScope();
        var writer = scope.ServiceProvider.GetRequiredService<IInboxWriter>();
        var messages = TestMessageFactory.CreateSimpleMessages(10);
        await writer.WriteBatchAsync(messages, "hosted-start");

        // Wait for processing
        var elapsed = await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 10);

        handler.ProcessedCount.Should().Be(10, "all messages should be processed after host starts");

        output.WriteLine($"Processed 10 messages in {elapsed.TotalMilliseconds:F0}ms after host started");
    }

    [Fact]
    public async Task HostedService_WhenHostStops_InboxStopsProcessing()
    {
        var handler = new DelayedHandler<SimpleMessage>(TimeSpan.FromMilliseconds(100));

        _host = CreateHost("hosted-stop", handler);
        await _host.StartAsync();

        using var scope = _host.Services.CreateScope();
        var writer = scope.ServiceProvider.GetRequiredService<IInboxWriter>();

        // Write some messages
        var messages = TestMessageFactory.CreateSimpleMessages(10);
        await writer.WriteBatchAsync(messages, "hosted-stop");

        // Wait for some processing to start (but not all)
        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 2, TimeSpan.FromSeconds(5));

        // Stop the host while messages are still being processed
        await _host.StopAsync();

        var countAfterStop = handler.ProcessedCount;

        // Wait a bit to verify no more processing happens
        await Task.Delay(500);

        // Count should not have increased after stop
        handler.ProcessedCount.Should().BeInRange(countAfterStop, countAfterStop + 1,
            "processing should stop after host stops (allowing for in-flight message)");

        output.WriteLine($"Processed {countAfterStop} messages before host stopped");
    }

    [Fact]
    public async Task HostedService_CanProcessMultipleBatchesWhileRunning()
    {
        var handler = new CountingHandler<SimpleMessage>();

        _host = CreateHost("hosted-multi-batch", handler);
        await _host.StartAsync();

        using var scope = _host.Services.CreateScope();
        var writer = scope.ServiceProvider.GetRequiredService<IInboxWriter>();

        // First batch
        var messages1 = TestMessageFactory.CreateSimpleMessages(5);
        await writer.WriteBatchAsync(messages1, "hosted-multi-batch");
        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 5);

        handler.ProcessedCount.Should().Be(5);
        output.WriteLine("First batch processed");

        // Small delay between batches
        await Task.Delay(100);

        // Second batch
        var messages2 = TestMessageFactory.CreateSimpleMessages(5);
        await writer.WriteBatchAsync(messages2, "hosted-multi-batch");
        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 10);

        handler.ProcessedCount.Should().Be(10);
        output.WriteLine("Second batch processed");

        // Third batch
        var messages3 = TestMessageFactory.CreateSimpleMessages(5);
        await writer.WriteBatchAsync(messages3, "hosted-multi-batch");
        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 15);

        handler.ProcessedCount.Should().Be(15, "all three batches processed correctly");
        output.WriteLine("Third batch processed - all 15 messages processed");
    }

    [Fact]
    public async Task HostedService_WithoutRunInboxAsHostedService_RequiresManualStart()
    {
        var handler = new CountingHandler<SimpleMessage>();

        // Create host WITHOUT RunInboxAsHostedService
        _host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddLogging();
                services.AddInbox("manual-start", builder =>
                {
                    builder.AsDefault()
                        .UseInMemory()
                        .ConfigureOptions(o => o.PollingInterval = TimeSpan.FromMilliseconds(50))
                        .RegisterHandler(handler);
                });
            })
            .Build();

        await _host.StartAsync();

        // Inbox is not started yet (no RunInboxAsHostedService), so can't write
        // Manually start the inbox first
        var manager = _host.Services.GetRequiredService<IInboxManager>();
        await manager.StartAsync(CancellationToken.None);

        // Now we can write and verify processing
        using var scope = _host.Services.CreateScope();
        var writer = scope.ServiceProvider.GetRequiredService<IInboxWriter>();
        var messages = TestMessageFactory.CreateSimpleMessages(5);
        await writer.WriteBatchAsync(messages, "manual-start");

        // Wait for processing
        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 5);

        handler.ProcessedCount.Should().Be(5, "messages processed after manual start");

        output.WriteLine("Manual start required without RunInboxAsHostedService");
    }

    [Fact]
    public async Task HostedService_ContinuousProcessing_HandlesMessagesAsTheyArrive()
    {
        var handler = new CountingHandler<SimpleMessage>();

        _host = CreateHost("hosted-continuous", handler);
        await _host.StartAsync();

        using var scope = _host.Services.CreateScope();
        var writer = scope.ServiceProvider.GetRequiredService<IInboxWriter>();

        // Send messages in waves
        for (int wave = 0; wave < 3; wave++)
        {
            var messages = TestMessageFactory.CreateSimpleMessages(10);
            await writer.WriteBatchAsync(messages, "hosted-continuous");
            await Task.Delay(100); // Small delay between waves
        }

        var expectedCount = 30;
        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, expectedCount);

        handler.ProcessedCount.Should().Be(expectedCount);

        output.WriteLine($"Processed {expectedCount} messages across 3 waves");
    }

    private IHost CreateHost<THandler>(string inboxName, THandler handler)
        where THandler : class, Abstractions.Handlers.IInboxHandler<SimpleMessage>
    {
        return Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddLogging();
                services.AddInbox(inboxName, builder =>
                {
                    builder.AsDefault()
                        .UseInMemory()
                        .ConfigureOptions(o => o.PollingInterval = TimeSpan.FromMilliseconds(50))
                        .RegisterHandler(handler);
                });
                services.RunInboxAsHostedService();
            })
            .Build();
    }
}