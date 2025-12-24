using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Extensions;
using Rh.Inbox.Postgres;
using Rh.Inbox.Tests.Integration.Common;
using Rh.Inbox.Tests.Integration.Common.Handlers;
using Rh.Inbox.Tests.Integration.Common.TestMessages;
using Xunit;
using Xunit.Abstractions;

namespace Rh.Inbox.Tests.Integration.Postgres;

[Collection("Postgres")]
public class PostgresMultiInboxTests(PostgresContainerFixture container, ITestOutputHelper output) : IAsyncLifetime
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
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
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
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
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

    [Fact]
    public async Task MultiInbox_FailureIsolation_OneInboxFailureDoesNotAffectOthers()
    {
        const int messagesPerInbox = 50;
        var healthyHandler = new CountingHandler<SimpleMessage>("healthy");
        var failingHandler = new FailingHandler<SimpleMessage>(1.0); // Always fails

        _serviceProvider = CreateTwoInboxServiceProvider(healthyHandler, failingHandler);

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        for (int i = 0; i < messagesPerInbox; i++)
        {
            await writer.WriteAsync(new SimpleMessage($"healthy-{i}", "data"), "healthy-inbox");
            await writer.WriteAsync(new SimpleMessage($"failing-{i}", "data"), "failing-inbox");
        }

        await TestWaitHelper.WaitForCountAsync(() => healthyHandler.ProcessedCount, messagesPerInbox);

        healthyHandler.ProcessedCount.Should().Be(messagesPerInbox,
            "healthy inbox should process all messages regardless of failing inbox");
        output.WriteLine($"Healthy: {healthyHandler.ProcessedCount}, Failing attempts: {failingHandler.FailedCount}");
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
                    .UsePostgres(container.ConnectionString)
                    .ConfigureOptions(o => o.PollingInterval = TimeSpan.FromMilliseconds(50))
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
                .UsePostgres(container.ConnectionString)
                .ConfigureOptions(o => o.PollingInterval = TimeSpan.FromMilliseconds(50))
                .RegisterHandler(defaultHandler);
        });
        services.AddInbox("batched-inbox", builder =>
        {
            builder.AsBatched()
                .UsePostgres(container.ConnectionString)
                .ConfigureOptions(o => o.PollingInterval = TimeSpan.FromMilliseconds(50))
                .RegisterHandler(batchedHandler);
        });
        return services.BuildServiceProvider();
    }

    private ServiceProvider CreateTwoInboxServiceProvider(
        CountingHandler<SimpleMessage> healthyHandler,
        FailingHandler<SimpleMessage> failingHandler)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox("healthy-inbox", builder =>
        {
            builder.AsDefault()
                .UsePostgres(container.ConnectionString)
                .ConfigureOptions(o => o.PollingInterval = TimeSpan.FromMilliseconds(50))
                .RegisterHandler(healthyHandler);
        });
        services.AddInbox("failing-inbox", builder =>
        {
            builder.AsDefault()
                .UsePostgres(container.ConnectionString)
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    o.MaxAttempts = 3;
                })
                .RegisterHandler(failingHandler);
        });
        return services.BuildServiceProvider();
    }
}
