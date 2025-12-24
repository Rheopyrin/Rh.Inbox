using System.Diagnostics;
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
public class PostgresWriteThroughputTests(PostgresContainerFixture container, ITestOutputHelper output) : IAsyncLifetime
{
    private ServiceProvider? _serviceProvider;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (_serviceProvider is not null)
            await _serviceProvider.DisposeAsync();
    }

    [Fact]
    public async Task WriteAsync_SingleMessages_MeasuresThroughput()
    {
        var handler = new CountingHandler<SimpleMessage>();
        _serviceProvider = CreateServiceProvider("write-single", handler);

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 1000;
        var sw = Stopwatch.StartNew();

        for (int i = 0; i < messageCount; i++)
        {
            await writer.WriteAsync(new SimpleMessage($"msg-{i}", $"data-{i}"), "write-single");
        }
        sw.Stop();

        var messagesPerSecond = messageCount / sw.Elapsed.TotalSeconds;
        output.WriteLine($"Single write: {messagesPerSecond:F2} msg/s ({sw.ElapsedMilliseconds}ms, avg {sw.ElapsedMilliseconds / (double)messageCount:F2}ms/msg)");
        messagesPerSecond.Should().BeGreaterThan(100);
    }

    [Fact]
    public async Task WriteBatchAsync_BatchMessages_MeasuresThroughput()
    {
        var handler = new CountingHandler<SimpleMessage>();
        _serviceProvider = CreateServiceProvider("write-batch", handler);

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 5000;
        var messages = TestMessageFactory.CreateSimpleMessages(messageCount);

        var sw = Stopwatch.StartNew();
        foreach (var batch in TestMessageFactory.BatchMessages(messages, 100))
        {
            await writer.WriteBatchAsync(batch, "write-batch");
        }
        sw.Stop();

        var messagesPerSecond = messageCount / sw.Elapsed.TotalSeconds;
        output.WriteLine($"Batch write: {messagesPerSecond:F2} msg/s ({sw.ElapsedMilliseconds}ms for {messageCount} messages)");
        messagesPerSecond.Should().BeGreaterThan(1000);
    }

    [Fact]
    public async Task WriteBatchAsync_ConcurrentWriters_MeasuresThroughput()
    {
        var handler = new CountingHandler<SimpleMessage>();
        _serviceProvider = CreateServiceProvider("write-concurrent", handler);

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int writerCount = 5;
        const int messagesPerWriter = 1000;

        var sw = Stopwatch.StartNew();
        var tasks = Enumerable.Range(0, writerCount).Select(async writerId =>
        {
            var messages = TestMessageFactory.CreateSimpleMessages(messagesPerWriter, $"w{writerId}");
            foreach (var batch in TestMessageFactory.BatchMessages(messages, 50))
            {
                await writer.WriteBatchAsync(batch, "write-concurrent");
            }
        });
        await Task.WhenAll(tasks);
        sw.Stop();

        var totalMessages = writerCount * messagesPerWriter;
        var messagesPerSecond = totalMessages / sw.Elapsed.TotalSeconds;
        output.WriteLine($"Concurrent write ({writerCount} writers): {messagesPerSecond:F2} msg/s ({sw.ElapsedMilliseconds}ms for {totalMessages} messages)");
        messagesPerSecond.Should().BeGreaterThan(2000);
    }

    private ServiceProvider CreateServiceProvider(string inboxName, CountingHandler<SimpleMessage> handler)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox(inboxName, builder =>
        {
            builder.AsDefault()
                .UsePostgres(container.ConnectionString)
                .RegisterHandler(handler);
        });
        return services.BuildServiceProvider();
    }
}
