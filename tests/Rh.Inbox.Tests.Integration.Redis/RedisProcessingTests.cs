using System.Diagnostics;
using Rh.Inbox.Abstractions.Configuration;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Extensions;
using Rh.Inbox.Redis;
using Rh.Inbox.Tests.Integration.Common;
using Rh.Inbox.Tests.Integration.Common.Handlers;
using Rh.Inbox.Tests.Integration.Common.TestMessages;
using Xunit;
using Xunit.Abstractions;

namespace Rh.Inbox.Tests.Integration.Redis;

[Collection("Redis")]
public class RedisProcessingTests(RedisContainerFixture container, ITestOutputHelper output) : IAsyncLifetime
{
    private ServiceProvider? _serviceProvider;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (_serviceProvider is not null)
            await _serviceProvider.DisposeAsync();
    }

    [Fact]
    public async Task EndToEnd_WriteAndProcess_AllMessagesProcessed()
    {
        var handler = new CountingHandler<SimpleMessage>();
        _serviceProvider = CreateServiceProvider("redis-e2e", handler);

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 200;
        var messages = TestMessageFactory.CreateSimpleMessages(messageCount);
        await writer.WriteBatchAsync(messages, "redis-e2e");

        var elapsed = await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, messageCount);

        handler.ProcessedCount.Should().Be(messageCount);
        output.WriteLine($"Processed {messageCount} messages in {elapsed.TotalMilliseconds:F0}ms ({messageCount / elapsed.TotalSeconds:F2} msg/s)");
    }

    [Fact]
    public async Task EndToEnd_HighThroughput_MeasuresPerformance()
    {
        var handler = new BatchedCountingHandler<SimpleMessage>();
        _serviceProvider = CreateBatchedServiceProvider("redis-throughput", handler, o =>
        {
            o.ReadBatchSize = 500;
            o.PollingInterval = TimeSpan.FromMilliseconds(20);
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 5000;
        var messages = TestMessageFactory.CreateSimpleMessages(messageCount);

        var writeSw = Stopwatch.StartNew();
        foreach (var batch in TestMessageFactory.BatchMessages(messages, 200))
        {
            await writer.WriteBatchAsync(batch, "redis-throughput");
        }
        writeSw.Stop();

        var elapsed = await TestWaitHelper.WaitForCountAsync(
            () => handler.ProcessedCount, messageCount, TestConstants.LongProcessingTimeout);

        handler.ProcessedCount.Should().Be(messageCount);
        output.WriteLine($"Write: {messageCount} messages in {writeSw.ElapsedMilliseconds}ms ({messageCount / writeSw.Elapsed.TotalSeconds:F2} msg/s)");
        output.WriteLine($"Process: {messageCount} messages in {elapsed.TotalMilliseconds:F0}ms ({messageCount / elapsed.TotalSeconds:F2} msg/s)");
    }

    private ServiceProvider CreateServiceProvider(
        string inboxName,
        CountingHandler<SimpleMessage> handler,
        Action<IConfigureInboxOptions>? configureOptions = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox(inboxName, builder =>
        {
            builder.AsDefault()
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

    private ServiceProvider CreateBatchedServiceProvider(
        string inboxName,
        BatchedCountingHandler<SimpleMessage> handler,
        Action<IConfigureInboxOptions>? configureOptions = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox(inboxName, builder =>
        {
            builder.AsBatched()
                .UseRedis(container.ConnectionString)
                .ConfigureOptions(o => configureOptions?.Invoke(o))
                .RegisterHandler(handler);
        });
        return services.BuildServiceProvider();
    }
}
