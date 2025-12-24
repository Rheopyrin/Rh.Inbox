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
public class RedisMultiInboxTests(RedisContainerFixture container, ITestOutputHelper output) : IAsyncLifetime
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
        const int messagesPerInbox = 300;
        var handlers = Enumerable.Range(1, 4)
            .Select(i => new CountingHandler<SimpleMessage>($"redis-inbox{i}"))
            .ToArray();

        _serviceProvider = CreateMultiInboxServiceProvider(handlers);

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        var writeTasks = new List<Task>();
        for (int i = 0; i < messagesPerInbox; i++)
        {
            for (int j = 0; j < 4; j++)
            {
                writeTasks.Add(writer.WriteAsync(new SimpleMessage($"msg{j + 1}-{i}", "data"), $"redis-inbox{j + 1}"));
            }
        }
        await Task.WhenAll(writeTasks);

        var elapsed = await TestWaitHelper.WaitForAllAsync(
            handlers.Select<CountingHandler<SimpleMessage>, Func<int>>(h => () => h.ProcessedCount).ToList(),
            messagesPerInbox);

        foreach (var handler in handlers)
        {
            handler.ProcessedCount.Should().Be(messagesPerInbox);
        }

        var totalMessages = messagesPerInbox * 4;
        output.WriteLine($"Processed {totalMessages} messages across 4 Redis inboxes in {elapsed.TotalMilliseconds:F0}ms ({totalMessages / elapsed.TotalSeconds:F2} msg/s)");
    }

    private ServiceProvider CreateMultiInboxServiceProvider(CountingHandler<SimpleMessage>[] handlers)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        for (int i = 0; i < handlers.Length; i++)
        {
            var handler = handlers[i];
            var inboxName = $"redis-inbox{i + 1}";
            services.AddInbox(inboxName, builder =>
            {
                builder.AsDefault()
                    .UseRedis(container.ConnectionString)
                    .ConfigureOptions(o => o.PollingInterval = TimeSpan.FromMilliseconds(30))
                    .RegisterHandler(handler);
            });
        }
        return services.BuildServiceProvider();
    }
}
