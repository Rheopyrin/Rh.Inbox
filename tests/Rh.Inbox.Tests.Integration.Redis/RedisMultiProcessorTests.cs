using System.Collections.Concurrent;
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
public class RedisMultiProcessorTests(RedisContainerFixture container, ITestOutputHelper output) : IAsyncLifetime
{
    private readonly List<ServiceProvider> _serviceProviders = [];

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var sp in _serviceProviders)
        {
            await sp.DisposeAsync();
        }
    }

    [Fact]
    public async Task MultiProcessor_CompetingForSameInbox_NoMessageLoss()
    {
        const string inboxName = "redis-multi-processor";
        const int processorCount = 3;
        const int messageCount = 500;

        var processedMessages = new ConcurrentDictionary<Guid, string>();
        var handlers = new List<MultiProcessorTrackingHandler<SimpleMessage>>();

        for (int i = 0; i < processorCount; i++)
        {
            var handler = new MultiProcessorTrackingHandler<SimpleMessage>($"processor-{i}", processedMessages);
            handlers.Add(handler);
            _serviceProviders.Add(CreateServiceProvider(inboxName, handler));
        }

        await _serviceProviders[0].GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await Task.WhenAll(_serviceProviders.Select(sp =>
            sp.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None)));

        var writer = _serviceProviders[0].GetRequiredService<IInboxWriter>();
        await writer.WriteBatchAsync(TestMessageFactory.CreateSimpleMessages(messageCount), inboxName);

        var elapsed = await TestWaitHelper.WaitForCountAsync(
            () => processedMessages.Count, messageCount, TestConstants.LongProcessingTimeout);

        processedMessages.Count.Should().Be(messageCount);

        output.WriteLine($"Processed {messageCount} messages in {elapsed.TotalMilliseconds:F0}ms ({messageCount / elapsed.TotalSeconds:F2} msg/s)");
        foreach (var h in handlers)
        {
            output.WriteLine($"  {h.ProcessorId}: {h.ProcessedCount} ({100.0 * h.ProcessedCount / messageCount:F1}%)");
        }
    }

    private ServiceProvider CreateServiceProvider(string inboxName, MultiProcessorTrackingHandler<SimpleMessage> handler)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox(inboxName, builder =>
        {
            builder.AsDefault()
                .UseRedis(container.ConnectionString)
                .ConfigureOptions(o =>
                {
                    o.ReadBatchSize = 50;
                    o.PollingInterval = TimeSpan.FromMilliseconds(30);
                })
                .RegisterHandler(handler);
        });
        return services.BuildServiceProvider();
    }
}
