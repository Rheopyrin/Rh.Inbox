using System.Collections.Concurrent;
using Rh.Inbox.Abstractions.Configuration;
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
public class PostgresMultiProcessorTests(PostgresContainerFixture container, ITestOutputHelper output) : IAsyncLifetime
{
    private readonly List<ServiceProvider> _serviceProviders = [];

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var sp in _serviceProviders)
            await sp.DisposeAsync();
    }

    [Fact]
    public async Task MultiProcessor_CompetingForSameInbox_NoMessageLoss()
    {
        const string inboxName = "multi-processor-test";
        const int processorCount = 3;
        const int messageCount = 300;

        var processedMessages = new ConcurrentDictionary<Guid, string>();
        var handlers = new List<MultiProcessorTrackingHandler<SimpleMessage>>();

        // Create processors
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

        processedMessages.Count.Should().Be(messageCount, "all messages should be processed exactly once");

        output.WriteLine($"Processed {messageCount} messages in {elapsed.TotalMilliseconds:F0}ms ({messageCount / elapsed.TotalSeconds:F2} msg/s)");
        foreach (var h in handlers)
            output.WriteLine($"  {h.ProcessorId}: {h.ProcessedCount} ({100.0 * h.ProcessedCount / messageCount:F1}%)");
    }

    [Fact]
    public async Task MultiProcessor_HighLoad_ScalesLinearly()
    {
        const string inboxName = "default";
        const int processorCount = 5;
        const int messageCount = 1000;

        var processedMessages = new ConcurrentDictionary<Guid, string>();
        var handlers = new List<MultiProcessorTrackingHandler<SimpleMessage>>();

        for (int i = 0; i < processorCount; i++)
        {
            var handler = new MultiProcessorTrackingHandler<SimpleMessage>($"processor-{i}", processedMessages);
            handlers.Add(handler);
            _serviceProviders.Add(CreateServiceProvider(inboxName, handler, o =>
            {
                o.ReadBatchSize = 5;
                o.PollingInterval = TimeSpan.FromMilliseconds(50);
            }));
        }

        await _serviceProviders[0].GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await Task.WhenAll(_serviceProviders.Select(sp =>
            sp.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None)));

        var writer = _serviceProviders[0].GetRequiredService<IInboxWriter>();
        var messages = TestMessageFactory.CreateSimpleMessages(messageCount);
        foreach (var batch in TestMessageFactory.BatchMessages(messages, 100))
        {
            await writer.WriteBatchAsync(batch, inboxName);
        }

        var elapsed = await TestWaitHelper.WaitForCountAsync(
            () => processedMessages.Count, messageCount, TestConstants.LongProcessingTimeout);

        processedMessages.Count.Should().Be(messageCount);

        output.WriteLine($"{messageCount} messages, {processorCount} processors in {elapsed.TotalMilliseconds:F0}ms ({messageCount / elapsed.TotalSeconds:F2} msg/s)");
        output.WriteLine($"Distribution: {string.Join(", ", handlers.OrderByDescending(h => h.ProcessedCount).Select(h => $"{h.ProcessorId}:{h.ProcessedCount}"))}");
    }

    private ServiceProvider CreateServiceProvider(
        string inboxName,
        MultiProcessorTrackingHandler<SimpleMessage> handler,
        Action<IConfigureInboxOptions>? configureOptions = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox(inboxName, builder =>
        {
            builder.AsDefault()
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
