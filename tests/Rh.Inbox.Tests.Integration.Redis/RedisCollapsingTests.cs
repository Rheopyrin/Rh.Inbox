using FluentAssertions;
using Rh.Inbox.Abstractions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Extensions;
using Rh.Inbox.Redis;
using Rh.Inbox.Tests.Integration.Common;
using Rh.Inbox.Tests.Integration.Common.Handlers;
using Rh.Inbox.Tests.Integration.Common.TestMessages;
using Xunit;
using Xunit.Abstractions;

namespace Rh.Inbox.Tests.Integration.Redis;

[Collection("Redis")]
public class RedisCollapsingTests(RedisContainerFixture container, ITestOutputHelper output) : IAsyncLifetime
{
    private ServiceProvider? _serviceProvider;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (_serviceProvider is not null)
        {
            await _serviceProvider.DisposeAsync();
        }
    }

    [Fact]
    public async Task Collapsing_NewerMessageReplacesOlder_OnlyLatestProcessed()
    {
        var handler = new CountingHandler<CollapsibleMessage>();
        _serviceProvider = CreateCollapsingServiceProvider("collapse-basic", handler, o =>
        {
            o.PollingInterval = TimeSpan.FromMilliseconds(500); // Longer interval to allow batch write before processing
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const string collapseKey = "entity-123";

        // Write multiple versions as batch - they should collapse
        var messages = new List<CollapsibleMessage>
        {
            new(collapseKey, 1, "version-1"),
            new(collapseKey, 2, "version-2"),
            new(collapseKey, 3, "version-3")
        };
        await writer.WriteBatchAsync(messages, "collapse-basic");

        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 1);
        await Task.Delay(500); // Extra wait to ensure no additional messages

        handler.ProcessedCount.Should().Be(1, "older versions should be collapsed");

        var processed = handler.Processed.Single();
        processed.Message.Version.Should().Be(3, "only the latest version should be processed");
        processed.Message.Data.Should().Be("version-3");

        output.WriteLine("Collapsing: Only latest version (v3) was processed");
    }

    [Fact]
    public async Task Collapsing_DifferentCollapseKeys_ProcessedIndependently()
    {
        var handler = new CountingHandler<CollapsibleMessage>();
        _serviceProvider = CreateCollapsingServiceProvider("collapse-independent", handler, o =>
        {
            o.PollingInterval = TimeSpan.FromMilliseconds(500); // Longer interval to allow batch write before processing
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // Write messages with different collapse keys as a batch
        var messages = new List<CollapsibleMessage>
        {
            new("entity-1", 1, "e1-v1"),
            new("entity-2", 1, "e2-v1"),
            new("entity-1", 2, "e1-v2"),
            new("entity-3", 1, "e3-v1"),
            new("entity-2", 2, "e2-v2")
        };
        await writer.WriteBatchAsync(messages, "collapse-independent");

        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 3);
        await Task.Delay(500);

        handler.ProcessedCount.Should().Be(3, "should process one message per collapse key");

        var processedByKey = handler.Processed
            .ToDictionary(p => p.Message.CollapseKey, p => p.Message);

        processedByKey["entity-1"].Version.Should().Be(2);
        processedByKey["entity-2"].Version.Should().Be(2);
        processedByKey["entity-3"].Version.Should().Be(1);

        output.WriteLine($"Processed {handler.ProcessedCount} messages (one per collapse key)");
    }

    [Fact]
    public async Task Collapsing_MessageWithoutCollapseKey_ProcessedNormally()
    {
        var handler = new CountingHandler<CollapsibleMessage>();
        _serviceProvider = CreateCollapsingServiceProvider("collapse-no-key", handler);

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // Messages without collapse key should not be collapsed
        var messages = new List<CollapsibleMessage>
        {
            new("", 1, "no-key-1"),
            new("", 2, "no-key-2"),
            new("", 3, "no-key-3"),
        };

        await writer.WriteBatchAsync(messages, "collapse-no-key");

        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 3);

        handler.ProcessedCount.Should().Be(3, "messages without collapse key should all be processed");

        output.WriteLine($"Processed {handler.ProcessedCount} messages without collapse keys");
    }

    [Fact]
    public async Task Collapsing_CapturedMessageNotCollapsed_BothProcessed()
    {
        var handler = new DelayedHandler<CollapsibleMessage>(TimeSpan.FromMilliseconds(300));
        _serviceProvider = CreateCollapsingServiceProvider("collapse-captured", handler, o =>
        {
            o.ReadBatchSize = 1; // Process one at a time
            o.PollingInterval = TimeSpan.FromMilliseconds(20);
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const string collapseKey = "entity-captured";

        // Write first message and wait for it to be captured
        await writer.WriteAsync(new CollapsibleMessage(collapseKey, 1, "v1"), "collapse-captured");
        await Task.Delay(100); // Wait for capture

        // Write second message - should NOT collapse the first since it's already captured
        await writer.WriteAsync(new CollapsibleMessage(collapseKey, 2, "v2"), "collapse-captured");

        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 2, TimeSpan.FromSeconds(5));

        handler.ProcessedCount.Should().Be(2, "captured message should not be collapsed");

        output.WriteLine("Both versions processed (first was already captured when second arrived)");
    }

    [Fact]
    public async Task Collapsing_BatchWrite_CollapsesWithinBatch()
    {
        var handler = new CountingHandler<CollapsibleMessage>();
        _serviceProvider = CreateCollapsingServiceProvider("collapse-batch", handler, o =>
        {
            o.PollingInterval = TimeSpan.FromMilliseconds(500); // Longer interval to allow batch write before processing
            o.ReadBatchSize = 100; // Ensure all messages are read in one batch for collapsing
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // Batch with multiple versions of same collapse key
        var messages = new List<CollapsibleMessage>
        {
            new("key-1", 1, "k1-v1"),
            new("key-2", 1, "k2-v1"),
            new("key-1", 2, "k1-v2"),
            new("key-3", 1, "k3-v1"),
            new("key-1", 3, "k1-v3"),
            new("key-2", 2, "k2-v2"),
        };

        await writer.WriteBatchAsync(messages, "collapse-batch");

        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 3);
        await Task.Delay(500);

        handler.ProcessedCount.Should().Be(3, "should collapse to one per key");

        var processedByKey = handler.Processed
            .ToDictionary(p => p.Message.CollapseKey, p => p.Message);

        processedByKey["key-1"].Version.Should().Be(3);
        processedByKey["key-2"].Version.Should().Be(2);
        processedByKey["key-3"].Version.Should().Be(1);

        output.WriteLine($"Batch of {messages.Count} collapsed to {handler.ProcessedCount} messages");
    }

    [Fact]
    public async Task Collapsing_HighVolume_CollapsesEfficiently()
    {
        var handler = new CountingHandler<CollapsibleMessage>();
        _serviceProvider = CreateCollapsingServiceProvider("collapse-volume", handler, o =>
        {
            o.ReadBatchSize = 1000; // Large batch size to ensure collapsing within batch
            o.PollingInterval = TimeSpan.FromMilliseconds(1000); // Longer interval to allow all writes before first read
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int entityCount = 50;
        const int versionsPerEntity = 10;
        var messages = new List<CollapsibleMessage>();

        // Create many versions per entity
        for (int v = 1; v <= versionsPerEntity; v++)
        {
            for (int e = 0; e < entityCount; e++)
            {
                messages.Add(new CollapsibleMessage($"entity-{e}", v, $"e{e}-v{v}"));
            }
        }

        // Write all messages in a single batch to ensure they can be collapsed
        await writer.WriteBatchAsync(messages, "collapse-volume");

        var elapsed = await TestWaitHelper.WaitForCountAsync(
            () => handler.ProcessedCount, entityCount, TestConstants.LongProcessingTimeout);
        await Task.Delay(1000);

        handler.ProcessedCount.Should().Be(entityCount, "should collapse to one per entity");

        // Verify all processed messages are the latest version
        foreach (var processed in handler.Processed)
        {
            processed.Message.Version.Should().Be(versionsPerEntity,
                $"entity should have latest version ({versionsPerEntity})");
        }

        output.WriteLine($"Collapsed {messages.Count} messages to {handler.ProcessedCount} ({versionsPerEntity}x reduction)");
        output.WriteLine($"Time: {elapsed.TotalMilliseconds:F0}ms");
    }

    private ServiceProvider CreateCollapsingServiceProvider<THandler>(
        string inboxName,
        THandler handler,
        Action<IConfigureInboxOptions>? configureOptions = null)
        where THandler : class, IInboxHandler<CollapsibleMessage>
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
                .RegisterHandler<CollapsibleMessage>(handler);
        });
        return services.BuildServiceProvider();
    }
}
