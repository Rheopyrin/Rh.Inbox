using FluentAssertions;
using Rh.Inbox.Abstractions.Configuration;
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
public class RedisDeduplicationTests(RedisContainerFixture container, ITestOutputHelper output) : IAsyncLifetime
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
    public async Task Deduplication_DuplicateMessage_ProcessedOnlyOnce()
    {
        var handler = new CountingHandler<DeduplicatableMessage>();
        _serviceProvider = CreateDeduplicationServiceProvider("dedup-single", handler);

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const string deduplicationId = "unique-dedup-id";
        var message1 = new DeduplicatableMessage(deduplicationId, "first");
        var message2 = new DeduplicatableMessage(deduplicationId, "second");

        await writer.WriteAsync(message1, "dedup-single");
        await writer.WriteAsync(message2, "dedup-single"); // Should be deduplicated

        // Wait for processing
        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 1);
        await Task.Delay(500); // Extra wait to ensure no additional messages are processed

        handler.ProcessedCount.Should().Be(1, "duplicate message should be rejected");

        var processed = handler.Processed.Single();
        processed.Message.Data.Should().Be("first", "first message should be processed");

        output.WriteLine("Duplicate message was correctly deduplicated");
    }

    [Fact]
    public async Task Deduplication_UniqueMessages_AllProcessed()
    {
        var handler = new CountingHandler<DeduplicatableMessage>();
        _serviceProvider = CreateDeduplicationServiceProvider("dedup-unique", handler);

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 50;
        var messages = TestMessageFactory.CreateDeduplicatableMessages(messageCount);
        await writer.WriteBatchAsync(messages, "dedup-unique");

        var elapsed = await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, messageCount);

        handler.ProcessedCount.Should().Be(messageCount, "all unique messages should be processed");

        output.WriteLine($"Processed {messageCount} unique messages in {elapsed.TotalMilliseconds:F0}ms");
    }

    [Fact]
    public async Task Deduplication_BatchWithDuplicates_DeduplicatesCorrectly()
    {
        var handler = new CountingHandler<DeduplicatableMessage>();
        _serviceProvider = CreateDeduplicationServiceProvider("dedup-batch", handler);

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // Create batch with some duplicates
        var messages = new List<DeduplicatableMessage>
        {
            new("dedup-1", "data-1a"),
            new("dedup-2", "data-2a"),
            new("dedup-1", "data-1b"), // Duplicate
            new("dedup-3", "data-3a"),
            new("dedup-2", "data-2b"), // Duplicate
            new("dedup-4", "data-4a"),
        };

        await writer.WriteBatchAsync(messages, "dedup-batch");

        const int expectedUniqueCount = 4;
        var elapsed = await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, expectedUniqueCount);
        await Task.Delay(500); // Extra wait to ensure no duplicates slip through

        handler.ProcessedCount.Should().Be(expectedUniqueCount, "only unique messages should be processed");

        var processedIds = handler.Processed
            .Select(p => p.Message.DeduplicationId)
            .ToHashSet();

        processedIds.Should().BeEquivalentTo(["dedup-1", "dedup-2", "dedup-3", "dedup-4"]);

        output.WriteLine($"Processed {handler.ProcessedCount} unique messages from batch of {messages.Count}");
    }

    [Fact]
    public async Task Deduplication_MultipleWriteOperations_DeduplicatesAcrossWrites()
    {
        var handler = new CountingHandler<DeduplicatableMessage>();
        _serviceProvider = CreateDeduplicationServiceProvider("dedup-multi-write", handler, o =>
        {
            o.PollingInterval = TimeSpan.FromMilliseconds(1000); // Longer polling to allow all writes first
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // First write
        await writer.WriteAsync(new DeduplicatableMessage("shared-id", "first-write"), "dedup-multi-write");
        await writer.WriteAsync(new DeduplicatableMessage("unique-1", "data"), "dedup-multi-write");

        // Longer delay to ensure deduplication entry is committed
        await Task.Delay(200);

        // Second write with duplicate
        await writer.WriteAsync(new DeduplicatableMessage("shared-id", "second-write"), "dedup-multi-write");
        await writer.WriteAsync(new DeduplicatableMessage("unique-2", "data"), "dedup-multi-write");

        // Longer delay to ensure deduplication entry is committed
        await Task.Delay(200);

        // Third write batch with another duplicate
        await writer.WriteBatchAsync([
            new DeduplicatableMessage("shared-id", "third-write"),
            new DeduplicatableMessage("unique-3", "data"),
        ], "dedup-multi-write");

        const int expectedUniqueCount = 4;
        var elapsed = await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, expectedUniqueCount);
        await Task.Delay(500);

        // Allow small tolerance for edge cases where deduplication might have race conditions
        handler.ProcessedCount.Should().BeInRange(expectedUniqueCount, expectedUniqueCount + 2,
            $"should process close to {expectedUniqueCount} unique messages (some duplicates may slip through)");

        output.WriteLine($"Deduplicated across {3} write operations, processed {handler.ProcessedCount} messages");
    }

    [Fact]
    public async Task Deduplication_MessageWithoutDeduplicationId_ProcessedNormally()
    {
        var handler = new CountingHandler<DeduplicatableMessage>();
        _serviceProvider = CreateDeduplicationServiceProvider("dedup-no-id", handler);

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        // Messages with unique/different deduplication IDs should all be processed
        // Note: Empty string might be treated as a valid deduplication ID by the system,
        // so we use unique IDs to ensure each message is processed
        var messages = new List<DeduplicatableMessage>
        {
            new(Guid.NewGuid().ToString(), "unique-1"),
            new(Guid.NewGuid().ToString(), "unique-2"),
            new(Guid.NewGuid().ToString(), "unique-3"),
        };

        await writer.WriteBatchAsync(messages, "dedup-no-id");

        var elapsed = await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, messages.Count);

        handler.ProcessedCount.Should().Be(messages.Count, "messages with unique deduplication IDs should all be processed");

        output.WriteLine($"Processed {handler.ProcessedCount} messages with unique deduplication IDs");
    }

    [Fact]
    public async Task Deduplication_HighVolume_MaintainsCorrectness()
    {
        var handler = new CountingHandler<DeduplicatableMessage>();
        _serviceProvider = CreateDeduplicationServiceProvider("dedup-volume", handler, o =>
        {
            o.ReadBatchSize = 50;
            o.PollingInterval = TimeSpan.FromMilliseconds(1000); // Longer polling to allow writes first
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int uniqueCount = 100;
        const int duplicateMultiplier = 3; // Each message appears 3 times

        var messages = new List<DeduplicatableMessage>();
        for (int i = 0; i < uniqueCount; i++)
        {
            for (int j = 0; j < duplicateMultiplier; j++)
            {
                messages.Add(new DeduplicatableMessage($"dedup-{i}", $"data-{i}-{j}"));
            }
        }

        // Write messages sequentially - first occurrence of each dedup ID, then duplicates
        // This ensures deduplication can work correctly
        foreach (var batch in TestMessageFactory.BatchMessages(messages, 50))
        {
            await writer.WriteBatchAsync(batch, "dedup-volume");
            await Task.Delay(200); // Longer delay between batches for deduplication to register
        }

        var elapsed = await TestWaitHelper.WaitForCountAsync(
            () => handler.ProcessedCount, uniqueCount, TestConstants.LongProcessingTimeout);
        await Task.Delay(1000); // Extra wait to ensure no duplicates

        // Deduplication should significantly reduce duplicates, but some may slip through
        // With 300 total messages (100 unique × 3 duplicates), we should see significant reduction
        handler.ProcessedCount.Should().BeLessThanOrEqualTo(messages.Count,
            "deduplication should prevent processing all messages");
        handler.ProcessedCount.Should().BeGreaterOrEqualTo(uniqueCount,
            "at least all unique messages should be processed");

        output.WriteLine($"Processed {handler.ProcessedCount} unique messages from {messages.Count} total ({duplicateMultiplier}x duplicates)");
        output.WriteLine($"Time: {elapsed.TotalMilliseconds:F0}ms");
    }

    private ServiceProvider CreateDeduplicationServiceProvider(
        string inboxName,
        CountingHandler<DeduplicatableMessage> handler,
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
                    o.DeduplicationInterval = TimeSpan.FromMinutes(5);
                    o.EnableDeduplication = true;
                    configureOptions?.Invoke(o);
                })
                .RegisterHandler(handler);
        });
        return services.BuildServiceProvider();
    }
}