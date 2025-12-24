using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Extensions;
using Rh.Inbox.InMemory;
using Rh.Inbox.Tests.Integration.Common;
using Rh.Inbox.Tests.Integration.Common.TestMessages;
using Xunit;
using Xunit.Abstractions;

namespace Rh.Inbox.Tests.Integration.InMemory;

public class InMemoryDeduplicationTests(ITestOutputHelper output) : IAsyncLifetime
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
    public async Task Deduplication_DuplicateMessages_OnlyFirstProcessed()
    {
        var handler = new DeduplicationTrackingHandler();
        _serviceProvider = CreateServiceProviderWithDeduplication("dedup-test", handler);

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const string deduplicationId = "unique-message-id";
        const int duplicateCount = 5;

        for (int i = 0; i < duplicateCount; i++)
        {
            await writer.WriteAsync(new DeduplicatableMessage(deduplicationId, $"data-{i}"), "dedup-test");
        }

        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 1);
        await Task.Delay(200); // Extra time for any duplicates

        handler.ProcessedCount.Should().Be(1, "duplicate messages should be deduplicated");
        handler.ProcessedDeduplicationIds.Should().ContainSingle().Which.Should().Be(deduplicationId);
        output.WriteLine($"Sent {duplicateCount} duplicate messages, processed {handler.ProcessedCount}");
    }

    [Fact]
    public async Task Deduplication_DifferentDeduplicationIds_AllProcessed()
    {
        var handler = new DeduplicationTrackingHandler();
        _serviceProvider = CreateServiceProviderWithDeduplication("dedup-different", handler);

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 10;
        for (int i = 0; i < messageCount; i++)
        {
            await writer.WriteAsync(new DeduplicatableMessage($"unique-id-{i}", $"data-{i}"), "dedup-different");
        }

        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, messageCount);

        handler.ProcessedCount.Should().Be(messageCount, "messages with different deduplication IDs should all be processed");
        output.WriteLine($"Processed {handler.ProcessedCount} messages with unique deduplication IDs");
    }

    [Fact]
    public async Task Deduplication_BatchWithDuplicates_OnlyUniqueProcessed()
    {
        var handler = new DeduplicationTrackingHandler();
        _serviceProvider = CreateServiceProviderWithDeduplication("dedup-batch", handler);

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        var messages = new List<DeduplicatableMessage>
        {
            new("id-1", "data-1"),
            new("id-2", "data-2"),
            new("id-1", "data-1-duplicate"),
            new("id-3", "data-3"),
            new("id-2", "data-2-duplicate"),
        };

        await writer.WriteBatchAsync(messages, "dedup-batch");

        await TestWaitHelper.WaitForCountAsync(() => handler.ProcessedCount, 3);
        await Task.Delay(200);

        handler.ProcessedCount.Should().Be(3, "only unique messages should be processed");
        handler.ProcessedDeduplicationIds.Should().BeEquivalentTo(["id-1", "id-2", "id-3"]);
        output.WriteLine($"Batch had 5 messages (3 unique), processed {handler.ProcessedCount}");
    }

    private ServiceProvider CreateServiceProviderWithDeduplication(string inboxName, DeduplicationTrackingHandler handler)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox(inboxName, builder =>
        {
            builder.AsDefault()
                .UseInMemory()
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(20);
                    o.DeduplicationInterval = TimeSpan.FromMinutes(5);
                    o.EnableDeduplication = true;
                })
                .RegisterHandler(handler);
        });
        return services.BuildServiceProvider();
    }

    private class DeduplicationTrackingHandler : IInboxHandler<DeduplicatableMessage>
    {
        private int _processedCount;
        private readonly List<string> _processedDeduplicationIds = [];

        public int ProcessedCount => _processedCount;
        public IReadOnlyList<string> ProcessedDeduplicationIds
        {
            get { lock (_processedDeduplicationIds) return [.. _processedDeduplicationIds]; }
        }

        public Task<InboxHandleResult> HandleAsync(InboxMessageEnvelope<DeduplicatableMessage> message, CancellationToken token)
        {
            Interlocked.Increment(ref _processedCount);
            lock (_processedDeduplicationIds)
            {
                _processedDeduplicationIds.Add(message.Payload.DeduplicationId);
            }
            return Task.FromResult(InboxHandleResult.Success);
        }
    }
}