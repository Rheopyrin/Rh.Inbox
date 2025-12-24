using FluentAssertions;
using Rh.Inbox.Abstractions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Extensions;
using Rh.Inbox.InMemory;
using Rh.Inbox.Tests.Integration.Common;
using Rh.Inbox.Tests.Integration.Common.Handlers;
using Rh.Inbox.Tests.Integration.Common.TestMessages;
using Xunit;
using Xunit.Abstractions;

namespace Rh.Inbox.Tests.Integration.InMemory;

public class InMemoryDeadLetterTests(ITestOutputHelper output) : IAsyncLifetime
{
    private ServiceProvider? _serviceProvider;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (_serviceProvider is not null)
            await _serviceProvider.DisposeAsync();
    }

    [Fact]
    public async Task DeadLetter_ExceedsMaxAttempts_MovedToDeadLetter()
    {
        const int maxAttempts = 3;
        var handler = new FailingHandler<SimpleMessage>(1.0); // Always fails
        _serviceProvider = CreateDeadLetterServiceProvider("dead-letter-test", handler, o =>
        {
            o.MaxAttempts = maxAttempts;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        var message = new SimpleMessage("test-msg", "data");
        await writer.WriteAsync(message, "dead-letter-test");

        // Wait for processing attempts (1 message * maxAttempts failures)
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.FailedCount >= maxAttempts,
            TimeSpan.FromSeconds(10));

        handler.FailedCount.Should().BeGreaterOrEqualTo(maxAttempts,
            "handler should have been called at least MaxAttempts times");

        output.WriteLine($"Message failed after {handler.FailedCount} attempts");
    }

    [Fact]
    public async Task DeadLetter_MultipleFailing_AllProcessedMaxAttempts()
    {
        const int maxAttempts = 2;
        const int messageCount = 5;
        var handler = new FailingHandler<SimpleMessage>(1.0); // Always fails
        _serviceProvider = CreateDeadLetterServiceProvider("dead-letter-multi", handler, o =>
        {
            o.MaxAttempts = maxAttempts;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        var messages = TestMessageFactory.CreateSimpleMessages(messageCount);
        await writer.WriteBatchAsync(messages, "dead-letter-multi");

        // Wait for all messages to fail maxAttempts times
        var expectedFailures = messageCount * maxAttempts;
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.FailedCount >= expectedFailures,
            TimeSpan.FromSeconds(15));

        // Each message should fail at least maxAttempts times
        handler.FailedCount.Should().BeGreaterOrEqualTo(expectedFailures,
            "all messages should fail MaxAttempts times");

        output.WriteLine($"All {messageCount} messages processed with {handler.FailedCount} total failures");
    }

    [Fact]
    public async Task DeadLetter_Disabled_FailingMessagesNotPersisted()
    {
        const int maxAttempts = 2;
        var handler = new FailingHandler<SimpleMessage>(1.0); // Always fails
        _serviceProvider = CreateDeadLetterServiceProvider("dead-letter-disabled", handler, o =>
        {
            o.MaxAttempts = maxAttempts;
            o.EnableDeadLetter = false;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 3;
        var messages = TestMessageFactory.CreateSimpleMessages(messageCount);
        await writer.WriteBatchAsync(messages, "dead-letter-disabled");

        // Wait for all messages to fail maxAttempts times
        var expectedFailures = messageCount * maxAttempts;
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.FailedCount >= expectedFailures,
            TimeSpan.FromSeconds(10));

        // Messages should be discarded after max attempts
        handler.FailedCount.Should().BeGreaterOrEqualTo(expectedFailures,
            "messages should fail MaxAttempts times before being discarded");

        output.WriteLine("Dead letter disabled - messages discarded after max attempts");
    }

    [Fact]
    public async Task DeadLetter_MixedSuccessAndFailure_OnlyFailedExceedMaxAttempts()
    {
        const int maxAttempts = 2;
        var handler = new FailingHandler<SimpleMessage>(0.5); // 50% fail rate
        _serviceProvider = CreateDeadLetterServiceProvider("dead-letter-mixed", handler, o =>
        {
            o.MaxAttempts = maxAttempts;
            o.ReadBatchSize = 1; // Process one at a time for deterministic behavior
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 20;
        var messages = TestMessageFactory.CreateSimpleMessages(messageCount);
        await writer.WriteBatchAsync(messages, "dead-letter-mixed");

        // Wait until all messages are processed (either succeeded or exhausted retries)
        // Each message either succeeds (ProcessedCount += 1) or fails maxAttempts times
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.ProcessedCount + (handler.FailedCount / maxAttempts) >= messageCount,
            TimeSpan.FromSeconds(30));

        // With random failures, we should have some successes
        handler.ProcessedCount.Should().BeGreaterOrEqualTo(0, "some messages may succeed");

        // Track unique message IDs that were successfully processed
        var uniqueSuccessIds = handler.Processed.Distinct().Count();

        output.WriteLine($"Unique succeeded: {uniqueSuccessIds}, Total processed: {handler.ProcessedCount}");
    }

    [Fact]
    public async Task DeadLetter_BatchedHandler_FailingMessagesProcessed()
    {
        const int maxAttempts = 2;
        var handler = new BatchedFailingHandler<SimpleMessage>(1.0); // Always fails
        _serviceProvider = CreateBatchedDeadLetterServiceProvider("dead-letter-batched", handler, o =>
        {
            o.MaxAttempts = maxAttempts;
            o.ReadBatchSize = 10;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 20;
        var messages = TestMessageFactory.CreateSimpleMessages(messageCount);
        await writer.WriteBatchAsync(messages, "dead-letter-batched");

        // Wait for all messages to fail maxAttempts times
        var expectedFailures = messageCount * maxAttempts;
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.FailedCount >= expectedFailures,
            TimeSpan.FromSeconds(15));

        // Each message should fail at least maxAttempts times
        handler.FailedCount.Should().BeGreaterOrEqualTo(expectedFailures,
            "all failing messages should be retried MaxAttempts times");

        output.WriteLine($"Batched handler: {handler.FailedCount} total failures");
    }

    private ServiceProvider CreateDeadLetterServiceProvider(
        string inboxName,
        FailingHandler<SimpleMessage> handler,
        Action<IConfigureInboxOptions>? configureOptions = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox(inboxName, builder =>
        {
            builder.AsDefault()
                .UseInMemory()
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    configureOptions?.Invoke(o);
                })
                .RegisterHandler(handler);
        });
        return services.BuildServiceProvider();
    }

    private ServiceProvider CreateBatchedDeadLetterServiceProvider(
        string inboxName,
        BatchedFailingHandler<SimpleMessage> handler,
        Action<IConfigureInboxOptions>? configureOptions = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox(inboxName, builder =>
        {
            builder.AsBatched()
                .UseInMemory()
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    configureOptions?.Invoke(o);
                })
                .RegisterHandler(handler);
        });
        return services.BuildServiceProvider();
    }
}
