using System.Collections.Concurrent;
using Rh.Inbox.Abstractions.Configuration;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Extensions;
using Rh.Inbox.Postgres;
using Rh.Inbox.Tests.Integration.Common;
using Rh.Inbox.Tests.Integration.Common.TestMessages;
using Xunit;
using Xunit.Abstractions;

namespace Rh.Inbox.Tests.Integration.Postgres;

[Collection("Postgres")]
public class PostgresRetryTests(PostgresContainerFixture container, ITestOutputHelper output) : IAsyncLifetime
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
    public async Task Retry_TransientFailure_RetriesAndSucceeds()
    {
        var handler = new TransientFailingHandler<SimpleMessage>(failuresBeforeSuccess: 2);
        _serviceProvider = CreateRetryServiceProvider("retry-transient", handler, o =>
        {
            o.MaxAttempts = 5;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        var message = new SimpleMessage("transient-msg", "data");
        await writer.WriteAsync(message, "retry-transient");

        await TestWaitHelper.WaitForCountAsync(() => handler.SuccessCount, 1, TimeSpan.FromSeconds(10));

        handler.SuccessCount.Should().Be(1, "message should eventually succeed after retries");
        handler.FailureCount.Should().Be(2, "message should fail twice before succeeding");

        output.WriteLine($"Message succeeded after {handler.FailureCount} failures");
    }

    [Fact]
    public async Task Retry_MultipleMessages_EachRetriesIndependently()
    {
        var handler = new TransientFailingHandler<SimpleMessage>(failuresBeforeSuccess: 1);
        _serviceProvider = CreateRetryServiceProvider("retry-multi", handler, o =>
        {
            o.MaxAttempts = 3;
            o.ReadBatchSize = 1; // Process one at a time
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 5;
        var messages = TestMessageFactory.CreateSimpleMessages(messageCount);
        await writer.WriteBatchAsync(messages, "retry-multi");

        await TestWaitHelper.WaitForCountAsync(
            () => handler.SuccessCount, messageCount, TimeSpan.FromSeconds(15));

        handler.SuccessCount.Should().Be(messageCount);
        handler.FailureCount.Should().Be(messageCount, "each message should fail once before succeeding");

        output.WriteLine($"All {messageCount} messages succeeded after retry");
    }

    [Fact]
    public async Task Retry_ExceedsMaxAttempts_GoesToDeadLetter()
    {
        var handler = new TransientFailingHandler<SimpleMessage>(failuresBeforeSuccess: 10); // Will exceed max
        _serviceProvider = CreateRetryServiceProvider("retry-exceed", handler, o =>
        {
            o.MaxAttempts = 3;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        var message = new SimpleMessage("exceed-msg", "data");
        await writer.WriteAsync(message, "retry-exceed");

        // Wait for retries to exhaust
        await Task.Delay(TimeSpan.FromSeconds(5));

        handler.FailureCount.Should().BeGreaterOrEqualTo(3, "message should be retried MaxAttempts times");
        handler.SuccessCount.Should().Be(0, "message should never succeed");

        output.WriteLine($"Message failed after {handler.FailureCount} attempts");
    }

    [Fact]
    public async Task Retry_ReleasedMessage_BecomesAvailableAgain()
    {
        var handler = new ReleaseOnFirstAttemptHandler<SimpleMessage>();
        _serviceProvider = CreateRetryServiceProvider("retry-release", handler, o =>
        {
            o.MaxAttempts = 5;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        var message = new SimpleMessage("release-msg", "data");
        await writer.WriteAsync(message, "retry-release");

        await TestWaitHelper.WaitForCountAsync(() => handler.SuccessCount, 1, TimeSpan.FromSeconds(10));

        handler.ReleaseCount.Should().Be(1, "message should be released once");
        handler.SuccessCount.Should().Be(1, "message should succeed after release");

        output.WriteLine($"Message released {handler.ReleaseCount} time(s), then succeeded");
    }

    [Fact]
    public async Task Retry_BatchedHandler_RetriesFailedMessages()
    {
        var handler = new BatchedTransientFailingHandler<SimpleMessage>(failuresBeforeSuccess: 1);
        _serviceProvider = CreateBatchedRetryServiceProvider("retry-batched", handler, o =>
        {
            o.MaxAttempts = 3;
            o.ReadBatchSize = 5;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 10;
        var messages = TestMessageFactory.CreateSimpleMessages(messageCount);
        await writer.WriteBatchAsync(messages, "retry-batched");

        await TestWaitHelper.WaitForCountAsync(
            () => handler.SuccessCount, messageCount, TimeSpan.FromSeconds(15));

        handler.SuccessCount.Should().Be(messageCount);
        handler.FailureCount.Should().Be(messageCount, "each message should fail once");

        output.WriteLine($"Batched handler: {messageCount} messages succeeded after retry");
    }

    [Fact]
    public async Task Retry_AttemptsCountIncremented_CorrectlyTracked()
    {
        var handler = new AttemptsTrackingHandler<SimpleMessage>(succeedOnAttempt: 3);
        _serviceProvider = CreateRetryServiceProvider("retry-attempts", handler, o =>
        {
            o.MaxAttempts = 5;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        var message = new SimpleMessage("attempts-msg", "data");
        await writer.WriteAsync(message, "retry-attempts");

        await TestWaitHelper.WaitForConditionAsync(
            () => handler.SuccessfulAttemptNumbers.Count > 0, TimeSpan.FromSeconds(10));

        var successfulAttempt = handler.SuccessfulAttemptNumbers.Single();
        successfulAttempt.Should().Be(3, "message should succeed on attempt 3");

        output.WriteLine($"Message succeeded on attempt {successfulAttempt}");
    }

    private ServiceProvider CreateRetryServiceProvider<THandler>(
        string inboxName,
        THandler handler,
        Action<IConfigureInboxOptions>? configureOptions = null)
        where THandler : class, IInboxHandler<SimpleMessage>
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox(inboxName, builder =>
        {
            builder.AsDefault()
                .UsePostgres(container.ConnectionString)
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    configureOptions?.Invoke(o);
                })
                .RegisterHandler<SimpleMessage>(handler);
        });
        return services.BuildServiceProvider();
    }

    private ServiceProvider CreateBatchedRetryServiceProvider<THandler>(
        string inboxName,
        THandler handler,
        Action<IConfigureInboxOptions>? configureOptions = null)
        where THandler : class, IBatchedInboxHandler<SimpleMessage>
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox(inboxName, builder =>
        {
            builder.AsBatched()
                .UsePostgres(container.ConnectionString)
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    configureOptions?.Invoke(o);
                })
                .RegisterHandler<SimpleMessage>(handler);
        });
        return services.BuildServiceProvider();
    }
}

/// <summary>
/// Handler that fails a specified number of times per message before succeeding.
/// </summary>
file class TransientFailingHandler<TMessage> : IInboxHandler<TMessage> where TMessage : class
{
    private readonly int _failuresBeforeSuccess;
    private readonly ConcurrentDictionary<Guid, int> _attemptCounts = new();
    private int _successCount;
    private int _failureCount;

    public TransientFailingHandler(int failuresBeforeSuccess)
    {
        _failuresBeforeSuccess = failuresBeforeSuccess;
    }

    public int SuccessCount => _successCount;
    public int FailureCount => _failureCount;

    public Task<InboxHandleResult> HandleAsync(InboxMessageEnvelope<TMessage> message, CancellationToken token)
    {
        var attempts = _attemptCounts.AddOrUpdate(message.Id, 1, (_, count) => count + 1);

        if (attempts <= _failuresBeforeSuccess)
        {
            Interlocked.Increment(ref _failureCount);
            return Task.FromResult(InboxHandleResult.Failed);
        }

        Interlocked.Increment(ref _successCount);
        return Task.FromResult(InboxHandleResult.Success);
    }
}

/// <summary>
/// Handler that retries the message on first attempt, then succeeds.
/// </summary>
file class ReleaseOnFirstAttemptHandler<TMessage> : IInboxHandler<TMessage> where TMessage : class
{
    private readonly ConcurrentDictionary<Guid, int> _attemptCounts = new();
    private int _releaseCount;
    private int _successCount;

    public int ReleaseCount => _releaseCount;
    public int SuccessCount => _successCount;

    public Task<InboxHandleResult> HandleAsync(InboxMessageEnvelope<TMessage> message, CancellationToken token)
    {
        var attempts = _attemptCounts.AddOrUpdate(message.Id, 1, (_, count) => count + 1);

        if (attempts == 1)
        {
            Interlocked.Increment(ref _releaseCount);
            return Task.FromResult(InboxHandleResult.Retry);
        }

        Interlocked.Increment(ref _successCount);
        return Task.FromResult(InboxHandleResult.Success);
    }
}

/// <summary>
/// Handler that tracks which attempt number led to success.
/// </summary>
file class AttemptsTrackingHandler<TMessage> : IInboxHandler<TMessage> where TMessage : class
{
    private readonly int _succeedOnAttempt;
    private readonly ConcurrentDictionary<Guid, int> _attemptCounts = new();
    private readonly ConcurrentBag<int> _successfulAttemptNumbers = new();

    public AttemptsTrackingHandler(int succeedOnAttempt)
    {
        _succeedOnAttempt = succeedOnAttempt;
    }

    public ConcurrentBag<int> SuccessfulAttemptNumbers => _successfulAttemptNumbers;

    public Task<InboxHandleResult> HandleAsync(InboxMessageEnvelope<TMessage> message, CancellationToken token)
    {
        var attempts = _attemptCounts.AddOrUpdate(message.Id, 1, (_, count) => count + 1);

        if (attempts < _succeedOnAttempt)
        {
            return Task.FromResult(InboxHandleResult.Failed);
        }

        _successfulAttemptNumbers.Add(attempts);
        return Task.FromResult(InboxHandleResult.Success);
    }
}

/// <summary>
/// Batched handler that fails a specified number of times per message before succeeding.
/// </summary>
file class BatchedTransientFailingHandler<TMessage> : IBatchedInboxHandler<TMessage> where TMessage : class
{
    private readonly int _failuresBeforeSuccess;
    private readonly ConcurrentDictionary<Guid, int> _attemptCounts = new();
    private int _successCount;
    private int _failureCount;

    public BatchedTransientFailingHandler(int failuresBeforeSuccess)
    {
        _failuresBeforeSuccess = failuresBeforeSuccess;
    }

    public int SuccessCount => _successCount;
    public int FailureCount => _failureCount;

    public Task<IReadOnlyList<InboxMessageResult>> HandleAsync(
        IReadOnlyList<InboxMessageEnvelope<TMessage>> messages,
        CancellationToken token)
    {
        var results = new List<InboxMessageResult>();

        foreach (var message in messages)
        {
            var attempts = _attemptCounts.AddOrUpdate(message.Id, 1, (_, count) => count + 1);

            if (attempts <= _failuresBeforeSuccess)
            {
                Interlocked.Increment(ref _failureCount);
                results.Add(new InboxMessageResult(
                    message.Id,
                    InboxHandleResult.Failed,
                    $"Transient failure (attempt {attempts})"));
            }
            else
            {
                Interlocked.Increment(ref _successCount);
                results.Add(new InboxMessageResult(message.Id, InboxHandleResult.Success));
            }
        }

        return Task.FromResult<IReadOnlyList<InboxMessageResult>>(results);
    }
}
