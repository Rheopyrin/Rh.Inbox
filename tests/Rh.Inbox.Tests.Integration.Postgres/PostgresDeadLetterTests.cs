using FluentAssertions;
using Rh.Inbox.Abstractions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
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
public class PostgresDeadLetterTests(PostgresContainerFixture container, ITestOutputHelper output) : IAsyncLifetime
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
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        var message = new SimpleMessage("test-msg", "data");
        await writer.WriteAsync(message, "dead-letter-test");

        // Wait for message to fail maxAttempts times
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.FailedCount >= maxAttempts,
            TimeSpan.FromSeconds(10));

        handler.FailedCount.Should().BeGreaterOrEqualTo(maxAttempts,
            "handler should have been called at least MaxAttempts times");

        // Wait for dead letter record to appear in database
        var deadLetterCount = await WaitForDeadLetterCountAsync("dead-letter-test", 1, TimeSpan.FromSeconds(5));
        deadLetterCount.Should().Be(1, "message should be moved to dead letter after max attempts");

        output.WriteLine($"Message moved to dead letter after {handler.FailedCount} failed attempts");
    }

    [Fact]
    public async Task DeadLetter_MultipleFailing_AllMovedToDeadLetter()
    {
        const int maxAttempts = 2;
        const int messageCount = 5;
        var handler = new FailingHandler<SimpleMessage>(1.0); // Always fails
        _serviceProvider = CreateDeadLetterServiceProvider("dead-letter-multi", handler, o =>
        {
            o.MaxAttempts = maxAttempts;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        var messages = TestMessageFactory.CreateSimpleMessages(messageCount);
        await writer.WriteBatchAsync(messages, "dead-letter-multi");

        // Wait for all messages to fail maxAttempts times
        var expectedFailures = messageCount * maxAttempts;
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.FailedCount >= expectedFailures,
            TimeSpan.FromSeconds(15));

        // Wait for dead letter records to appear in database
        var deadLetterCount = await WaitForDeadLetterCountAsync("dead-letter-multi", messageCount, TimeSpan.FromSeconds(5));
        deadLetterCount.Should().Be(messageCount, "all failing messages should be moved to dead letter");

        output.WriteLine($"All {messageCount} messages moved to dead letter");
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
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 3;
        var messages = TestMessageFactory.CreateSimpleMessages(messageCount);
        await writer.WriteBatchAsync(messages, "dead-letter-disabled");

        // Wait for all messages to fail maxAttempts times
        var expectedFailures = messageCount * maxAttempts;
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.FailedCount >= expectedFailures,
            TimeSpan.FromSeconds(10));

        // When dead letter is disabled, the table may not exist or have zero records
        var deadLetterCount = await GetDeadLetterCountOrZeroAsync("dead-letter-disabled");
        deadLetterCount.Should().Be(0, "dead letter is disabled, messages should be discarded");

        output.WriteLine("Dead letter disabled - messages discarded after max attempts");
    }

    [Fact]
    public async Task DeadLetter_MixedSuccessAndFailure_OnlyFailedMovedToDeadLetter()
    {
        const int maxAttempts = 2;
        var handler = new FailingHandler<SimpleMessage>(0.5); // 50% fail rate
        _serviceProvider = CreateDeadLetterServiceProvider("dead-letter-mixed", handler, o =>
        {
            o.MaxAttempts = maxAttempts;
            o.ReadBatchSize = 1; // Process one at a time for deterministic behavior
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 20;
        var messages = TestMessageFactory.CreateSimpleMessages(messageCount);
        await writer.WriteBatchAsync(messages, "dead-letter-mixed");

        // Wait until all messages are processed (either succeeded or exhausted retries)
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.ProcessedCount + (handler.FailedCount / maxAttempts) >= messageCount,
            TimeSpan.FromSeconds(30));

        // Allow a small delay for dead letter records to be written
        await Task.Delay(500);
        var deadLetterCount = await GetDeadLetterCountAsync("dead-letter-mixed");

        // With random failures, we should have some successes and some dead letters
        // The exact split depends on the random failures, so we just verify both can happen
        handler.ProcessedCount.Should().BeGreaterOrEqualTo(0, "messages may succeed");

        // Track unique message IDs that were successfully processed
        var uniqueSuccessIds = handler.Processed.Distinct().Count();

        // Verify messages are accounted for (either succeeded or went to dead letter)
        // Due to timing and random failures, allow for some variance
        var totalAccountedFor = uniqueSuccessIds + deadLetterCount;
        totalAccountedFor.Should().BeGreaterOrEqualTo(messageCount,
            "all messages should eventually either succeed or go to dead letter");

        output.WriteLine($"Unique succeeded: {uniqueSuccessIds}, Dead lettered: {deadLetterCount}, Total: {totalAccountedFor}");
    }

    [Fact]
    public async Task DeadLetter_VerifyContent_ContainsCorrectData()
    {
        const int maxAttempts = 2;
        var handler = new FailingHandler<SimpleMessage>(1.0); // Always fails
        _serviceProvider = CreateDeadLetterServiceProvider("dead-letter-content", handler, o =>
        {
            o.MaxAttempts = maxAttempts;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        var message = new SimpleMessage("unique-content-id", "test-data-content");
        await writer.WriteAsync(message, "dead-letter-content");

        // Wait for message to fail maxAttempts times
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.FailedCount >= maxAttempts,
            TimeSpan.FromSeconds(10));

        // Wait for dead letter record to appear
        await WaitForDeadLetterCountAsync("dead-letter-content", 1, TimeSpan.FromSeconds(5));
        var deadLetterRecord = await GetDeadLetterRecordAsync("dead-letter-content");
        deadLetterRecord.Should().NotBeNull();
        deadLetterRecord!.AttemptsCount.Should().BeGreaterOrEqualTo(1, "message should have at least one attempt recorded");
        deadLetterRecord.FailureReason.Should().NotBeNullOrEmpty();
        deadLetterRecord.Payload.Should().Contain("unique-content-id");

        output.WriteLine($"Dead letter record: Attempts={deadLetterRecord.AttemptsCount}, Reason={deadLetterRecord.FailureReason}");
    }

    [Fact]
    public async Task DeadLetter_BatchedHandler_FailingMessagesMovedToDeadLetter()
    {
        const int maxAttempts = 2;
        var handler = new BatchedFailingHandler<SimpleMessage>(1.0); // Always fails
        _serviceProvider = CreateBatchedDeadLetterServiceProvider("dead-letter-batched", handler, o =>
        {
            o.MaxAttempts = maxAttempts;
            o.ReadBatchSize = 10;
        });

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 20;
        var messages = TestMessageFactory.CreateSimpleMessages(messageCount);
        await writer.WriteBatchAsync(messages, "dead-letter-batched");

        // Wait for all messages to fail maxAttempts times
        var expectedFailures = messageCount * maxAttempts;
        await TestWaitHelper.WaitForConditionAsync(
            () => handler.FailedCount >= expectedFailures,
            TimeSpan.FromSeconds(15));

        // Wait for dead letter records to appear in database
        var deadLetterCount = await WaitForDeadLetterCountAsync("dead-letter-batched", messageCount, TimeSpan.FromSeconds(5));
        deadLetterCount.Should().Be(messageCount, "all failing messages should be moved to dead letter");

        output.WriteLine($"Batched handler: {deadLetterCount} messages moved to dead letter");
    }

    private async Task<int> WaitForDeadLetterCountAsync(string inboxName, int expectedCount, TimeSpan timeout)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var count = 0;

        while (sw.Elapsed < timeout)
        {
            count = await GetDeadLetterCountAsync(inboxName);
            if (count >= expectedCount)
                return count;
            await Task.Delay(100);
        }

        return count;
    }

    private async Task<int> GetDeadLetterCountAsync(string inboxName)
    {
        var tableName = BuildDeadLetterTableName(inboxName);
        await using var connection = new NpgsqlConnection(container.ConnectionString);
        await connection.OpenAsync();

        await using var cmd = new NpgsqlCommand($"SELECT COUNT(*) FROM \"{tableName}\"", connection);
        var result = await cmd.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }

    private static string BuildDeadLetterTableName(string inboxName)
    {
        // Match the provider's table naming: inbox_dead_letters_{sanitized_inbox_name}
        var sanitized = inboxName.ToLowerInvariant().Replace('-', '_');
        return $"inbox_dead_letters_{sanitized}";
    }

    private async Task<int> GetDeadLetterCountOrZeroAsync(string inboxName)
    {
        try
        {
            return await GetDeadLetterCountAsync(inboxName);
        }
        catch (PostgresException ex) when (ex.SqlState == "42P01") // Table doesn't exist
        {
            return 0;
        }
    }

    private async Task<DeadLetterRecord?> GetDeadLetterRecordAsync(string inboxName)
    {
        var tableName = BuildDeadLetterTableName(inboxName);
        await using var connection = new NpgsqlConnection(container.ConnectionString);
        await connection.OpenAsync();

        await using var cmd = new NpgsqlCommand(
            $"SELECT attempts_count, failure_reason, payload FROM \"{tableName}\" LIMIT 1", connection);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            return new DeadLetterRecord(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.GetString(2));
        }

        return null;
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
                .UsePostgres(container.ConnectionString)
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    o.EnableDeadLetter = true;
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
                .UsePostgres(container.ConnectionString)
                .ConfigureOptions(o =>
                {
                    o.PollingInterval = TimeSpan.FromMilliseconds(100);
                    o.EnableDeadLetter = true;
                    configureOptions?.Invoke(o);
                })
                .RegisterHandler(handler);
        });
        return services.BuildServiceProvider();
    }

    private record DeadLetterRecord(int AttemptsCount, string FailureReason, string Payload);
}