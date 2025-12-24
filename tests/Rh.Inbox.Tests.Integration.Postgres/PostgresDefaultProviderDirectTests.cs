using FluentAssertions;
using Npgsql;
using NSubstitute;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Postgres.Options;
using Rh.Inbox.Postgres.Provider;
using Rh.Inbox.Providers;
using Xunit;

namespace Rh.Inbox.Tests.Integration.Postgres;

/// <summary>
/// Direct provider integration tests for PostgresDefaultInboxStorageProvider.
/// Tests create provider instances directly, bypassing DI, and verify behavior
/// by directly querying the database.
/// </summary>
[Collection("Postgres")]
public class PostgresDefaultProviderDirectTests : IAsyncLifetime
{
    private readonly PostgresContainerFixture _container;
    private NpgsqlDataSource _dataSource = null!;
    private PostgresInboxProviderOptions _providerOptions = null!;

    private const string InboxName = "default-direct-test";
    private const string TableName = "inbox_messages_default_direct";
    private const string DeadLetterTableName = "inbox_dlq_default_direct";
    private const string DeduplicationTableName = "inbox_dedup_default_direct";
    private const string GroupLocksTableName = "inbox_locks_default_direct";

    public PostgresDefaultProviderDirectTests(PostgresContainerFixture container)
    {
        _container = container;
    }

    public Task InitializeAsync()
    {
        _dataSource = NpgsqlDataSource.Create(_container.ConnectionString);
        _providerOptions = new PostgresInboxProviderOptions
        {
            DataSource = _dataSource,
            TableName = TableName,
            DeadLetterTableName = DeadLetterTableName,
            DeduplicationTableName = DeduplicationTableName,
            GroupLocksTableName = GroupLocksTableName
        };
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await CleanupTablesAsync();
        await _dataSource.DisposeAsync();
    }

    #region ExtendLocksAsync Tests

    [Fact]
    public async Task ExtendLocksAsync_WithCapturedMessages_UpdatesCapturedAt()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var messageId = Guid.NewGuid();
        var processorId = "processor-1";
        var originalCapturedAt = DateTime.UtcNow.AddMinutes(-5);
        var newCapturedAt = DateTime.UtcNow;

        await InsertTestMessageAsync(messageId, capturedAt: originalCapturedAt, capturedBy: processorId);

        var result = await provider.ExtendLocksAsync(
            processorId,
            [new MessageIdentifier(messageId, null)],
            newCapturedAt,
            CancellationToken.None);

        result.Should().Be(1);
        var message = await GetMessageAsync(messageId);
        message!.CapturedAt.Should().BeCloseTo(newCapturedAt, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task ExtendLocksAsync_WrongProcessor_DoesNotUpdate()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var messageId = Guid.NewGuid();
        var originalCapturedAt = DateTime.UtcNow.AddMinutes(-5);
        await InsertTestMessageAsync(messageId, capturedAt: originalCapturedAt, capturedBy: "other-processor");

        var result = await provider.ExtendLocksAsync(
            "processor-1",
            [new MessageIdentifier(messageId, null)],
            DateTime.UtcNow,
            CancellationToken.None);

        result.Should().Be(0);
        var message = await GetMessageAsync(messageId);
        message!.CapturedAt.Should().BeCloseTo(originalCapturedAt, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task ExtendLocksAsync_MultipleMessages_UpdatesAll()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var processorId = "processor-1";
        var originalCapturedAt = DateTime.UtcNow.AddMinutes(-5);
        var newCapturedAt = DateTime.UtcNow;
        var messageIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

        foreach (var id in messageIds)
        {
            await InsertTestMessageAsync(id, capturedAt: originalCapturedAt, capturedBy: processorId);
        }

        var result = await provider.ExtendLocksAsync(
            processorId,
            messageIds.Select(id => new MessageIdentifier(id, null)).ToArray(),
            newCapturedAt,
            CancellationToken.None);

        result.Should().Be(3);
        foreach (var id in messageIds)
        {
            var message = await GetMessageAsync(id);
            message!.CapturedAt.Should().BeCloseTo(newCapturedAt, TimeSpan.FromSeconds(1));
        }
    }

    [Fact]
    public async Task ExtendLocksAsync_EmptyList_ReturnsZero()
    {
        var provider = await CreateAndMigrateProviderAsync();

        var result = await provider.ExtendLocksAsync(
            "processor-1",
            [],
            DateTime.UtcNow,
            CancellationToken.None);

        result.Should().Be(0);
    }

    #endregion

    #region MoveToDeadLetterAsync Tests

    [Fact]
    public async Task MoveToDeadLetterAsync_WithDeadLetterEnabled_MovesMessageToDLQ()
    {
        var provider = await CreateAndMigrateProviderAsync(enableDeadLetter: true);
        var messageId = Guid.NewGuid();
        await InsertTestMessageAsync(messageId);

        await provider.MoveToDeadLetterAsync(messageId, "Max retries exceeded", CancellationToken.None);

        var message = await GetMessageAsync(messageId);
        message.Should().BeNull();

        var dlqCount = await CountDeadLettersAsync();
        dlqCount.Should().Be(1);
    }

    [Fact]
    public async Task MoveToDeadLetterAsync_WithDeadLetterDisabled_DeletesMessageOnly()
    {
        var provider = await CreateAndMigrateProviderAsync(enableDeadLetter: false);
        var messageId = Guid.NewGuid();
        await InsertTestMessageAsync(messageId);

        await provider.MoveToDeadLetterAsync(messageId, "Max retries exceeded", CancellationToken.None);

        var message = await GetMessageAsync(messageId);
        message.Should().BeNull();
        // Note: DLQ table doesn't exist when dead letter is disabled, so we don't verify count
    }

    #endregion

    #region MoveToDeadLetterBatchAsync Tests

    [Fact]
    public async Task MoveToDeadLetterBatchAsync_MultipleMessages_MovesAllToDLQ()
    {
        var provider = await CreateAndMigrateProviderAsync(enableDeadLetter: true);
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

        foreach (var id in ids)
        {
            await InsertTestMessageAsync(id);
        }

        await provider.MoveToDeadLetterBatchAsync(
            ids.Select(id => (id, "Failed")).ToList(),
            CancellationToken.None);

        foreach (var id in ids)
        {
            var message = await GetMessageAsync(id);
            message.Should().BeNull();
        }

        var dlqCount = await CountDeadLettersAsync();
        dlqCount.Should().Be(3);
    }

    [Fact]
    public async Task MoveToDeadLetterBatchAsync_WithDeadLetterDisabled_DeletesAllWithoutDLQ()
    {
        var provider = await CreateAndMigrateProviderAsync(enableDeadLetter: false);
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid() };

        foreach (var id in ids)
        {
            await InsertTestMessageAsync(id);
        }

        await provider.MoveToDeadLetterBatchAsync(
            ids.Select(id => (id, "Failed")).ToList(),
            CancellationToken.None);

        foreach (var id in ids)
        {
            var message = await GetMessageAsync(id);
            message.Should().BeNull();
        }
        // Note: DLQ table doesn't exist when dead letter is disabled, so we don't verify count
    }

    [Fact]
    public async Task MoveToDeadLetterBatchAsync_EmptyList_DoesNothing()
    {
        var provider = await CreateAndMigrateProviderAsync(enableDeadLetter: true);

        await provider.MoveToDeadLetterBatchAsync([], CancellationToken.None);

        var dlqCount = await CountDeadLettersAsync();
        dlqCount.Should().Be(0);
    }

    #endregion

    #region ReleaseBatchAsync Tests

    [Fact]
    public async Task ReleaseBatchAsync_CapturedMessages_ClearsCapture()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid() };

        foreach (var id in ids)
        {
            await InsertTestMessageAsync(id, capturedAt: DateTime.UtcNow, capturedBy: "proc-1");
        }

        await provider.ReleaseBatchAsync(ids, CancellationToken.None);

        foreach (var id in ids)
        {
            var message = await GetMessageAsync(id);
            message!.CapturedAt.Should().BeNull();
            message.CapturedBy.Should().BeNull();
        }
    }

    [Fact]
    public async Task ReleaseBatchAsync_EmptyList_DoesNothing()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var id = Guid.NewGuid();
        await InsertTestMessageAsync(id, capturedAt: DateTime.UtcNow, capturedBy: "proc-1");

        await provider.ReleaseBatchAsync([], CancellationToken.None);

        var message = await GetMessageAsync(id);
        message!.CapturedAt.Should().NotBeNull();
        message.CapturedBy.Should().Be("proc-1");
    }

    #endregion

    #region ReadDeadLettersAsync Tests

    [Fact]
    public async Task ReadDeadLettersAsync_WithMessages_ReturnsDeadLetters()
    {
        var provider = await CreateAndMigrateProviderAsync(enableDeadLetter: true);
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid() };
        foreach (var id in ids)
        {
            await InsertTestMessageAsync(id);
            await provider.MoveToDeadLetterAsync(id, "Test failure", CancellationToken.None);
        }

        var deadLetters = await provider.ReadDeadLettersAsync(10, CancellationToken.None);

        deadLetters.Should().HaveCount(2);
        deadLetters.Select(d => d.Id).Should().BeEquivalentTo(ids);
        deadLetters.All(d => d.FailureReason == "Test failure").Should().BeTrue();
    }

    [Fact]
    public async Task ReadDeadLettersAsync_WithDeadLetterDisabled_ReturnsEmpty()
    {
        var provider = await CreateAndMigrateProviderAsync(enableDeadLetter: false);

        var deadLetters = await provider.ReadDeadLettersAsync(10, CancellationToken.None);

        deadLetters.Should().BeEmpty();
    }

    [Fact]
    public async Task ReadDeadLettersAsync_LimitsCount()
    {
        var provider = await CreateAndMigrateProviderAsync(enableDeadLetter: true);
        for (int i = 0; i < 5; i++)
        {
            var id = Guid.NewGuid();
            await InsertTestMessageAsync(id);
            await provider.MoveToDeadLetterAsync(id, "Failed", CancellationToken.None);
        }

        var deadLetters = await provider.ReadDeadLettersAsync(3, CancellationToken.None);

        deadLetters.Should().HaveCount(3);
    }

    #endregion

    #region GetHealthMetricsAsync Tests

    [Fact]
    public async Task GetHealthMetricsAsync_WithMessages_ReturnsCorrectMetrics()
    {
        var provider = await CreateAndMigrateProviderAsync(enableDeadLetter: true);

        // Insert 3 pending messages
        for (int i = 0; i < 3; i++)
        {
            await InsertTestMessageAsync(Guid.NewGuid());
        }

        // Insert 2 captured (in-flight) messages
        for (int i = 0; i < 2; i++)
        {
            await InsertTestMessageAsync(Guid.NewGuid(), capturedAt: DateTime.UtcNow, capturedBy: "proc");
        }

        // Move 1 to dead letter
        var dlqId = Guid.NewGuid();
        await InsertTestMessageAsync(dlqId);
        await provider.MoveToDeadLetterAsync(dlqId, "Failed", CancellationToken.None);

        var metrics = await provider.GetHealthMetricsAsync(CancellationToken.None);

        metrics.PendingCount.Should().Be(3);
        metrics.CapturedCount.Should().Be(2);
        metrics.DeadLetterCount.Should().Be(1);
        metrics.OldestPendingMessageAt.Should().NotBeNull();
    }

    [Fact]
    public async Task GetHealthMetricsAsync_EmptyInbox_ReturnsZeros()
    {
        var provider = await CreateAndMigrateProviderAsync();

        var metrics = await provider.GetHealthMetricsAsync(CancellationToken.None);

        metrics.PendingCount.Should().Be(0);
        metrics.CapturedCount.Should().Be(0);
        metrics.DeadLetterCount.Should().Be(0);
        metrics.OldestPendingMessageAt.Should().BeNull();
    }

    [Fact]
    public async Task GetHealthMetricsAsync_WithDeadLetterDisabled_ReturnsZeroDeadLetterCount()
    {
        var provider = await CreateAndMigrateProviderAsync(enableDeadLetter: false);

        await InsertTestMessageAsync(Guid.NewGuid());

        var metrics = await provider.GetHealthMetricsAsync(CancellationToken.None);

        metrics.PendingCount.Should().Be(1);
        metrics.CapturedCount.Should().Be(0);
        metrics.DeadLetterCount.Should().Be(0);
    }

    #endregion

    #region WriteAsync Tests

    [Fact]
    public async Task WriteAsync_SingleMessage_InsertsToDatabase()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var message = CreateInboxMessage();

        await provider.WriteAsync(message, CancellationToken.None);

        var count = await CountMessagesAsync();
        count.Should().Be(1);

        var dbMessage = await GetMessageAsync(message.Id);
        dbMessage.Should().NotBeNull();
    }

    [Fact]
    public async Task WriteAsync_WithCollapseKey_ReplacesExistingMessage()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var collapseKey = "same-key";

        var message1 = CreateInboxMessage(collapseKey: collapseKey, payload: "first");
        var message2 = CreateInboxMessage(collapseKey: collapseKey, payload: "second");

        await provider.WriteAsync(message1, CancellationToken.None);
        await provider.WriteAsync(message2, CancellationToken.None);

        var count = await CountMessagesAsync();
        count.Should().Be(1, "collapsed message should replace the previous one");
    }

    [Fact]
    public async Task WriteAsync_WithDeduplication_SkipsDuplicate()
    {
        var provider = await CreateAndMigrateProviderAsync(enableDeduplication: true);
        var deduplicationId = "dedup-123";

        var message1 = CreateInboxMessage(deduplicationId: deduplicationId);
        var message2 = CreateInboxMessage(deduplicationId: deduplicationId);

        await provider.WriteAsync(message1, CancellationToken.None);
        await provider.WriteAsync(message2, CancellationToken.None);

        var count = await CountMessagesAsync();
        count.Should().Be(1, "duplicate message should be skipped");
    }

    #endregion

    #region WriteBatchAsync Tests

    [Fact]
    public async Task WriteBatchAsync_MultipleMessages_InsertsAll()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var messages = new[]
        {
            CreateInboxMessage(),
            CreateInboxMessage(),
            CreateInboxMessage()
        };

        await provider.WriteBatchAsync(messages, CancellationToken.None);

        var count = await CountMessagesAsync();
        count.Should().Be(3);
    }

    [Fact]
    public async Task WriteBatchAsync_EmptyBatch_DoesNothing()
    {
        var provider = await CreateAndMigrateProviderAsync();

        await provider.WriteBatchAsync([], CancellationToken.None);

        var count = await CountMessagesAsync();
        count.Should().Be(0);
    }

    [Fact]
    public async Task WriteBatchAsync_WithCollapseKeys_CollapsesMessages()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var collapseKey = "collapse-key";

        // First insert a message with collapse key
        await provider.WriteAsync(CreateInboxMessage(collapseKey: collapseKey), CancellationToken.None);

        // Then batch insert another with same collapse key
        var messages = new[]
        {
            CreateInboxMessage(collapseKey: collapseKey),
            CreateInboxMessage() // Different message without collapse key
        };

        await provider.WriteBatchAsync(messages, CancellationToken.None);

        var count = await CountMessagesAsync();
        count.Should().Be(2, "collapsed messages count as one");
    }

    [Fact]
    public async Task WriteBatchAsync_WithDeduplication_SkipsDuplicates()
    {
        var provider = await CreateAndMigrateProviderAsync(enableDeduplication: true);
        var deduplicationId = "dedup-batch";

        var messages = new[]
        {
            CreateInboxMessage(deduplicationId: deduplicationId),
            CreateInboxMessage(deduplicationId: deduplicationId), // Duplicate
            CreateInboxMessage() // No dedup id
        };

        await provider.WriteBatchAsync(messages, CancellationToken.None);

        var count = await CountMessagesAsync();
        count.Should().Be(2, "duplicate should be skipped");
    }

    #endregion

    #region FailAsync Tests

    [Fact]
    public async Task FailAsync_CapturedMessage_ReleasesAndIncrementsAttempts()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var messageId = Guid.NewGuid();
        await InsertTestMessageAsync(messageId, capturedAt: DateTime.UtcNow, capturedBy: "proc-1", attemptsCount: 0);

        await provider.FailAsync(messageId, CancellationToken.None);

        var message = await GetMessageAsync(messageId);
        message!.CapturedAt.Should().BeNull();
        message.CapturedBy.Should().BeNull();
        message.AttemptsCount.Should().Be(1);
    }

    [Fact]
    public async Task FailAsync_MultipleFailures_IncrementsAttemptsEachTime()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var messageId = Guid.NewGuid();
        await InsertTestMessageAsync(messageId, capturedAt: DateTime.UtcNow, capturedBy: "proc-1", attemptsCount: 2);

        await provider.FailAsync(messageId, CancellationToken.None);

        var message = await GetMessageAsync(messageId);
        message!.AttemptsCount.Should().Be(3);
    }

    #endregion

    #region FailBatchAsync Tests

    [Fact]
    public async Task FailBatchAsync_MultipleMessages_ReleasesAllAndIncrementsAttempts()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid() };

        foreach (var id in ids)
        {
            await InsertTestMessageAsync(id, capturedAt: DateTime.UtcNow, capturedBy: "proc-1", attemptsCount: 0);
        }

        await provider.FailBatchAsync(ids, CancellationToken.None);

        foreach (var id in ids)
        {
            var message = await GetMessageAsync(id);
            message!.CapturedAt.Should().BeNull();
            message.CapturedBy.Should().BeNull();
            message.AttemptsCount.Should().Be(1);
        }
    }

    [Fact]
    public async Task FailBatchAsync_EmptyList_DoesNothing()
    {
        var provider = await CreateAndMigrateProviderAsync();

        await provider.FailBatchAsync([], CancellationToken.None);

        // No exception should be thrown
    }

    #endregion

    #region ProcessResultsBatchAsync Tests

    [Fact]
    public async Task ProcessResultsBatchAsync_CompletesMessages_DeletesFromDatabase()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid() };

        foreach (var id in ids)
        {
            await InsertTestMessageAsync(id);
        }

        await provider.ProcessResultsBatchAsync(
            toComplete: ids,
            toFail: [],
            toRelease: [],
            toDeadLetter: [],
            CancellationToken.None);

        foreach (var id in ids)
        {
            var message = await GetMessageAsync(id);
            message.Should().BeNull("completed messages should be deleted");
        }
    }

    [Fact]
    public async Task ProcessResultsBatchAsync_FailsMessages_ReleasesAndIncrementsAttempts()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid() };

        foreach (var id in ids)
        {
            await InsertTestMessageAsync(id, capturedAt: DateTime.UtcNow, capturedBy: "proc", attemptsCount: 0);
        }

        await provider.ProcessResultsBatchAsync(
            toComplete: [],
            toFail: ids,
            toRelease: [],
            toDeadLetter: [],
            CancellationToken.None);

        foreach (var id in ids)
        {
            var message = await GetMessageAsync(id);
            message!.CapturedAt.Should().BeNull();
            message.AttemptsCount.Should().Be(1);
        }
    }

    [Fact]
    public async Task ProcessResultsBatchAsync_ReleasesMessages_ClearsCapture()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid() };

        foreach (var id in ids)
        {
            await InsertTestMessageAsync(id, capturedAt: DateTime.UtcNow, capturedBy: "proc");
        }

        await provider.ProcessResultsBatchAsync(
            toComplete: [],
            toFail: [],
            toRelease: ids,
            toDeadLetter: [],
            CancellationToken.None);

        foreach (var id in ids)
        {
            var message = await GetMessageAsync(id);
            message!.CapturedAt.Should().BeNull();
            message.CapturedBy.Should().BeNull();
        }
    }

    [Fact]
    public async Task ProcessResultsBatchAsync_MovesToDeadLetter_RemovesFromMainAndAddsToDLQ()
    {
        var provider = await CreateAndMigrateProviderAsync(enableDeadLetter: true);
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid() };

        foreach (var id in ids)
        {
            await InsertTestMessageAsync(id);
        }

        await provider.ProcessResultsBatchAsync(
            toComplete: [],
            toFail: [],
            toRelease: [],
            toDeadLetter: ids.Select(id => (id, "Failed")).ToList(),
            CancellationToken.None);

        foreach (var id in ids)
        {
            var message = await GetMessageAsync(id);
            message.Should().BeNull();
        }

        var dlqCount = await CountDeadLettersAsync();
        dlqCount.Should().Be(2);
    }

    [Fact]
    public async Task ProcessResultsBatchAsync_MixedOperations_ProcessesAllCorrectly()
    {
        var provider = await CreateAndMigrateProviderAsync(enableDeadLetter: true);

        var completeId = Guid.NewGuid();
        var failId = Guid.NewGuid();
        var releaseId = Guid.NewGuid();
        var deadLetterId = Guid.NewGuid();

        await InsertTestMessageAsync(completeId);
        await InsertTestMessageAsync(failId, capturedAt: DateTime.UtcNow, capturedBy: "proc", attemptsCount: 0);
        await InsertTestMessageAsync(releaseId, capturedAt: DateTime.UtcNow, capturedBy: "proc");
        await InsertTestMessageAsync(deadLetterId);

        await provider.ProcessResultsBatchAsync(
            toComplete: [completeId],
            toFail: [failId],
            toRelease: [releaseId],
            toDeadLetter: [(deadLetterId, "Max retries")],
            CancellationToken.None);

        (await GetMessageAsync(completeId)).Should().BeNull();
        (await GetMessageAsync(failId))!.AttemptsCount.Should().Be(1);
        (await GetMessageAsync(releaseId))!.CapturedAt.Should().BeNull();
        (await GetMessageAsync(deadLetterId)).Should().BeNull();

        var dlqCount = await CountDeadLettersAsync();
        dlqCount.Should().Be(1);
    }

    [Fact]
    public async Task ProcessResultsBatchAsync_EmptyLists_DoesNothing()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var id = Guid.NewGuid();
        await InsertTestMessageAsync(id);

        await provider.ProcessResultsBatchAsync([], [], [], [], CancellationToken.None);

        var message = await GetMessageAsync(id);
        message.Should().NotBeNull();
    }

    #endregion

    #region ReadAndCaptureAsync Tests

    [Fact]
    public async Task ReadAndCaptureAsync_WithPendingMessages_CapturesMessages()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

        foreach (var id in ids)
        {
            await InsertTestMessageAsync(id);
        }

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        messages.Should().HaveCount(3);

        foreach (var id in ids)
        {
            var dbMessage = await GetMessageAsync(id);
            dbMessage!.CapturedAt.Should().NotBeNull();
            dbMessage.CapturedBy.Should().Be("processor-1");
        }
    }

    [Fact]
    public async Task ReadAndCaptureAsync_NoMessages_ReturnsEmpty()
    {
        var provider = await CreateAndMigrateProviderAsync();

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        messages.Should().BeEmpty();
    }

    [Fact]
    public async Task ReadAndCaptureAsync_AlreadyCaptured_SkipsMessages()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var capturedId = Guid.NewGuid();
        var pendingId = Guid.NewGuid();

        await InsertTestMessageAsync(capturedId, capturedAt: DateTime.UtcNow, capturedBy: "other-proc");
        await InsertTestMessageAsync(pendingId);

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        messages.Should().HaveCount(1);
        messages[0].Id.Should().Be(pendingId);
    }

    [Fact]
    public async Task ReadAndCaptureAsync_ExpiredCapture_RecapturesMessage()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var messageId = Guid.NewGuid();
        // Captured 10 minutes ago - should be expired with default 5 min MaxProcessingTime
        var expiredCaptureTime = DateTime.UtcNow.AddMinutes(-10);
        await InsertTestMessageAsync(messageId, capturedAt: expiredCaptureTime, capturedBy: "old-proc");

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        messages.Should().HaveCount(1);
        messages[0].Id.Should().Be(messageId);

        var dbMessage = await GetMessageAsync(messageId);
        dbMessage!.CapturedBy.Should().Be("processor-1");
    }

    [Fact]
    public async Task ReadAndCaptureAsync_RespectsBatchSize()
    {
        var provider = await CreateAndMigrateProviderAsync(batchSize: 2);

        for (int i = 0; i < 5; i++)
        {
            await InsertTestMessageAsync(Guid.NewGuid());
        }

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        messages.Should().HaveCount(2);
    }

    [Fact]
    public async Task ReadAndCaptureAsync_ReturnsMessagesInOrder()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();
        var id3 = Guid.NewGuid();

        // Insert with explicit timestamps to ensure order
        await InsertTestMessageAsync(id1, receivedAt: DateTime.UtcNow.AddSeconds(-3));
        await InsertTestMessageAsync(id2, receivedAt: DateTime.UtcNow.AddSeconds(-2));
        await InsertTestMessageAsync(id3, receivedAt: DateTime.UtcNow.AddSeconds(-1));

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        messages.Should().HaveCount(3);
        messages[0].Id.Should().Be(id1);
        messages[1].Id.Should().Be(id2);
        messages[2].Id.Should().Be(id3);
    }

    #endregion

    #region Helper Methods

    private IProviderOptionsAccessor CreateMockOptionsAccessor()
    {
        var mock = Substitute.For<IProviderOptionsAccessor>();
        mock.GetForInbox(InboxName).Returns(_providerOptions);
        return mock;
    }

    private IInboxConfiguration CreateConfiguration(
        bool enableDeadLetter = true,
        bool enableDeduplication = false,
        int batchSize = 100)
    {
        var options = Substitute.For<IInboxOptions>();
        options.InboxName.Returns(InboxName);
        options.ReadBatchSize.Returns(batchSize);
        options.MaxProcessingTime.Returns(TimeSpan.FromMinutes(5));
        options.EnableDeadLetter.Returns(enableDeadLetter);
        options.EnableDeduplication.Returns(enableDeduplication);
        options.DeduplicationInterval.Returns(enableDeduplication ? TimeSpan.FromHours(1) : TimeSpan.Zero);

        var config = Substitute.For<IInboxConfiguration>();
        config.InboxName.Returns(InboxName);
        config.InboxType.Returns(InboxType.Default);
        config.Options.Returns(options);
        config.DateTimeProvider.Returns(new DateTimeProvider());

        return config;
    }

    private async Task<PostgresDefaultInboxStorageProvider> CreateAndMigrateProviderAsync(
        bool enableDeadLetter = true,
        bool enableDeduplication = false,
        int batchSize = 100)
    {
        await CleanupTablesAsync();
        var config = CreateConfiguration(enableDeadLetter, enableDeduplication, batchSize);
        var optionsAccessor = CreateMockOptionsAccessor();
        var provider = new PostgresDefaultInboxStorageProvider(config, optionsAccessor);
        await provider.MigrateAsync(CancellationToken.None);
        return provider;
    }

    private static InboxMessage CreateInboxMessage(
        string? collapseKey = null,
        string? deduplicationId = null,
        string? groupId = null,
        string payload = "{}")
    {
        return new InboxMessage
        {
            Id = Guid.NewGuid(),
            MessageType = "TestMessage",
            Payload = payload,
            GroupId = groupId,
            CollapseKey = collapseKey,
            DeduplicationId = deduplicationId,
            AttemptsCount = 0,
            ReceivedAt = DateTime.UtcNow
        };
    }

    private async Task InsertTestMessageAsync(
        Guid id,
        string messageType = "TestMessage",
        string? groupId = null,
        DateTime? capturedAt = null,
        string? capturedBy = null,
        int attemptsCount = 0,
        DateTime? receivedAt = null)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand($"""
            INSERT INTO "{TableName}"
            (id, inbox_name, message_type, payload, group_id, attempts_count, received_at, captured_at, captured_by)
            VALUES (@id, @inboxName, @messageType, @payload, @groupId, @attemptsCount, @receivedAt, @capturedAt, @capturedBy)
            """, conn);

        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("inboxName", InboxName);
        cmd.Parameters.AddWithValue("messageType", messageType);
        cmd.Parameters.AddWithValue("payload", "{}");
        cmd.Parameters.AddWithValue("groupId", (object?)groupId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("attemptsCount", attemptsCount);
        cmd.Parameters.AddWithValue("receivedAt", receivedAt ?? DateTime.UtcNow);
        cmd.Parameters.AddWithValue("capturedAt", (object?)capturedAt ?? DBNull.Value);
        cmd.Parameters.AddWithValue("capturedBy", (object?)capturedBy ?? DBNull.Value);

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<int> CountMessagesAsync()
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand($"""
            SELECT COUNT(*) FROM "{TableName}" WHERE inbox_name = @inboxName
            """, conn);
        cmd.Parameters.AddWithValue("inboxName", InboxName);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }

    private async Task<InboxMessageRecord?> GetMessageAsync(Guid id)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand($"""
            SELECT id, captured_at, captured_by, attempts_count
            FROM "{TableName}" WHERE id = @id
            """, conn);
        cmd.Parameters.AddWithValue("id", id);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            return new InboxMessageRecord(
                reader.GetGuid(0),
                reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetInt32(3));
        }
        return null;
    }

    private async Task<int> CountDeadLettersAsync()
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand($"""
            SELECT COUNT(*) FROM "{DeadLetterTableName}" WHERE inbox_name = @inboxName
            """, conn);
        cmd.Parameters.AddWithValue("inboxName", InboxName);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }

    private async Task CleanupTablesAsync()
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand($"""
            DROP TABLE IF EXISTS "{TableName}" CASCADE;
            DROP TABLE IF EXISTS "{DeadLetterTableName}" CASCADE;
            DROP TABLE IF EXISTS "{DeduplicationTableName}" CASCADE;
            DROP TABLE IF EXISTS "{GroupLocksTableName}" CASCADE;
            """, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private record InboxMessageRecord(Guid Id, DateTime? CapturedAt, string? CapturedBy, int AttemptsCount);
    private record MessageIdentifier(Guid Id, string? GroupId) : IInboxMessageIdentifiers;

    #endregion
}
