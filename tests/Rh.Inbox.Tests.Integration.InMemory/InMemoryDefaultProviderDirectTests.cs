using FluentAssertions;
using NSubstitute;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.InMemory;
using Rh.Inbox.InMemory.Options;
using Rh.Inbox.Providers;
using Xunit;

namespace Rh.Inbox.Tests.Integration.InMemory;

/// <summary>
/// Direct provider integration tests for InMemoryInboxStorageProvider (Default mode).
/// Tests create provider instances directly with mocked dependencies and verify behavior
/// through the provider's API and ReadAndCaptureAsync.
/// </summary>
public class InMemoryDefaultProviderDirectTests
{
    private const string InboxName = "default-direct-test";

    #region ExtendLocksAsync Tests

    [Fact]
    public async Task ExtendLocksAsync_WithCapturedMessages_UpdatesCapturedAt()
    {
        var provider = await CreateProviderAsync();
        var messageId = Guid.NewGuid();
        var processorId = "processor-1";
        var originalCapturedAt = DateTime.UtcNow.AddMinutes(-5);
        var newCapturedAt = DateTime.UtcNow;

        var message = CreateInboxMessage(id: messageId);
        await provider.WriteAsync(message, CancellationToken.None);

        // Capture the message
        message.CapturedAt = originalCapturedAt;
        message.CapturedBy = processorId;
        var captured = await provider.ReadAndCaptureAsync(processorId, CancellationToken.None);
        captured.Should().HaveCount(1);

        var result = await provider.ExtendLocksAsync(
            processorId,
            [new MessageIdentifier(messageId, null)],
            newCapturedAt,
            CancellationToken.None);

        result.Should().Be(1);

        // Verify by reading again - should not capture since it's still locked
        var recaptured = await provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        recaptured.Should().BeEmpty("message should still be locked");
    }

    [Fact]
    public async Task ExtendLocksAsync_WrongProcessor_DoesNotUpdate()
    {
        var provider = await CreateProviderAsync();
        var messageId = Guid.NewGuid();
        var originalCapturedAt = DateTime.UtcNow.AddMinutes(-5);

        var message = CreateInboxMessage(id: messageId);
        await provider.WriteAsync(message, CancellationToken.None);

        // Capture with processor-1
        await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        var result = await provider.ExtendLocksAsync(
            "processor-2",
            [new MessageIdentifier(messageId, null)],
            DateTime.UtcNow,
            CancellationToken.None);

        result.Should().Be(0, "wrong processor should not be able to extend lock");
    }

    [Fact]
    public async Task ExtendLocksAsync_MultipleMessages_UpdatesAll()
    {
        var provider = await CreateProviderAsync();
        var processorId = "processor-1";
        var newCapturedAt = DateTime.UtcNow;
        var messageIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

        foreach (var id in messageIds)
        {
            var message = CreateInboxMessage(id: id);
            await provider.WriteAsync(message, CancellationToken.None);
        }

        // Capture all messages
        await provider.ReadAndCaptureAsync(processorId, CancellationToken.None);

        var result = await provider.ExtendLocksAsync(
            processorId,
            messageIds.Select(id => new MessageIdentifier(id, null)).ToArray(),
            newCapturedAt,
            CancellationToken.None);

        result.Should().Be(3);
    }

    [Fact]
    public async Task ExtendLocksAsync_EmptyList_ReturnsZero()
    {
        var provider = await CreateProviderAsync();

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
        var provider = await CreateProviderAsync(enableDeadLetter: true);
        var messageId = Guid.NewGuid();
        var message = CreateInboxMessage(id: messageId);
        await provider.WriteAsync(message, CancellationToken.None);

        await provider.MoveToDeadLetterAsync(messageId, "Max retries exceeded", CancellationToken.None);

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        messages.Should().BeEmpty("message should be removed from inbox");

        var dlq = await provider.ReadDeadLettersAsync(10, CancellationToken.None);
        dlq.Should().HaveCount(1);
        dlq[0].Id.Should().Be(messageId);
        dlq[0].FailureReason.Should().Be("Max retries exceeded");
    }

    [Fact]
    public async Task MoveToDeadLetterAsync_WithDeadLetterDisabled_DeletesMessageOnly()
    {
        var provider = await CreateProviderAsync(enableDeadLetter: false);
        var messageId = Guid.NewGuid();
        var message = CreateInboxMessage(id: messageId);
        await provider.WriteAsync(message, CancellationToken.None);

        await provider.MoveToDeadLetterAsync(messageId, "Max retries exceeded", CancellationToken.None);

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        messages.Should().BeEmpty("message should be removed from inbox");

        var dlq = await provider.ReadDeadLettersAsync(10, CancellationToken.None);
        dlq.Should().BeEmpty("dead letter is disabled");
    }

    #endregion

    #region MoveToDeadLetterBatchAsync Tests

    [Fact]
    public async Task MoveToDeadLetterBatchAsync_MultipleMessages_MovesAllToDLQ()
    {
        var provider = await CreateProviderAsync(enableDeadLetter: true);
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
        foreach (var id in ids)
        {
            var message = CreateInboxMessage(id: id);
            await provider.WriteAsync(message, CancellationToken.None);
        }

        await provider.MoveToDeadLetterBatchAsync(
            ids.Select(id => (id, "Failed")).ToList(),
            CancellationToken.None);

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        messages.Should().BeEmpty("all messages should be removed");

        var dlq = await provider.ReadDeadLettersAsync(10, CancellationToken.None);
        dlq.Should().HaveCount(3);
    }

    [Fact]
    public async Task MoveToDeadLetterBatchAsync_WithDeadLetterDisabled_DeletesAllWithoutDLQ()
    {
        var provider = await CreateProviderAsync(enableDeadLetter: false);
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid() };
        foreach (var id in ids)
        {
            var message = CreateInboxMessage(id: id);
            await provider.WriteAsync(message, CancellationToken.None);
        }

        await provider.MoveToDeadLetterBatchAsync(
            ids.Select(id => (id, "Failed")).ToList(),
            CancellationToken.None);

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        messages.Should().BeEmpty("all messages should be removed");

        var dlq = await provider.ReadDeadLettersAsync(10, CancellationToken.None);
        dlq.Should().BeEmpty("dead letter is disabled");
    }

    [Fact]
    public async Task MoveToDeadLetterBatchAsync_EmptyList_DoesNothing()
    {
        var provider = await CreateProviderAsync(enableDeadLetter: true);

        await provider.MoveToDeadLetterBatchAsync([], CancellationToken.None);

        var dlq = await provider.ReadDeadLettersAsync(10, CancellationToken.None);
        dlq.Should().BeEmpty();
    }

    #endregion

    #region ReleaseBatchAsync Tests

    [Fact]
    public async Task ReleaseBatchAsync_CapturedMessages_ClearsCapture()
    {
        var provider = await CreateProviderAsync();
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid() };
        foreach (var id in ids)
        {
            var message = CreateInboxMessage(id: id);
            await provider.WriteAsync(message, CancellationToken.None);
        }

        // Capture messages
        var captured = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        captured.Should().HaveCount(2);

        await provider.ReleaseBatchAsync(ids, CancellationToken.None);

        // Should be able to capture again
        var recaptured = await provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        recaptured.Should().HaveCount(2, "messages should be released");
    }

    [Fact]
    public async Task ReleaseBatchAsync_EmptyList_DoesNothing()
    {
        var provider = await CreateProviderAsync();
        var id = Guid.NewGuid();
        var message = CreateInboxMessage(id: id);
        await provider.WriteAsync(message, CancellationToken.None);

        // Capture message
        await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        await provider.ReleaseBatchAsync([], CancellationToken.None);

        // Should still be captured
        var recaptured = await provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        recaptured.Should().BeEmpty("message should still be captured");
    }

    #endregion

    #region ReadDeadLettersAsync Tests

    [Fact]
    public async Task ReadDeadLettersAsync_WithMessages_ReturnsDeadLetters()
    {
        var provider = await CreateProviderAsync(enableDeadLetter: true);
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid() };
        foreach (var id in ids)
        {
            var message = CreateInboxMessage(id: id);
            await provider.WriteAsync(message, CancellationToken.None);
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
        var provider = await CreateProviderAsync(enableDeadLetter: false);

        var deadLetters = await provider.ReadDeadLettersAsync(10, CancellationToken.None);

        deadLetters.Should().BeEmpty();
    }

    [Fact]
    public async Task ReadDeadLettersAsync_LimitsCount()
    {
        var provider = await CreateProviderAsync(enableDeadLetter: true);
        for (int i = 0; i < 5; i++)
        {
            var id = Guid.NewGuid();
            var message = CreateInboxMessage(id: id);
            await provider.WriteAsync(message, CancellationToken.None);
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
        var provider = await CreateProviderAsync(enableDeadLetter: true);

        // Insert and capture 2 messages (in-flight)
        var capturedIds = new[] { Guid.NewGuid(), Guid.NewGuid() };
        foreach (var id in capturedIds)
        {
            var message = CreateInboxMessage(id: id);
            await provider.WriteAsync(message, CancellationToken.None);
        }
        await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        // Insert 3 pending messages AFTER capturing
        for (int i = 0; i < 3; i++)
        {
            var message = CreateInboxMessage();
            await provider.WriteAsync(message, CancellationToken.None);
        }

        // Move 1 message to dead letter
        var dlqId = Guid.NewGuid();
        var dlqMessage = CreateInboxMessage(id: dlqId);
        await provider.WriteAsync(dlqMessage, CancellationToken.None);
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
        var provider = await CreateProviderAsync();

        var metrics = await provider.GetHealthMetricsAsync(CancellationToken.None);

        metrics.PendingCount.Should().Be(0);
        metrics.CapturedCount.Should().Be(0);
        metrics.DeadLetterCount.Should().Be(0);
        metrics.OldestPendingMessageAt.Should().BeNull();
    }

    [Fact]
    public async Task GetHealthMetricsAsync_WithDeadLetterDisabled_ReturnsZeroDeadLetterCount()
    {
        var provider = await CreateProviderAsync(enableDeadLetter: false);

        var message = CreateInboxMessage();
        await provider.WriteAsync(message, CancellationToken.None);

        var metrics = await provider.GetHealthMetricsAsync(CancellationToken.None);

        metrics.PendingCount.Should().Be(1);
        metrics.CapturedCount.Should().Be(0);
        metrics.DeadLetterCount.Should().Be(0);
    }

    #endregion

    #region WriteAsync Tests

    [Fact]
    public async Task WriteAsync_SingleMessage_InsertsToInbox()
    {
        var provider = await CreateProviderAsync();
        var message = CreateInboxMessage();

        await provider.WriteAsync(message, CancellationToken.None);

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        messages.Should().HaveCount(1);
        messages[0].Id.Should().Be(message.Id);
    }

    [Fact]
    public async Task WriteAsync_WithCollapseKey_ReplacesExistingMessage()
    {
        var provider = await CreateProviderAsync();
        var collapseKey = "same-key";

        var message1 = CreateInboxMessage(collapseKey: collapseKey, payload: "first");
        var message2 = CreateInboxMessage(collapseKey: collapseKey, payload: "second");

        await provider.WriteAsync(message1, CancellationToken.None);
        await provider.WriteAsync(message2, CancellationToken.None);

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        messages.Should().HaveCount(1, "collapsed message should replace the previous one");
        messages[0].Payload.Should().Be("second");
    }

    [Fact]
    public async Task WriteAsync_WithDeduplication_SkipsDuplicate()
    {
        var provider = await CreateProviderAsync(enableDeduplication: true);
        var deduplicationId = "dedup-123";

        var message1 = CreateInboxMessage(deduplicationId: deduplicationId);
        var message2 = CreateInboxMessage(deduplicationId: deduplicationId);

        await provider.WriteAsync(message1, CancellationToken.None);
        await provider.WriteAsync(message2, CancellationToken.None);

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        messages.Should().HaveCount(1, "duplicate message should be skipped");
        messages[0].Id.Should().Be(message1.Id);
    }

    #endregion

    #region WriteBatchAsync Tests

    [Fact]
    public async Task WriteBatchAsync_MultipleMessages_InsertsAll()
    {
        var provider = await CreateProviderAsync();
        var messages = new[]
        {
            CreateInboxMessage(),
            CreateInboxMessage(),
            CreateInboxMessage()
        };

        await provider.WriteBatchAsync(messages, CancellationToken.None);

        var result = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        result.Should().HaveCount(3);
    }

    [Fact]
    public async Task WriteBatchAsync_EmptyBatch_DoesNothing()
    {
        var provider = await CreateProviderAsync();

        await provider.WriteBatchAsync([], CancellationToken.None);

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        messages.Should().BeEmpty();
    }

    [Fact]
    public async Task WriteBatchAsync_WithCollapseKeys_CollapsesMessages()
    {
        var provider = await CreateProviderAsync();
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

        var result = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        result.Should().HaveCount(2, "collapsed messages count as one");
    }

    [Fact]
    public async Task WriteBatchAsync_WithDeduplication_SkipsDuplicates()
    {
        var provider = await CreateProviderAsync(enableDeduplication: true);
        var deduplicationId = "dedup-batch";

        // First, write a message with deduplication ID to establish it in the store
        var firstMessage = CreateInboxMessage(deduplicationId: deduplicationId);
        await provider.WriteAsync(firstMessage, CancellationToken.None);

        // Now batch write with the same deduplication ID (should be skipped) and a new message
        var messages = new[]
        {
            CreateInboxMessage(deduplicationId: deduplicationId), // Duplicate - should be skipped
            CreateInboxMessage() // No dedup id - should be inserted
        };

        await provider.WriteBatchAsync(messages, CancellationToken.None);

        var result = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        result.Should().HaveCount(2, "one original + one new (duplicate should be skipped)");
    }

    #endregion

    #region FailAsync Tests

    [Fact]
    public async Task FailAsync_CapturedMessage_ReleasesAndIncrementsAttempts()
    {
        var provider = await CreateProviderAsync();
        var messageId = Guid.NewGuid();
        var message = CreateInboxMessage(id: messageId);
        await provider.WriteAsync(message, CancellationToken.None);

        // Capture the message
        var captured = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        captured.Should().HaveCount(1);
        captured[0].AttemptsCount.Should().Be(0);

        await provider.FailAsync(messageId, CancellationToken.None);

        // Should be able to capture again (released)
        var recaptured = await provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        recaptured.Should().HaveCount(1);
        recaptured[0].AttemptsCount.Should().Be(1);
    }

    [Fact]
    public async Task FailAsync_MultipleFailures_IncrementsAttemptsEachTime()
    {
        var provider = await CreateProviderAsync();
        var messageId = Guid.NewGuid();
        var message = CreateInboxMessage(id: messageId);
        await provider.WriteAsync(message, CancellationToken.None);

        // First failure
        await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        await provider.FailAsync(messageId, CancellationToken.None);

        // Second failure
        var captured = await provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        captured[0].AttemptsCount.Should().Be(1);
        await provider.FailAsync(messageId, CancellationToken.None);

        // Third capture
        var recaptured = await provider.ReadAndCaptureAsync("processor-3", CancellationToken.None);
        recaptured.Should().HaveCount(1);
        recaptured[0].AttemptsCount.Should().Be(2);
    }

    #endregion

    #region FailBatchAsync Tests

    [Fact]
    public async Task FailBatchAsync_MultipleMessages_ReleasesAllAndIncrementsAttempts()
    {
        var provider = await CreateProviderAsync();
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid() };
        foreach (var id in ids)
        {
            var message = CreateInboxMessage(id: id);
            await provider.WriteAsync(message, CancellationToken.None);
        }

        // Capture messages
        var captured = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        captured.Should().HaveCount(2);

        await provider.FailBatchAsync(ids, CancellationToken.None);

        // Should be able to capture again
        var recaptured = await provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        recaptured.Should().HaveCount(2);
        recaptured.All(m => m.AttemptsCount == 1).Should().BeTrue();
    }

    [Fact]
    public async Task FailBatchAsync_EmptyList_DoesNothing()
    {
        var provider = await CreateProviderAsync();

        await provider.FailBatchAsync([], CancellationToken.None);

        // No exception should be thrown
    }

    #endregion

    #region ProcessResultsBatchAsync Tests

    [Fact]
    public async Task ProcessResultsBatchAsync_CompletesMessages_DeletesFromInbox()
    {
        var provider = await CreateProviderAsync();
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid() };
        foreach (var id in ids)
        {
            var message = CreateInboxMessage(id: id);
            await provider.WriteAsync(message, CancellationToken.None);
        }

        await provider.ProcessResultsBatchAsync(
            toComplete: ids,
            toFail: [],
            toRelease: [],
            toDeadLetter: [],
            CancellationToken.None);

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        messages.Should().BeEmpty("completed messages should be deleted");
    }

    [Fact]
    public async Task ProcessResultsBatchAsync_FailsMessages_ReleasesAndIncrementsAttempts()
    {
        var provider = await CreateProviderAsync();
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid() };
        foreach (var id in ids)
        {
            var message = CreateInboxMessage(id: id);
            await provider.WriteAsync(message, CancellationToken.None);
        }

        // Capture messages
        await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        await provider.ProcessResultsBatchAsync(
            toComplete: [],
            toFail: ids,
            toRelease: [],
            toDeadLetter: [],
            CancellationToken.None);

        var recaptured = await provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        recaptured.Should().HaveCount(2);
        recaptured.All(m => m.AttemptsCount == 1).Should().BeTrue();
    }

    [Fact]
    public async Task ProcessResultsBatchAsync_ReleasesMessages_ClearsCapture()
    {
        var provider = await CreateProviderAsync();
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid() };
        foreach (var id in ids)
        {
            var message = CreateInboxMessage(id: id);
            await provider.WriteAsync(message, CancellationToken.None);
        }

        // Capture messages
        await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        await provider.ProcessResultsBatchAsync(
            toComplete: [],
            toFail: [],
            toRelease: ids,
            toDeadLetter: [],
            CancellationToken.None);

        var recaptured = await provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        recaptured.Should().HaveCount(2, "messages should be released");
        recaptured.All(m => m.AttemptsCount == 0).Should().BeTrue("attempts should not increment");
    }

    [Fact]
    public async Task ProcessResultsBatchAsync_MovesToDeadLetter_RemovesFromMainAndAddsToDLQ()
    {
        var provider = await CreateProviderAsync(enableDeadLetter: true);
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid() };
        foreach (var id in ids)
        {
            var message = CreateInboxMessage(id: id);
            await provider.WriteAsync(message, CancellationToken.None);
        }

        await provider.ProcessResultsBatchAsync(
            toComplete: [],
            toFail: [],
            toRelease: [],
            toDeadLetter: ids.Select(id => (id, "Failed")).ToList(),
            CancellationToken.None);

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        messages.Should().BeEmpty("messages should be removed");

        var dlq = await provider.ReadDeadLettersAsync(10, CancellationToken.None);
        dlq.Should().HaveCount(2);
    }

    [Fact]
    public async Task ProcessResultsBatchAsync_MixedOperations_ProcessesAllCorrectly()
    {
        var provider = await CreateProviderAsync(enableDeadLetter: true);

        var completeId = Guid.NewGuid();
        var failId = Guid.NewGuid();
        var releaseId = Guid.NewGuid();
        var deadLetterId = Guid.NewGuid();

        foreach (var id in new[] { completeId, failId, releaseId, deadLetterId })
        {
            var message = CreateInboxMessage(id: id);
            await provider.WriteAsync(message, CancellationToken.None);
        }

        // Capture all messages
        await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        await provider.ProcessResultsBatchAsync(
            toComplete: [completeId],
            toFail: [failId],
            toRelease: [releaseId],
            toDeadLetter: [(deadLetterId, "Max retries")],
            CancellationToken.None);

        var remaining = await provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        remaining.Should().HaveCount(2, "complete and dead letter should be removed");

        var failedMsg = remaining.FirstOrDefault(m => m.Id == failId);
        failedMsg.Should().NotBeNull();
        failedMsg!.AttemptsCount.Should().Be(1);

        var releasedMsg = remaining.FirstOrDefault(m => m.Id == releaseId);
        releasedMsg.Should().NotBeNull();
        releasedMsg!.AttemptsCount.Should().Be(0);

        var dlq = await provider.ReadDeadLettersAsync(10, CancellationToken.None);
        dlq.Should().HaveCount(1);
        dlq[0].Id.Should().Be(deadLetterId);
    }

    [Fact]
    public async Task ProcessResultsBatchAsync_EmptyLists_DoesNothing()
    {
        var provider = await CreateProviderAsync();
        var id = Guid.NewGuid();
        var message = CreateInboxMessage(id: id);
        await provider.WriteAsync(message, CancellationToken.None);

        await provider.ProcessResultsBatchAsync([], [], [], [], CancellationToken.None);

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        messages.Should().HaveCount(1, "message should still exist");
    }

    #endregion

    #region ReadAndCaptureAsync Tests

    [Fact]
    public async Task ReadAndCaptureAsync_WithPendingMessages_CapturesMessages()
    {
        var provider = await CreateProviderAsync();
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
        foreach (var id in ids)
        {
            var message = CreateInboxMessage(id: id);
            await provider.WriteAsync(message, CancellationToken.None);
        }

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        messages.Should().HaveCount(3);
        messages.Select(m => m.Id).Should().BeEquivalentTo(ids);
        messages.All(m => m.CapturedBy == "processor-1").Should().BeTrue();
        messages.All(m => m.CapturedAt != null).Should().BeTrue();
    }

    [Fact]
    public async Task ReadAndCaptureAsync_NoMessages_ReturnsEmpty()
    {
        var provider = await CreateProviderAsync();

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        messages.Should().BeEmpty();
    }

    [Fact]
    public async Task ReadAndCaptureAsync_AlreadyCaptured_SkipsMessages()
    {
        var provider = await CreateProviderAsync();
        var capturedId = Guid.NewGuid();
        var pendingId = Guid.NewGuid();

        await provider.WriteAsync(CreateInboxMessage(id: capturedId), CancellationToken.None);
        await provider.WriteAsync(CreateInboxMessage(id: pendingId), CancellationToken.None);

        // First processor captures both
        var firstCapture = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        firstCapture.Should().HaveCount(2);

        // Second processor should get nothing
        var secondCapture = await provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        secondCapture.Should().BeEmpty("all messages are already captured");
    }

    [Fact]
    public async Task ReadAndCaptureAsync_ExpiredCapture_RecapturesMessage()
    {
        var provider = await CreateProviderAsync();
        var messageId = Guid.NewGuid();
        var message = CreateInboxMessage(id: messageId);
        await provider.WriteAsync(message, CancellationToken.None);

        // Capture with processor-1
        var firstCapture = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        firstCapture.Should().HaveCount(1);

        // Simulate expired capture by waiting or adjusting configuration
        // For this test, we'll manually set the captured time to be expired
        // Since InMemory provider uses configuration's MaxProcessingTime
        // We need to wait or use a shorter MaxProcessingTime
        await Task.Delay(100); // Small delay to ensure time difference

        // In real scenario with MaxProcessingTime of 5 minutes, this wouldn't work
        // For testing, we'd need a provider with short MaxProcessingTime
        // Let's create a new provider with very short processing time
        var shortTimeProvider = await CreateProviderAsync(maxProcessingTime: TimeSpan.FromMilliseconds(50));
        await shortTimeProvider.WriteAsync(CreateInboxMessage(id: Guid.NewGuid()), CancellationToken.None);
        await shortTimeProvider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        await Task.Delay(100); // Wait for expiration

        var recaptured = await shortTimeProvider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        recaptured.Should().HaveCount(1, "expired message should be recaptured");
    }

    [Fact]
    public async Task ReadAndCaptureAsync_RespectsBatchSize()
    {
        var provider = await CreateProviderAsync(batchSize: 2);
        for (int i = 0; i < 5; i++)
        {
            var message = CreateInboxMessage();
            await provider.WriteAsync(message, CancellationToken.None);
        }

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        messages.Should().HaveCount(2, "should respect batch size");
    }

    [Fact]
    public async Task ReadAndCaptureAsync_ReturnsMessagesInOrder()
    {
        var provider = await CreateProviderAsync();
        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();
        var id3 = Guid.NewGuid();

        // Insert with explicit timestamps to ensure order
        await provider.WriteAsync(CreateInboxMessage(id: id1, receivedAt: DateTime.UtcNow.AddSeconds(-3)), CancellationToken.None);
        await Task.Delay(10); // Small delay to ensure timestamp difference
        await provider.WriteAsync(CreateInboxMessage(id: id2, receivedAt: DateTime.UtcNow.AddSeconds(-2)), CancellationToken.None);
        await Task.Delay(10);
        await provider.WriteAsync(CreateInboxMessage(id: id3, receivedAt: DateTime.UtcNow.AddSeconds(-1)), CancellationToken.None);

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        messages.Should().HaveCount(3);
        messages[0].Id.Should().Be(id1, "oldest message should be first");
        messages[1].Id.Should().Be(id2);
        messages[2].Id.Should().Be(id3);
    }

    #endregion

    #region Helper Methods

    private static IProviderOptionsAccessor CreateMockOptionsAccessor(
        InMemoryDeduplicationStore? deduplicationStore = null,
        InMemoryDeadLetterStore? deadLetterStore = null)
    {
        var options = new InMemoryInboxProviderOptions
        {
            DeduplicationStore = deduplicationStore ?? new InMemoryDeduplicationStore(),
            DeadLetterStore = deadLetterStore ?? new InMemoryDeadLetterStore()
        };

        var mock = Substitute.For<IProviderOptionsAccessor>();
        mock.GetForInbox(InboxName).Returns(options);
        return mock;
    }

    private static IInboxConfiguration CreateConfiguration(
        bool enableDeadLetter = true,
        bool enableDeduplication = false,
        int batchSize = 100,
        TimeSpan? maxProcessingTime = null)
    {
        var options = Substitute.For<IInboxOptions>();
        options.InboxName.Returns(InboxName);
        options.ReadBatchSize.Returns(batchSize);
        options.MaxProcessingTime.Returns(maxProcessingTime ?? TimeSpan.FromMinutes(5));
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

    private static Task<InMemoryInboxStorageProvider> CreateProviderAsync(
        bool enableDeadLetter = true,
        bool enableDeduplication = false,
        int batchSize = 100,
        TimeSpan? maxProcessingTime = null)
    {
        // Create fresh stores for each provider
        var deduplicationStore = new InMemoryDeduplicationStore();
        var deadLetterStore = new InMemoryDeadLetterStore();

        var config = CreateConfiguration(enableDeadLetter, enableDeduplication, batchSize, maxProcessingTime);
        var optionsAccessor = CreateMockOptionsAccessor(deduplicationStore, deadLetterStore);
        var provider = new InMemoryInboxStorageProvider(optionsAccessor, config);

        return Task.FromResult(provider);
    }

    private static InboxMessage CreateInboxMessage(
        Guid? id = null,
        string? collapseKey = null,
        string? deduplicationId = null,
        string? groupId = null,
        string payload = "{}",
        DateTime? receivedAt = null)
    {
        return new InboxMessage
        {
            Id = id ?? Guid.NewGuid(),
            MessageType = "TestMessage",
            Payload = payload,
            GroupId = groupId,
            CollapseKey = collapseKey,
            DeduplicationId = deduplicationId,
            AttemptsCount = 0,
            ReceivedAt = receivedAt ?? DateTime.UtcNow
        };
    }

    private record MessageIdentifier(Guid Id, string? GroupId) : IInboxMessageIdentifiers;

    #endregion
}
