using Rh.Inbox.Abstractions.Messages;

namespace Rh.Inbox.Abstractions.Storage;

/// <summary>
/// Interface for inbox storage providers.
/// Implement this interface to create custom storage backends (e.g., PostgreSQL, SQL Server, Redis).
/// </summary>
public interface IInboxStorageProvider
{
    /// <summary>
    /// Writes a single message to storage.
    /// If the message has a collapse key, existing uncaptured messages with the same key are removed.
    /// </summary>
    /// <param name="message">The message to write.</param>
    /// <param name="token">Cancellation token.</param>
    Task WriteAsync(InboxMessage message, CancellationToken token = default);

    /// <summary>
    /// Writes multiple messages to storage in a batch.
    /// If any messages have collapse keys, existing uncaptured messages with matching keys are removed.
    /// </summary>
    /// <param name="messages">The messages to write.</param>
    /// <param name="token">Cancellation token.</param>
    Task WriteBatchAsync(IEnumerable<InboxMessage> messages, CancellationToken token = default);

    /// <summary>
    /// Reads available messages and marks them as captured for processing.
    /// Returns messages that are not currently captured or whose capture has expired.
    /// </summary>
    /// <param name="processorId">Identifier of the processor capturing the messages.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>A list of captured messages ready for processing.</returns>
    Task<IReadOnlyList<InboxMessage>> ReadAndCaptureAsync(string processorId, CancellationToken token = default);


    /// <summary>
    /// Marks a message as failed, incrementing its attempt count and releasing it for retry.
    /// </summary>
    /// <param name="messageId">The ID of the message that failed.</param>
    /// <param name="token">Cancellation token.</param>
    Task FailAsync(Guid messageId, CancellationToken token = default);

    /// <summary>
    /// Marks multiple messages as failed, incrementing their attempt counts and releasing them for retry.
    /// </summary>
    /// <param name="messageIds">The IDs of the messages that failed.</param>
    /// <param name="token">Cancellation token.</param>
    Task FailBatchAsync(IReadOnlyList<Guid> messageIds, CancellationToken token = default);

    /// <summary>
    /// Moves a message to the dead-letter queue.
    /// </summary>
    /// <param name="messageId">The ID of the message to move.</param>
    /// <param name="reason">The reason for moving to dead-letter.</param>
    /// <param name="token">Cancellation token.</param>
    Task MoveToDeadLetterAsync(Guid messageId, string reason, CancellationToken token = default);

    /// <summary>
    /// Moves multiple messages to the dead-letter queue.
    /// </summary>
    /// <param name="messages">The messages to move with their failure reasons.</param>
    /// <param name="token">Cancellation token.</param>
    Task MoveToDeadLetterBatchAsync(IReadOnlyList<(Guid MessageId, string Reason)> messages, CancellationToken token = default);

    /// <summary>
    /// Releases multiple captured messages without completing or failing them.
    /// </summary>
    /// <param name="messageIds">The IDs of the messages to release.</param>
    /// <param name="token">Cancellation token.</param>
    Task ReleaseBatchAsync(IReadOnlyList<Guid> messageIds, CancellationToken token = default);

    /// <summary>
    /// Reads messages from the dead-letter queue.
    /// </summary>
    /// <param name="count">Maximum number of messages to read.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>A list of dead-letter messages.</returns>
    Task<IReadOnlyList<DeadLetterMessage>> ReadDeadLettersAsync(int count, CancellationToken token = default);

    /// <summary>
    /// Processes multiple message results in a single operation.
    /// Executes complete, fail, release, and dead-letter operations in one connection.
    /// </summary>
    /// <param name="toComplete">Message IDs to mark as completed.</param>
    /// <param name="toFail">Message IDs to mark as failed.</param>
    /// <param name="toRelease">Message IDs to release for retry.</param>
    /// <param name="toDeadLetter">Messages to move to dead-letter with reasons.</param>
    /// <param name="token">Cancellation token.</param>
    Task ProcessResultsBatchAsync(
        IReadOnlyList<Guid> toComplete,
        IReadOnlyList<Guid> toFail,
        IReadOnlyList<Guid> toRelease,
        IReadOnlyList<(Guid MessageId, string Reason)> toDeadLetter,
        CancellationToken token = default);

    /// <summary>
    /// Extends the locks for captured messages to prevent expiration during long-running processing.
    /// </summary>
    /// <remarks>
    /// This method is called by the processing loop when lock extension is enabled.
    /// Base implementations extend message capture locks (captured_at).
    /// FIFO providers override to also extend group locks.
    /// </remarks>
    /// <param name="processorId">The processor ID that owns the locks (for validation).</param>
    /// <param name="capturedMessages">The messages whose locks should be extended.</param>
    /// <param name="newCapturedAt">The new timestamp to use for captured_at/locked_at.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>The number of message locks successfully extended.</returns>
    Task<int> ExtendLocksAsync(
        string processorId,
        IReadOnlyList<IInboxMessageIdentifiers> capturedMessages,
        DateTime newCapturedAt,
        CancellationToken token = default);
}