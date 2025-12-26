using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;

namespace Rh.Inbox.Processing;

/// <summary>
/// Context for message processing that tracks in-flight messages and provides
/// storage operations. Messages are automatically removed from in-flight tracking
/// when they are processed (completed, failed, or moved to dead letter).
/// </summary>
internal interface IMessageProcessingContext
{
    /// <summary>
    /// Gets messages that are still being processed (not yet completed/failed/dead-lettered).
    /// </summary>
    IReadOnlyList<InboxMessage> GetInFlightMessages();

    /// <summary>
    /// Processes multiple message results in a batch operation.
    /// Automatically removes processed messages from in-flight tracking.
    /// </summary>
    /// <param name="results">The results from handler execution.</param>
    /// <param name="token">Cancellation token.</param>
    Task ProcessResultsBatchAsync(
        IReadOnlyList<InboxMessageResult> results,
        CancellationToken token);

    /// <summary>
    /// Marks a message as failed. Checks max attempts and moves to dead letter if exceeded.
    /// Automatically removes from in-flight tracking.
    /// </summary>
    /// <param name="message">The message that failed.</param>
    /// <param name="token">Cancellation token.</param>
    Task FailMessageAsync(InboxMessage message, CancellationToken token);

    /// <summary>
    /// Marks multiple messages as failed. Checks max attempts and moves to dead letter if exceeded.
    /// Automatically removes from in-flight tracking.
    /// </summary>
    /// <param name="messages">The messages that failed.</param>
    /// <param name="token">Cancellation token.</param>
    Task FailMessageBatchAsync(IReadOnlyList<InboxMessage> messages, CancellationToken token);

    /// <summary>
    /// Moves a message to dead letter queue.
    /// Automatically removes from in-flight tracking.
    /// </summary>
    /// <param name="message">The message to move.</param>
    /// <param name="reason">The reason for moving to dead letter.</param>
    /// <param name="token">Cancellation token.</param>
    Task MoveToDeadLetterAsync(InboxMessage message, string reason, CancellationToken token);

    /// <summary>
    /// Moves multiple messages to dead letter queue.
    /// Automatically removes from in-flight tracking.
    /// </summary>
    /// <param name="messages">The messages to move with their reasons.</param>
    /// <param name="token">Cancellation token.</param>
    Task MoveToDeadLetterBatchAsync(IReadOnlyList<(InboxMessage Message, string Reason)> messages, CancellationToken token);

    /// <summary>
    /// Clears all in-flight tracking (called after batch processing completes).
    /// </summary>
    void Clear();
}
