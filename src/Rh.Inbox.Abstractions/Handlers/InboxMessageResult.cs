namespace Rh.Inbox.Abstractions.Handlers;

/// <summary>
/// Represents the processing result for a specific message in a batch.
/// </summary>
/// <param name="MessageId">The unique identifier of the processed message.</param>
/// <param name="Result">The outcome of processing this message.</param>
/// <param name="FailureReason">Optional reason for failure, used when moving to dead-letter queue.</param>
public record InboxMessageResult(
    Guid MessageId,
    InboxHandleResult Result,
    string? FailureReason = null);
