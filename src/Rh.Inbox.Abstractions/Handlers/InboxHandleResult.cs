namespace Rh.Inbox.Abstractions.Handlers;

/// <summary>
/// Represents the result of processing a single message.
/// </summary>
public enum InboxHandleResult
{
    /// <summary>
    /// Message was processed successfully and will be removed from the inbox.
    /// </summary>
    Success,

    /// <summary>
    /// Message processing failed. The message will be retried up to the configured maximum attempts.
    /// </summary>
    Failed,

    /// <summary>
    /// Message should be retried immediately. The message remains in the inbox for reprocessing.
    /// </summary>
    Retry,

    /// <summary>
    /// Message should be moved to the dead-letter queue without further retry attempts.
    /// </summary>
    MoveToDeadLetter
}
