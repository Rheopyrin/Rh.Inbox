namespace Rh.Inbox.Abstractions.Health;

/// <summary>
/// Health metrics for an inbox, used for monitoring and alerting.
/// </summary>
/// <param name="PendingCount">Number of messages waiting to be processed.</param>
/// <param name="CapturedCount">Number of messages currently being processed.</param>
/// <param name="DeadLetterCount">Number of messages in the dead-letter queue.</param>
/// <param name="OldestPendingMessageAt">Timestamp of the oldest pending message, or null if no pending messages.</param>
public record InboxHealthMetrics(
    long PendingCount,
    long CapturedCount,
    long DeadLetterCount,
    DateTime? OldestPendingMessageAt);
