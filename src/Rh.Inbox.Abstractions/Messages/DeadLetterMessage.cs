namespace Rh.Inbox.Abstractions.Messages;

/// <summary>
/// Represents a message that has been moved to the dead-letter queue
/// after exceeding retry attempts or being explicitly marked for dead-lettering.
/// </summary>
public class DeadLetterMessage
{
    /// <summary>
    /// Gets or sets the unique identifier for this message (same as original message ID).
    /// </summary>
    public Guid Id { get; set; }

    /// <summary>
    /// Gets or sets the name of the inbox this message originated from.
    /// </summary>
    public required string InboxName { get; set; }

    /// <summary>
    /// Gets or sets the fully qualified type name of the message payload.
    /// </summary>
    public required string MessageType { get; set; }

    /// <summary>
    /// Gets or sets the serialized message payload.
    /// </summary>
    public required string Payload { get; set; }

    /// <summary>
    /// Gets or sets the group identifier for FIFO ordering (if applicable).
    /// </summary>
    public string? GroupId { get; set; }

    /// <summary>
    /// Gets or sets the collapse key (if applicable).
    /// </summary>
    public string? CollapseKey { get; set; }

    /// <summary>
    /// Gets or sets the number of processing attempts before dead-lettering.
    /// </summary>
    public int AttemptsCount { get; set; }

    /// <summary>
    /// Gets or sets the timestamp when this message was originally received.
    /// </summary>
    public DateTime ReceivedAt { get; set; }

    /// <summary>
    /// Gets or sets the reason why this message was moved to the dead-letter queue.
    /// </summary>
    public required string FailureReason { get; set; }

    /// <summary>
    /// Gets or sets the timestamp when this message was moved to the dead-letter queue.
    /// </summary>
    public DateTime MovedAt { get; set; }
}
