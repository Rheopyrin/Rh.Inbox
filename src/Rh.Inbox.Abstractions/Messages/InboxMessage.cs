namespace Rh.Inbox.Abstractions.Messages;

/// <summary>
/// Represents a message stored in an inbox.
/// This is the internal storage representation used by storage providers.
/// </summary>
public class InboxMessage : IInboxMessageIdentifiers
{
    /// <summary>
    /// Gets or sets the unique identifier for this message.
    /// </summary>
    public Guid Id { get; init; }

    /// <summary>
    /// Gets or sets the fully qualified type name of the message payload.
    /// </summary>
    public required string MessageType { get; init; }

    /// <summary>
    /// Gets or sets the serialized message payload.
    /// </summary>
    public required string Payload { get; init; }

    /// <summary>
    /// Gets or sets the group identifier for FIFO ordering.
    /// Messages with the same GroupId are processed in order.
    /// </summary>
    public string? GroupId { get; init; }

    /// <summary>
    /// Gets or sets the collapse key for message deduplication.
    /// When a new message arrives with the same collapse key, older uncaptured messages are removed.
    /// </summary>
    public string? CollapseKey { get; init; }

    /// <summary>
    /// Gets or sets the deduplication identifier for this message.
    /// Messages with the same DeduplicationId are considered duplicates.
    /// </summary>
    public string? DeduplicationId { get; init; }

    /// <summary>
    /// Gets or sets the number of processing attempts for this message.
    /// </summary>
    public int AttemptsCount { get; set; }

    /// <summary>
    /// Gets or sets the timestamp when this message was received by the inbox.
    /// </summary>
    public DateTime ReceivedAt { get; init; }

    /// <summary>
    /// Gets or sets the timestamp when this message was captured for processing.
    /// Null if the message is not currently being processed.
    /// </summary>
    public DateTime? CapturedAt { get; set; }

    /// <summary>
    /// Gets or sets the identifier of the processor that captured this message.
    /// Null if the message is not currently being processed.
    /// </summary>
    public string? CapturedBy { get; set; }
}