namespace Rh.Inbox.Abstractions.Storage;

/// <summary>
/// Specifies the type of inbox and its processing behavior.
/// </summary>
public enum InboxType
{
    /// <summary>
    /// Default inbox type. Messages are processed one at a time, grouped by message type.
    /// No ordering guarantees between messages.
    /// </summary>
    Default,

    /// <summary>
    /// Batched inbox type. Messages are processed in batches grouped by message type.
    /// No ordering guarantees between messages.
    /// </summary>
    Batched,

    /// <summary>
    /// FIFO inbox type. Messages are processed one at a time with strict ordering within each group.
    /// Messages with the same GroupId are guaranteed to be processed in order.
    /// </summary>
    Fifo,

    /// <summary>
    /// FIFO batched inbox type. Messages are processed in batches grouped by GroupId and message type.
    /// All messages for a group are delivered together while maintaining order within the group.
    /// </summary>
    FifoBatched
}
