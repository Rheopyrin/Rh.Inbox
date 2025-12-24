namespace Rh.Inbox.Abstractions.Messages;

/// <summary>
/// Implement this interface on message classes to enable FIFO ordering.
/// Messages with the same GroupId are guaranteed to be processed in order.
/// Required for messages used with Fifo or FifoBatched inbox types.
/// </summary>
public interface IHasGroupId
{
    /// <summary>
    /// Gets the group identifier for this message.
    /// Messages with the same GroupId are processed in FIFO order.
    /// </summary>
    /// <returns>The group identifier string.</returns>
    string GetGroupId();
}
