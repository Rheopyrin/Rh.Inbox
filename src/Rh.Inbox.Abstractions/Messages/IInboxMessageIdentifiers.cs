namespace Rh.Inbox.Abstractions.Messages;

/// <summary>
/// Provides message identification properties for lock operations.
/// </summary>
/// <remarks>
/// This interface is implemented by <see cref="InboxMessage"/> and used by storage providers
/// to identify messages when extending locks or releasing messages and group locks.
/// </remarks>
public interface IInboxMessageIdentifiers
{
    /// <summary>
    /// Gets the unique identifier for this message.
    /// </summary>
    Guid Id { get; }

    /// <summary>
    /// Gets the group identifier for FIFO ordering.
    /// Messages with the same GroupId are processed in order.
    /// </summary>
    string? GroupId { get; }
}
