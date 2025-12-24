using Rh.Inbox.Abstractions.Storage;

namespace Rh.Inbox.Abstractions;

/// <summary>
/// Base interface for all inbox types.
/// An inbox is a reliable message queue that ensures at-least-once delivery
/// with support for message grouping, collapsing, and dead-letter handling.
/// </summary>
public interface IInbox
{
    /// <summary>
    /// Gets the unique name of this inbox.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Gets the type of this inbox (Default, Fifo, or FifoBatched).
    /// </summary>
    InboxType Type { get; }
}
