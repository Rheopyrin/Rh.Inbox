namespace Rh.Inbox.Abstractions.Messages;

/// <summary>
/// Implement this interface on message classes to enable message collapsing (deduplication).
/// When a new message with the same collapse key arrives, older uncaptured messages
/// with the same key are automatically removed, ensuring only the latest message is processed.
/// </summary>
public interface IHasCollapseKey
{
    /// <summary>
    /// Gets the collapse key for this message.
    /// Messages with the same collapse key will deduplicate, keeping only the most recent.
    /// </summary>
    /// <returns>The collapse key string.</returns>
    string GetCollapseKey();
}
