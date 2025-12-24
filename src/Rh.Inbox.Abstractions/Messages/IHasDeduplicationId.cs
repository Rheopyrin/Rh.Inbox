namespace Rh.Inbox.Abstractions.Messages;

/// <summary>
/// Implement this interface on message classes to support deduplication.
/// Messages with the same DeduplicationId will be deduplicated.
/// </summary>
public interface IHasDeduplicationId
{
    /// <summary>
    /// Gets the deduplication identifier for this message.
    /// Messages with the same DeduplicationId are considered duplicates.
    /// </summary>
    /// <returns>The deduplication identifier string.</returns>
    string GetDeduplicationId();
}
