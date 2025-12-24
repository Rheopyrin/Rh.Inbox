namespace Rh.Inbox.Abstractions.Messages;

/// <summary>
/// Implement this interface on message classes to provide a custom message identifier.
/// When implemented, the inbox will use this ID instead of generating a new GUID.
/// Useful for idempotency when the same message might be written multiple times.
/// </summary>
public interface IHasExternalId
{
    /// <summary>
    /// Gets the external identifier for this message.
    /// This ID will be used as the message's unique identifier in storage.
    /// </summary>
    /// <returns>The external identifier GUID.</returns>
    Guid GetId();
}
