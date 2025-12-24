namespace Rh.Inbox.Abstractions.Messages;

/// <summary>
/// Implement this interface on message classes to provide a custom received timestamp.
/// When implemented, the inbox will use this timestamp instead of the current UTC time.
/// Useful for preserving original timestamps when replaying or migrating messages.
/// </summary>
public interface IHasReceivedAt
{
    /// <summary>
    /// Gets the timestamp when this message was received.
    /// This timestamp will be used as the message's ReceivedAt value in storage.
    /// </summary>
    /// <returns>The received timestamp.</returns>
    DateTime GetReceivedAt();
}
