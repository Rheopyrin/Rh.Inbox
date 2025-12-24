namespace Rh.Inbox.Abstractions.Handlers;

/// <summary>
/// Handler interface for processing messages from a default inbox.
/// Messages are delivered one at a time for processing.
/// </summary>
/// <typeparam name="TMessage">The type of message this handler processes.</typeparam>
public interface IInboxHandler<TMessage> where TMessage : class
{
    /// <summary>
    /// Handles a single message.
    /// </summary>
    /// <param name="message">The message to process, wrapped in an envelope containing the message ID and payload.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>The result indicating the outcome of processing.</returns>
    Task<InboxHandleResult> HandleAsync(
        InboxMessageEnvelope<TMessage> message,
        CancellationToken token = default);
}
