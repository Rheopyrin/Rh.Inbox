namespace Rh.Inbox.Abstractions.Handlers;

/// <summary>
/// Handler interface for processing messages from a batched inbox.
/// Messages are delivered in batches grouped by message type for efficient processing.
/// </summary>
/// <typeparam name="TMessage">The type of message this handler processes.</typeparam>
public interface IBatchedInboxHandler<TMessage> where TMessage : class
{
    /// <summary>
    /// Handles a batch of messages of the same type.
    /// </summary>
    /// <param name="messages">The messages to process, wrapped in envelopes containing the message ID and payload.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>A list of results indicating the outcome for each message.</returns>
    Task<IReadOnlyList<InboxMessageResult>> HandleAsync(
        IReadOnlyList<InboxMessageEnvelope<TMessage>> messages,
        CancellationToken token = default);
}
