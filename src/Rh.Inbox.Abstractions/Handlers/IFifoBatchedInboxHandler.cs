using Rh.Inbox.Abstractions.Messages;

namespace Rh.Inbox.Abstractions.Handlers;

/// <summary>
/// Handler interface for processing messages from a FIFO batched inbox.
/// Messages are delivered in batches grouped by GroupId and message type,
/// with ordering guaranteed within each group.
/// </summary>
/// <typeparam name="TMessage">The type of message this handler processes. Must implement <see cref="IHasGroupId"/>.</typeparam>
public interface IFifoBatchedInboxHandler<TMessage> where TMessage : class, IHasGroupId
{
    /// <summary>
    /// Handles a batch of messages belonging to the same group.
    /// </summary>
    /// <param name="groupId">The group identifier for all messages in this batch.</param>
    /// <param name="messages">The messages to process, wrapped in envelopes containing the message ID and payload.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>A list of results indicating the outcome for each message.</returns>
    Task<IReadOnlyList<InboxMessageResult>> HandleAsync(
        string groupId,
        IReadOnlyList<InboxMessageEnvelope<TMessage>> messages,
        CancellationToken token = default);
}
