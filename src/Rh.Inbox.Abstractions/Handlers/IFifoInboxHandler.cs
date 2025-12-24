using Rh.Inbox.Abstractions.Messages;

namespace Rh.Inbox.Abstractions.Handlers;

/// <summary>
/// Handler interface for processing messages from a FIFO inbox.
/// Messages are delivered one at a time, with ordering guaranteed within each group.
/// </summary>
/// <typeparam name="TMessage">The type of message this handler processes. Must implement <see cref="IHasGroupId"/>.</typeparam>
public interface IFifoInboxHandler<TMessage> where TMessage : class, IHasGroupId
{
    /// <summary>
    /// Handles a single message.
    /// </summary>
    /// <param name="message">The message to process, wrapped in an envelope containing the message ID and payload.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>The result indicating the outcome of processing.</returns>
    Task<InboxHandleResult> HandleAsync(InboxMessageEnvelope<TMessage> message, CancellationToken token = default);
}