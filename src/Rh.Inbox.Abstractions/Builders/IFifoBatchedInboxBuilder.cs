using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;

namespace Rh.Inbox.Abstractions.Builders;

/// <summary>
/// Builder interface for configuring a FIFO batched inbox that processes messages in batches while maintaining order within groups.
/// </summary>
public interface IFifoBatchedInboxBuilder : IInboxBuilderBase<IFifoBatchedInboxBuilder>
{
    /// <summary>
    /// Registers a FIFO batched message handler for the specified message type.
    /// </summary>
    /// <typeparam name="THandler">The handler type that implements <see cref="IFifoBatchedInboxHandler{TMessage}"/>.</typeparam>
    /// <typeparam name="TMessage">The message type to handle. Must implement <see cref="IHasGroupId"/>.</typeparam>
    /// <returns>The builder for chaining.</returns>
    IFifoBatchedInboxBuilder RegisterHandler<THandler, TMessage>()
        where THandler : class, IFifoBatchedInboxHandler<TMessage>
        where TMessage : class, IHasGroupId;

    /// <summary>
    /// Registers a FIFO batched message handler using a factory function.
    /// </summary>
    /// <typeparam name="TMessage">The message type to handle. Must implement <see cref="IHasGroupId"/>.</typeparam>
    /// <param name="handlerFactory">The factory function to create the handler.</param>
    /// <returns>The builder for chaining.</returns>
    IFifoBatchedInboxBuilder RegisterHandler<TMessage>(Func<IServiceProvider, IFifoBatchedInboxHandler<TMessage>> handlerFactory)
        where TMessage : class, IHasGroupId;

    /// <summary>
    /// Registers a FIFO batched message handler instance.
    /// </summary>
    /// <typeparam name="TMessage">The message type to handle. Must implement <see cref="IHasGroupId"/>.</typeparam>
    /// <param name="handler">The handler instance.</param>
    /// <returns>The builder for chaining.</returns>
    IFifoBatchedInboxBuilder RegisterHandler<TMessage>(IFifoBatchedInboxHandler<TMessage> handler)
        where TMessage : class, IHasGroupId;
}
