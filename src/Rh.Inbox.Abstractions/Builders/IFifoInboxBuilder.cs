using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;

namespace Rh.Inbox.Abstractions.Builders;

/// <summary>
/// Builder interface for configuring a FIFO inbox that processes messages one at a time while maintaining order within groups.
/// </summary>
public interface IFifoInboxBuilder : IInboxBuilderBase<IFifoInboxBuilder>
{
    /// <summary>
    /// Registers a FIFO message handler for the specified message type.
    /// </summary>
    /// <typeparam name="THandler">The handler type that implements <see cref="IFifoInboxHandler{TMessage}"/>.</typeparam>
    /// <typeparam name="TMessage">The message type to handle. Must implement <see cref="IHasGroupId"/>.</typeparam>
    /// <returns>The builder for chaining.</returns>
    IFifoInboxBuilder RegisterHandler<THandler, TMessage>()
        where THandler : class, IFifoInboxHandler<TMessage>
        where TMessage : class, IHasGroupId;

    /// <summary>
    /// Registers a FIFO message handler using a factory function.
    /// </summary>
    /// <typeparam name="TMessage">The message type to handle. Must implement <see cref="IHasGroupId"/>.</typeparam>
    /// <param name="handlerFactory">The factory function to create the handler.</param>
    /// <returns>The builder for chaining.</returns>
    IFifoInboxBuilder RegisterHandler<TMessage>(Func<IServiceProvider, IFifoInboxHandler<TMessage>> handlerFactory)
        where TMessage : class, IHasGroupId;

    /// <summary>
    /// Registers a FIFO message handler instance.
    /// </summary>
    /// <typeparam name="TMessage">The message type to handle. Must implement <see cref="IHasGroupId"/>.</typeparam>
    /// <param name="handler">The handler instance.</param>
    /// <returns>The builder for chaining.</returns>
    IFifoInboxBuilder RegisterHandler<TMessage>(IFifoInboxHandler<TMessage> handler)
        where TMessage : class, IHasGroupId;
}
