using Rh.Inbox.Abstractions.Handlers;

namespace Rh.Inbox.Abstractions.Builders;

/// <summary>
/// Builder interface for configuring a batched inbox that processes messages in batches.
/// </summary>
public interface IBatchedInboxBuilder : IInboxBuilderBase<IBatchedInboxBuilder>
{
    /// <summary>
    /// Registers a batched message handler for the specified message type.
    /// </summary>
    /// <typeparam name="THandler">The handler type that implements <see cref="IBatchedInboxHandler{TMessage}"/>.</typeparam>
    /// <typeparam name="TMessage">The message type to handle.</typeparam>
    /// <returns>The builder for chaining.</returns>
    IBatchedInboxBuilder RegisterHandler<THandler, TMessage>()
        where THandler : class, IBatchedInboxHandler<TMessage>
        where TMessage : class;

    /// <summary>
    /// Registers a batched message handler using a factory function.
    /// </summary>
    /// <typeparam name="TMessage">The message type to handle.</typeparam>
    /// <param name="handlerFactory">The factory function to create the handler.</param>
    /// <returns>The builder for chaining.</returns>
    IBatchedInboxBuilder RegisterHandler<TMessage>(Func<IServiceProvider, IBatchedInboxHandler<TMessage>> handlerFactory)
        where TMessage : class;

    /// <summary>
    /// Registers a batched message handler instance.
    /// </summary>
    /// <typeparam name="TMessage">The message type to handle.</typeparam>
    /// <param name="handler">The handler instance.</param>
    /// <returns>The builder for chaining.</returns>
    IBatchedInboxBuilder RegisterHandler<TMessage>(IBatchedInboxHandler<TMessage> handler)
        where TMessage : class;
}
