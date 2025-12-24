using Rh.Inbox.Abstractions.Handlers;

namespace Rh.Inbox.Abstractions.Builders;

/// <summary>
/// Builder interface for configuring a default inbox that processes messages one at a time.
/// </summary>
public interface IDefaultInboxBuilder : IInboxBuilderBase<IDefaultInboxBuilder>
{
    /// <summary>
    /// Registers a message handler for the specified message type.
    /// </summary>
    /// <typeparam name="THandler">The handler type that implements <see cref="IInboxHandler{TMessage}"/>.</typeparam>
    /// <typeparam name="TMessage">The message type to handle.</typeparam>
    /// <returns>The builder for chaining.</returns>
    IDefaultInboxBuilder RegisterHandler<THandler, TMessage>()
        where THandler : class, IInboxHandler<TMessage>
        where TMessage : class;

    /// <summary>
    /// Registers a message handler using a factory function.
    /// </summary>
    /// <typeparam name="TMessage">The message type to handle.</typeparam>
    /// <param name="handlerFactory">The factory function to create the handler.</param>
    /// <returns>The builder for chaining.</returns>
    IDefaultInboxBuilder RegisterHandler<TMessage>(Func<IServiceProvider, IInboxHandler<TMessage>> handlerFactory)
        where TMessage : class;

    /// <summary>
    /// Registers a message handler instance.
    /// </summary>
    /// <typeparam name="TMessage">The message type to handle.</typeparam>
    /// <param name="handler">The handler instance.</param>
    /// <returns>The builder for chaining.</returns>
    IDefaultInboxBuilder RegisterHandler<TMessage>(IInboxHandler<TMessage> handler)
        where TMessage : class;
}
