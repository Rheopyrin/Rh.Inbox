using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions.Builders;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;

namespace Rh.Inbox.Configuration.Builders;

internal sealed class FifoInboxBuilder : TypedInboxBuilder<FifoInboxBuilder>, IFifoInboxBuilder
{
    internal FifoInboxBuilder(string inboxName)
        : base(inboxName)
    {
    }

    protected override InboxType Type => InboxType.Fifo;

    public FifoInboxBuilder RegisterHandler<THandler, TMessage>()
        where THandler : class, IFifoInboxHandler<TMessage>
        where TMessage : class, IHasGroupId
        => RegisterKeyedHandler<THandler, IFifoInboxHandler<TMessage>, TMessage>();

    public FifoInboxBuilder RegisterHandler<TMessage>(Func<IServiceProvider, IFifoInboxHandler<TMessage>> handlerFactory)
        where TMessage : class, IHasGroupId
        => RegisterKeyedHandler<IFifoInboxHandler<TMessage>, TMessage>(handlerFactory);

    public FifoInboxBuilder RegisterHandler<TMessage>(IFifoInboxHandler<TMessage> handler)
        where TMessage : class, IHasGroupId
        => RegisterKeyedHandler<IFifoInboxHandler<TMessage>, TMessage>(handler);

    #region IFifoInboxBuilder explicit implementation

    IFifoInboxBuilder IInboxBuilderBase<IFifoInboxBuilder>.UseStorageProviderFactory(IInboxStorageProviderFactory factory)
        => UseStorageProviderFactory(factory);

    IFifoInboxBuilder IInboxBuilderBase<IFifoInboxBuilder>.UseStorageProviderFactory<TFactory>()
        => UseStorageProviderFactory<TFactory>();

    IFifoInboxBuilder IInboxBuilderBase<IFifoInboxBuilder>.UseStorageProviderFactory(Func<IServiceProvider, IInboxStorageProviderFactory> factoryFunc)
        => UseStorageProviderFactory(factoryFunc);

    IFifoInboxBuilder IInboxBuilderBase<IFifoInboxBuilder>.UseSerializerFactory(IInboxSerializerFactory factory)
        => UseSerializerFactory(factory);

    IFifoInboxBuilder IInboxBuilderBase<IFifoInboxBuilder>.UseSerializerFactory<TFactory>()
        => UseSerializerFactory<TFactory>();

    IFifoInboxBuilder IInboxBuilderBase<IFifoInboxBuilder>.UseSerializerFactory<TFactory>(Func<IServiceProvider, TFactory> factoryFunc)
        => UseSerializerFactory(factoryFunc);

    IFifoInboxBuilder IInboxBuilderBase<IFifoInboxBuilder>.RegisterMessage<TMessage>(string? messageType)
        => RegisterMessage<TMessage>(messageType);

    IFifoInboxBuilder IInboxBuilderBase<IFifoInboxBuilder>.ConfigureHealthCheck(Action<IInboxHealthCheckOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return ConfigureHealthCheck(opts => configure(opts));
    }

    IFifoInboxBuilder IInboxBuilderBase<IFifoInboxBuilder>.ConfigureServices(Action<IServiceCollection> configure)
        => ConfigureServices(configure);

    IFifoInboxBuilder IInboxBuilderBase<IFifoInboxBuilder>.PostConfigure(Action<IInboxConfiguration, IServiceCollection> action)
        => PostConfigure(action);

    IFifoInboxBuilder IInboxBuilderBase<IFifoInboxBuilder>.ConfigureOptions(Action<IConfigureInboxOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return ConfigureOptions(opts => configure(opts));
    }

    IFifoInboxBuilder IFifoInboxBuilder.RegisterHandler<THandler, TMessage>()
        => RegisterHandler<THandler, TMessage>();

    IFifoInboxBuilder IFifoInboxBuilder.RegisterHandler<TMessage>(Func<IServiceProvider, IFifoInboxHandler<TMessage>> handlerFactory)
        => RegisterHandler(handlerFactory);

    IFifoInboxBuilder IFifoInboxBuilder.RegisterHandler<TMessage>(IFifoInboxHandler<TMessage> handler)
        => RegisterHandler(handler);

    #endregion
}