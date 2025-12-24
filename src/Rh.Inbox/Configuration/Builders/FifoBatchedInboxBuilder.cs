using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions.Builders;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;

namespace Rh.Inbox.Configuration.Builders;

internal sealed class FifoBatchedInboxBuilder : TypedInboxBuilder<FifoBatchedInboxBuilder>, IFifoBatchedInboxBuilder
{
    internal FifoBatchedInboxBuilder(string inboxName)
        : base(inboxName)
    {
    }

    protected override InboxType Type => InboxType.FifoBatched;

    public FifoBatchedInboxBuilder RegisterHandler<THandler, TMessage>()
        where THandler : class, IFifoBatchedInboxHandler<TMessage>
        where TMessage : class, IHasGroupId
        => RegisterKeyedHandler<THandler, IFifoBatchedInboxHandler<TMessage>, TMessage>();

    public FifoBatchedInboxBuilder RegisterHandler<TMessage>(Func<IServiceProvider, IFifoBatchedInboxHandler<TMessage>> handlerFactory)
        where TMessage : class, IHasGroupId
        => RegisterKeyedHandler<IFifoBatchedInboxHandler<TMessage>, TMessage>(handlerFactory);

    public FifoBatchedInboxBuilder RegisterHandler<TMessage>(IFifoBatchedInboxHandler<TMessage> handler)
        where TMessage : class, IHasGroupId
        => RegisterKeyedHandler<IFifoBatchedInboxHandler<TMessage>, TMessage>(handler);

    #region IFifoBatchedInboxBuilder explicit implementation

    IFifoBatchedInboxBuilder IInboxBuilderBase<IFifoBatchedInboxBuilder>.UseStorageProviderFactory(IInboxStorageProviderFactory factory)
        => UseStorageProviderFactory(factory);

    IFifoBatchedInboxBuilder IInboxBuilderBase<IFifoBatchedInboxBuilder>.UseStorageProviderFactory<TFactory>()
        => UseStorageProviderFactory<TFactory>();

    IFifoBatchedInboxBuilder IInboxBuilderBase<IFifoBatchedInboxBuilder>.UseStorageProviderFactory(Func<IServiceProvider, IInboxStorageProviderFactory> factoryFunc)
        => UseStorageProviderFactory(factoryFunc);

    IFifoBatchedInboxBuilder IInboxBuilderBase<IFifoBatchedInboxBuilder>.UseSerializerFactory(IInboxSerializerFactory factory)
        => UseSerializerFactory(factory);

    IFifoBatchedInboxBuilder IInboxBuilderBase<IFifoBatchedInboxBuilder>.UseSerializerFactory<TFactory>()
        => UseSerializerFactory<TFactory>();

    IFifoBatchedInboxBuilder IInboxBuilderBase<IFifoBatchedInboxBuilder>.UseSerializerFactory<TFactory>(Func<IServiceProvider, TFactory> factoryFunc)
        => UseSerializerFactory(factoryFunc);

    IFifoBatchedInboxBuilder IInboxBuilderBase<IFifoBatchedInboxBuilder>.RegisterMessage<TMessage>(string? messageType)
        => RegisterMessage<TMessage>(messageType);

    IFifoBatchedInboxBuilder IInboxBuilderBase<IFifoBatchedInboxBuilder>.ConfigureHealthCheck(Action<IInboxHealthCheckOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return ConfigureHealthCheck(opts => configure(opts));
    }

    IFifoBatchedInboxBuilder IInboxBuilderBase<IFifoBatchedInboxBuilder>.ConfigureServices(Action<IServiceCollection> configure)
        => ConfigureServices(configure);

    IFifoBatchedInboxBuilder IInboxBuilderBase<IFifoBatchedInboxBuilder>.PostConfigure(Action<IInboxConfiguration, IServiceCollection> action)
        => PostConfigure(action);

    IFifoBatchedInboxBuilder IInboxBuilderBase<IFifoBatchedInboxBuilder>.ConfigureOptions(Action<IConfigureInboxOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return ConfigureOptions(opts => configure(opts));
    }

    IFifoBatchedInboxBuilder IFifoBatchedInboxBuilder.RegisterHandler<THandler, TMessage>()
        => RegisterHandler<THandler, TMessage>();

    IFifoBatchedInboxBuilder IFifoBatchedInboxBuilder.RegisterHandler<TMessage>(Func<IServiceProvider, IFifoBatchedInboxHandler<TMessage>> handlerFactory)
        => RegisterHandler(handlerFactory);

    IFifoBatchedInboxBuilder IFifoBatchedInboxBuilder.RegisterHandler<TMessage>(IFifoBatchedInboxHandler<TMessage> handler)
        => RegisterHandler(handler);

    #endregion
}