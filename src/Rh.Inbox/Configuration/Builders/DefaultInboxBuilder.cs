using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions.Builders;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;

namespace Rh.Inbox.Configuration.Builders;

internal sealed class DefaultInboxBuilder : TypedInboxBuilder<DefaultInboxBuilder>, IDefaultInboxBuilder
{
    internal DefaultInboxBuilder(string inboxName)
        : base(inboxName)
    {
    }

    protected override InboxType Type => InboxType.Default;

    public DefaultInboxBuilder RegisterHandler<THandler, TMessage>()
        where THandler : class, IInboxHandler<TMessage>
        where TMessage : class
        => RegisterKeyedHandler<THandler, IInboxHandler<TMessage>, TMessage>();

    public DefaultInboxBuilder RegisterHandler<TMessage>(Func<IServiceProvider, IInboxHandler<TMessage>> handlerFactory)
        where TMessage : class
        => RegisterKeyedHandler<IInboxHandler<TMessage>, TMessage>(handlerFactory);

    public DefaultInboxBuilder RegisterHandler<TMessage>(IInboxHandler<TMessage> handler)
        where TMessage : class
        => RegisterKeyedHandler<IInboxHandler<TMessage>, TMessage>(handler);

    #region IDefaultInboxBuilder explicit implementation

    IDefaultInboxBuilder IInboxBuilderBase<IDefaultInboxBuilder>.UseStorageProviderFactory(IInboxStorageProviderFactory factory)
        => UseStorageProviderFactory(factory);

    IDefaultInboxBuilder IInboxBuilderBase<IDefaultInboxBuilder>.UseStorageProviderFactory<TFactory>()
        => UseStorageProviderFactory<TFactory>();

    IDefaultInboxBuilder IInboxBuilderBase<IDefaultInboxBuilder>.UseStorageProviderFactory(Func<IServiceProvider, IInboxStorageProviderFactory> factoryFunc)
        => UseStorageProviderFactory(factoryFunc);

    IDefaultInboxBuilder IInboxBuilderBase<IDefaultInboxBuilder>.UseSerializerFactory(IInboxSerializerFactory factory)
        => UseSerializerFactory(factory);

    IDefaultInboxBuilder IInboxBuilderBase<IDefaultInboxBuilder>.UseSerializerFactory<TFactory>()
        => UseSerializerFactory<TFactory>();

    IDefaultInboxBuilder IInboxBuilderBase<IDefaultInboxBuilder>.UseSerializerFactory<TFactory>(Func<IServiceProvider, TFactory> factoryFunc)
        => UseSerializerFactory(factoryFunc);

    IDefaultInboxBuilder IInboxBuilderBase<IDefaultInboxBuilder>.RegisterMessage<TMessage>(string? messageType)
        => RegisterMessage<TMessage>(messageType);

    IDefaultInboxBuilder IInboxBuilderBase<IDefaultInboxBuilder>.ConfigureHealthCheck(Action<IInboxHealthCheckOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return ConfigureHealthCheck(opts => configure(opts));
    }

    IDefaultInboxBuilder IInboxBuilderBase<IDefaultInboxBuilder>.ConfigureServices(Action<IServiceCollection> configure)
        => ConfigureServices(configure);

    IDefaultInboxBuilder IInboxBuilderBase<IDefaultInboxBuilder>.PostConfigure(Action<IInboxConfiguration, IServiceCollection> action)
        => PostConfigure(action);

    IDefaultInboxBuilder IInboxBuilderBase<IDefaultInboxBuilder>.ConfigureOptions(Action<IConfigureInboxOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return ConfigureOptions(opts => configure(opts));
    }

    IDefaultInboxBuilder IDefaultInboxBuilder.RegisterHandler<THandler, TMessage>()
        => RegisterHandler<THandler, TMessage>();

    IDefaultInboxBuilder IDefaultInboxBuilder.RegisterHandler<TMessage>(Func<IServiceProvider, IInboxHandler<TMessage>> handlerFactory)
        => RegisterHandler(handlerFactory);

    IDefaultInboxBuilder IDefaultInboxBuilder.RegisterHandler<TMessage>(IInboxHandler<TMessage> handler)
        => RegisterHandler(handler);

    #endregion
}