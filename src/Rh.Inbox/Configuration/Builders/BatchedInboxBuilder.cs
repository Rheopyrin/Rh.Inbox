using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions.Builders;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;

namespace Rh.Inbox.Configuration.Builders;

internal sealed class BatchedInboxBuilder : TypedInboxBuilder<BatchedInboxBuilder>, IBatchedInboxBuilder
{
    internal BatchedInboxBuilder(string inboxName)
        : base(inboxName)
    {
    }

    protected override InboxType Type => InboxType.Batched;

    public BatchedInboxBuilder RegisterHandler<THandler, TMessage>()
        where THandler : class, IBatchedInboxHandler<TMessage>
        where TMessage : class
        => RegisterKeyedHandler<THandler, IBatchedInboxHandler<TMessage>, TMessage>();

    public BatchedInboxBuilder RegisterHandler<TMessage>(Func<IServiceProvider, IBatchedInboxHandler<TMessage>> handlerFactory)
        where TMessage : class
        => RegisterKeyedHandler<IBatchedInboxHandler<TMessage>, TMessage>(handlerFactory);

    public BatchedInboxBuilder RegisterHandler<TMessage>(IBatchedInboxHandler<TMessage> handler)
        where TMessage : class
        => RegisterKeyedHandler<IBatchedInboxHandler<TMessage>, TMessage>(handler);

    #region IBatchedInboxBuilder explicit implementation

    IBatchedInboxBuilder IInboxBuilderBase<IBatchedInboxBuilder>.UseStorageProviderFactory(IInboxStorageProviderFactory factory)
        => UseStorageProviderFactory(factory);

    IBatchedInboxBuilder IInboxBuilderBase<IBatchedInboxBuilder>.UseStorageProviderFactory<TFactory>()
        => UseStorageProviderFactory<TFactory>();

    IBatchedInboxBuilder IInboxBuilderBase<IBatchedInboxBuilder>.UseStorageProviderFactory(Func<IServiceProvider, IInboxStorageProviderFactory> factoryFunc)
        => UseStorageProviderFactory(factoryFunc);

    IBatchedInboxBuilder IInboxBuilderBase<IBatchedInboxBuilder>.UseSerializerFactory(IInboxSerializerFactory factory)
        => UseSerializerFactory(factory);

    IBatchedInboxBuilder IInboxBuilderBase<IBatchedInboxBuilder>.UseSerializerFactory<TFactory>()
        => UseSerializerFactory<TFactory>();

    IBatchedInboxBuilder IInboxBuilderBase<IBatchedInboxBuilder>.UseSerializerFactory<TFactory>(Func<IServiceProvider, TFactory> factoryFunc)
        => UseSerializerFactory(factoryFunc);

    IBatchedInboxBuilder IInboxBuilderBase<IBatchedInboxBuilder>.RegisterMessage<TMessage>(string? messageType)
        => RegisterMessage<TMessage>(messageType);

    IBatchedInboxBuilder IInboxBuilderBase<IBatchedInboxBuilder>.ConfigureHealthCheck(Action<IInboxHealthCheckOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return ConfigureHealthCheck(opts => configure(opts));
    }

    IBatchedInboxBuilder IInboxBuilderBase<IBatchedInboxBuilder>.ConfigureServices(Action<IServiceCollection> configure)
        => ConfigureServices(configure);

    IBatchedInboxBuilder IInboxBuilderBase<IBatchedInboxBuilder>.PostConfigure(Action<IInboxConfiguration, IServiceCollection> action)
        => PostConfigure(action);

    IBatchedInboxBuilder IInboxBuilderBase<IBatchedInboxBuilder>.ConfigureOptions(Action<IConfigureInboxOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return ConfigureOptions(opts => configure(opts));
    }

    IBatchedInboxBuilder IBatchedInboxBuilder.RegisterHandler<THandler, TMessage>()
        => RegisterHandler<THandler, TMessage>();

    IBatchedInboxBuilder IBatchedInboxBuilder.RegisterHandler<TMessage>(Func<IServiceProvider, IBatchedInboxHandler<TMessage>> handlerFactory)
        => RegisterHandler(handlerFactory);

    IBatchedInboxBuilder IBatchedInboxBuilder.RegisterHandler<TMessage>(IBatchedInboxHandler<TMessage> handler)
        => RegisterHandler(handler);

    #endregion
}