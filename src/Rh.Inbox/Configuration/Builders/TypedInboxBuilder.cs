using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;
using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions.Builders;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Configuration.Builders.Options;
using Rh.Inbox.Exceptions;
using Rh.Inbox.Health;
using Rh.Inbox.Serialization;

namespace Rh.Inbox.Configuration.Builders;

internal interface ITypedInboxBuilder
{
    BuiltInboxConfiguration Build();
}

internal abstract class TypedInboxBuilder<TBuilder> : ITypedInboxBuilder, IInboxBuilderBase<TBuilder>
    where TBuilder : TypedInboxBuilder<TBuilder>, IInboxBuilderBase<TBuilder>
{
    private readonly string _inboxName;
    private InboxHealthCheckOptions _healthCheckOptions = new();
    private Func<IServiceProvider, IInboxStorageProviderFactory>? _storageProviderFunc;
    private readonly ConfigureInboxOptions _options = new();
    private readonly List<Action<IInboxConfiguration, IServiceCollection>> _postConfigureActions = new();
    private readonly List<Action<IInboxMessageMetadataRegistry, IServiceCollection>> _registerMessagesActions = new();
    private readonly List<Action<IServiceCollection>> _configureServicesActions = new();
    private Func<IServiceProvider, IInboxSerializerFactory> _serializerFactoryFunc = _ => new SystemTextJsonInboxSerializerFactory();

    public TypedInboxBuilder(string inboxName)
    {
        _inboxName = inboxName;
    }

    protected abstract InboxType Type { get; }

    public string InboxName => _inboxName;

    public TBuilder UseStorageProviderFactory(IInboxStorageProviderFactory factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        _storageProviderFunc = _ => factory;
        return (TBuilder)this;
    }

    public TBuilder PostConfigure(Action<IInboxConfiguration, IServiceCollection> action)
    {
        _postConfigureActions.Add(action);
        return (TBuilder)this;
    }

    public TBuilder UseStorageProviderFactory<TFactory>()
        where TFactory : class, IInboxStorageProviderFactory
    {
        _storageProviderFunc = svc => svc.GetRequiredService<TFactory>();
        return (TBuilder)this;
    }

    public TBuilder UseStorageProviderFactory(Func<IServiceProvider, IInboxStorageProviderFactory> factoryFunc)
    {
        ArgumentNullException.ThrowIfNull(factoryFunc);
        _storageProviderFunc = factoryFunc;
        return (TBuilder)this;
    }

    public TBuilder ConfigureServices(Action<IServiceCollection> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        _configureServicesActions.Add(configure);
        return (TBuilder)this;
    }

    public TBuilder ConfigureOptions(Action<IConfigureInboxOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        configure(_options);

        return (TBuilder)this;
    }

    public TBuilder UseSerializerFactory(IInboxSerializerFactory factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        _serializerFactoryFunc = _ => factory;
        return (TBuilder)this;
    }

    public TBuilder UseSerializerFactory<TFactory>()
        where TFactory : class, IInboxSerializerFactory
    {
        _serializerFactoryFunc = (svc) => svc.GetRequiredService<TFactory>();
        return (TBuilder)this;
    }

    public TBuilder UseSerializerFactory<TFactory>(Func<IServiceProvider, TFactory> factoryFunc)
        where TFactory : class, IInboxSerializerFactory
    {
        _serializerFactoryFunc = factoryFunc;
        return (TBuilder)this;
    }

    public TBuilder RegisterMessage<TMessage>(string? messageType = null) where TMessage : class
    {
        _registerMessagesActions.Add((r, _) => r.Register<TMessage>(messageType));
        return (TBuilder)this;
    }

    BuiltInboxConfiguration ITypedInboxBuilder.Build()
    {
        if (_storageProviderFunc is null)
        {
            throw new InvalidInboxConfigurationException("StorageProviderFactory is not configured. Call UseStorageProviderFactory().");
        }

        if (_registerMessagesActions.Count == 0)
        {
            throw new InvalidInboxConfigurationException("No messages are registered. Call RegisterMessage() to register a message type and handler.");
        }

        if (_options.ReadBatchSize <= 0)
        {
            throw new InvalidInboxConfigurationException($"ReadBatchSize must be greater than 0, but was {_options.ReadBatchSize}.");
        }

        if (_options.WriteBatchSize <= 0)
        {
            throw new InvalidInboxConfigurationException($"WriteBatchSize must be greater than 0, but was {_options.WriteBatchSize}.");
        }

        if (_options.MaxProcessingTime <= TimeSpan.Zero)
        {
            throw new InvalidInboxConfigurationException($"MaxProcessingTime must be greater than 0, but was {_options.MaxProcessingTime}.");
        }

        if (_options.PollingInterval <= TimeSpan.Zero)
        {
            throw new InvalidInboxConfigurationException($"PollingInterval must be greater than 0, but was {_options.PollingInterval}.");
        }

        if (_options.ShutdownTimeout <= TimeSpan.Zero)
        {
            throw new InvalidInboxConfigurationException($"ShutdownTimeout must be greater than 0, but was {_options.ShutdownTimeout}.");
        }

        if (_options.MaxAttempts <= 0)
        {
            throw new InvalidInboxConfigurationException($"MaxAttempts must be greater than 0, but was {_options.MaxAttempts}.");
        }

        if (_options.DateTimeProvider is null)
        {
            throw new InvalidInboxConfigurationException("DateTimeProvider is null");
        }

        if (_options.MaxProcessingThreads <= 0)
        {
            throw new InvalidInboxConfigurationException($"MaxProcessingThreads must be greater than 0, but was {_options.MaxProcessingThreads}.");
        }

        if (_options.MaxWriteThreads <= 0)
        {
            throw new InvalidInboxConfigurationException($"MaxWriteThreads must be greater than 0, but was {_options.MaxWriteThreads}.");
        }

        if (_options.ReadDelay < TimeSpan.Zero)
        {
            throw new InvalidInboxConfigurationException($"ReadDelay cannot be negative, but was {_options.ReadDelay}.");
        }

        if (_options.EnableDeduplication && _options.DeduplicationInterval < TimeSpan.Zero)
        {
            throw new InvalidInboxConfigurationException($"DeduplicationInterval cannot be negative when deduplication is enabled, but was {_options.DeduplicationInterval}.");
        }

        if (_options.EnableDeadLetter && _options.DeadLetterMaxMessageLifetime < TimeSpan.Zero)
        {
            throw new InvalidInboxConfigurationException($"DeadLetterMaxMessageLifetime cannot be negative when dead letter is enabled, but was {_options.DeadLetterMaxMessageLifetime}.");
        }

        if (_options.EnableLockExtension && (_options.LockExtensionThreshold < 0.1 || _options.LockExtensionThreshold > 0.9))
        {
            throw new InvalidInboxConfigurationException($"LockExtensionThreshold must be between 0.1 and 0.9, but was {_options.LockExtensionThreshold}.");
        }

        return new BuiltInboxConfiguration
        {
            ConfigureServicesActions = _configureServicesActions.ToArray(),
            InboxOptions = _options,
            PostConfigureActions = _postConfigureActions.ToArray(),
            RegisterMessagesActions = _registerMessagesActions.ToArray(),
            StorageProviderFunc = _storageProviderFunc,
            HealthCheckOptions = _healthCheckOptions,
            SerializerFactoryFunc = _serializerFactoryFunc,
            Type = Type
        };
    }

    public TBuilder ConfigureHealthCheck(Action<IInboxHealthCheckOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var options = new InboxHealthCheckOptions();
        configure(options);

        _healthCheckOptions = options;
        return (TBuilder)this;
    }

    protected TBuilder RegisterKeyedHandler<THandler, THandlerInterface, TMessage>()
        where THandler : class, THandlerInterface
        where THandlerInterface : class
        where TMessage : class
    {
        _registerMessagesActions.Add((r, svc) =>
        {
            r.Register<TMessage>();
            svc.AddKeyedScoped<THandler>(_inboxName);
            svc.AddKeyedScoped<THandlerInterface>(_inboxName, (sp, key) => sp.GetRequiredKeyedService<THandler>(key));
        });


        return (TBuilder)this;
    }

    protected TBuilder RegisterKeyedHandler<THandlerInterface, TMessage>(
        Func<IServiceProvider, THandlerInterface> handlerFactory)
        where THandlerInterface : class
        where TMessage : class
    {
        _registerMessagesActions.Add((r, svc) =>
        {
            r.Register<TMessage>();
            svc.AddKeyedScoped(_inboxName, (sp, _) => handlerFactory(sp));
        });

        return (TBuilder)this;
    }

    protected TBuilder RegisterKeyedHandler<THandlerInterface, TMessage>(THandlerInterface handler)
        where THandlerInterface : class
        where TMessage : class
    {
        _registerMessagesActions.Add((r, svc) =>
        {
            r.Register<TMessage>();
            svc.AddKeyedScoped(_inboxName, (_, _) => handler);
        });

        return (TBuilder)this;
    }
}