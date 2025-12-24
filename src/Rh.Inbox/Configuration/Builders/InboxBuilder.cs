using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions.Builders;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Exceptions;
using Rh.Inbox.Processing.Utility;

namespace Rh.Inbox.Configuration.Builders;

internal sealed class InboxBuilder : IInboxBuilder
{
    private readonly IServiceCollection _services;
    private readonly string _inboxName;
    private readonly InboxMessageMetadataRegistry _metadataRegistry = new ();
    private ITypedInboxBuilder? _builder;

    internal InboxBuilder(IServiceCollection services, string inboxName)
    {
        _services = services;
        _inboxName = inboxName;
    }

    public IDefaultInboxBuilder AsDefault()
    {
        var builder = new DefaultInboxBuilder(_inboxName);
        _builder = builder;
        return builder;
    }

    public IBatchedInboxBuilder AsBatched()
    {
        var builder = new BatchedInboxBuilder(_inboxName);
        _builder = builder;

        return builder;
    }

    public IFifoInboxBuilder AsFifo()
    {
        var builder = new FifoInboxBuilder(_inboxName);
        _builder = builder;

        return builder;
    }

    public IFifoBatchedInboxBuilder AsFifoBatched()
    {
        var builder = new FifoBatchedInboxBuilder(_inboxName);
        _builder = builder;

        return builder;
    }

    internal IInboxConfiguration Build()
    {
        if (_builder == null)
        {
            throw new InvalidInboxConfigurationException("No inbox type was selected. Call AsDefault(), AsBatched(), AsFifo() or AsFifoBatched().");
        }

        var configuration = _builder.Build();

        Array.ForEach(configuration.RegisterMessagesActions, action => action(_metadataRegistry, _services));
        Array.ForEach(configuration.ConfigureServicesActions, action => action(_services));

        var options = new InboxOptions
        {
            InboxName = _inboxName,
            EnableDeadLetter = configuration.InboxOptions.EnableDeadLetter,
            MaxAttempts = configuration.InboxOptions.MaxAttempts,
            ShutdownTimeout = configuration.InboxOptions.ShutdownTimeout,
            PollingInterval = configuration.InboxOptions.PollingInterval,
            MaxProcessingThreads = configuration.InboxOptions.MaxProcessingThreads,
            ReadBatchSize = configuration.InboxOptions.ReadBatchSize,
            WriteBatchSize = configuration.InboxOptions.WriteBatchSize,
            MaxProcessingTime = configuration.InboxOptions.MaxProcessingTime,
            MaxWriteThreads = configuration.InboxOptions.MaxWriteThreads,
            ReadDelay = configuration.InboxOptions.ReadDelay,
            DeduplicationInterval = configuration.InboxOptions.DeduplicationInterval,
            EnableLockExtension = configuration.InboxOptions.EnableLockExtension,
            LockExtensionThreshold = configuration.InboxOptions.LockExtensionThreshold,
            DeadLetterMaxMessageLifetime = configuration.InboxOptions.DeadLetterMaxMessageLifetime,
            EnableDeduplication = configuration.InboxOptions.EnableDeduplication,
            DateTimeProvider = configuration.InboxOptions.DateTimeProvider
        };

        var inboxConfiguration = new InboxConfiguration
        {
            InboxName = _inboxName,
            HealthCheckOptions = configuration.HealthCheckOptions,
            InboxType = configuration.Type,
            MetadataRegistry = _metadataRegistry,
            Options = options,
            SerializerFactoryFunc = configuration.SerializerFactoryFunc,
            StorageProviderFactoryFunc = configuration.StorageProviderFunc,
            DateTimeProvider = configuration.InboxOptions.DateTimeProvider
        };

        Array.ForEach(configuration.PostConfigureActions, action => action(inboxConfiguration, _services));
        return inboxConfiguration;
    }
}