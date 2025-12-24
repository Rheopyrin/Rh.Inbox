using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Inboxes.Implementation;

namespace Rh.Inbox.Inboxes.Factory;

internal sealed class InboxFactory : IInboxFactory
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IDateTimeProvider _dateTimeProvider;

    public InboxFactory(IServiceProvider serviceProvider, IDateTimeProvider dateTimeProvider)
    {
        _serviceProvider = serviceProvider;
        _dateTimeProvider = dateTimeProvider;
    }

    public InboxBase Create(IInboxConfiguration configuration)
    {
        var storageProviderFactory = configuration.StorageProviderFactoryFunc(_serviceProvider);
        if (storageProviderFactory is null)
        {
            throw new InvalidOperationException(
                $"StorageProviderFactory is not configured for inbox '{configuration.InboxName}'. " +
                "Call UseStorageProviderFactory() during inbox configuration.");
        }

        var serializerFactory = configuration.SerializerFactoryFunc(_serviceProvider);
        var serializer = serializerFactory.Create(configuration.InboxName);
        var storageProvider = storageProviderFactory.Create(configuration);

        return configuration.InboxType switch
        {
            InboxType.Default => new DefaultInbox(configuration, storageProvider, serializer, _dateTimeProvider),
            InboxType.Batched => new BatchedInbox(configuration, storageProvider, serializer, _dateTimeProvider),
            InboxType.Fifo => new FifoInbox(configuration, storageProvider, serializer, _dateTimeProvider),
            InboxType.FifoBatched => new FifoBatchedInbox(configuration, storageProvider, serializer, _dateTimeProvider),
            _ => throw new InvalidOperationException($"Unknown inbox type: {configuration.InboxType}")
        };
    }
}