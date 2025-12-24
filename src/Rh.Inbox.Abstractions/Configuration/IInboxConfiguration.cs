using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;

namespace Rh.Inbox.Abstractions.Configuration;

public interface IInboxConfiguration
{
    string InboxName { get; }

    InboxType InboxType { get; }

    IInboxOptions Options { get; }

    IInboxMessageMetadataRegistry MetadataRegistry { get; }

    Func<IServiceProvider, IInboxStorageProviderFactory> StorageProviderFactoryFunc { get; }

    Func<IServiceProvider, IInboxSerializerFactory> SerializerFactoryFunc { get; }

    IInboxHealthCheckOptions HealthCheckOptions { get; }

    IDateTimeProvider DateTimeProvider { get; }
}