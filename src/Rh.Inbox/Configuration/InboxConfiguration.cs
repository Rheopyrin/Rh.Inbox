using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;

namespace Rh.Inbox.Configuration;

internal sealed class InboxConfiguration : IInboxConfiguration
{
    public required string InboxName { get; init; }

    public required InboxType InboxType { get; init; }

    public required IInboxOptions Options { get; init; }

    public required IInboxMessageMetadataRegistry MetadataRegistry { get; init; }

    public required Func<IServiceProvider, IInboxStorageProviderFactory> StorageProviderFactoryFunc { get; init; }

    public required Func<IServiceProvider, IInboxSerializerFactory> SerializerFactoryFunc { get; init; }

    public required IInboxHealthCheckOptions HealthCheckOptions { get; init; }

    public required IDateTimeProvider DateTimeProvider { get; init; }
}