using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Health;

namespace Rh.Inbox.Configuration.Builders.Options;

internal class BuiltInboxConfiguration
{
    public required InboxType Type {get; init;}

    public required ConfigureInboxOptions InboxOptions { get; init; }

    public IInboxHealthCheckOptions HealthCheckOptions { get; init; } = new InboxHealthCheckOptions();

    public required Action<IInboxConfiguration, IServiceCollection>[] PostConfigureActions { get; init; }

    public required Action<IInboxMessageMetadataRegistry, IServiceCollection>[] RegisterMessagesActions { get; init; }

    public required Func<IServiceProvider, IInboxStorageProviderFactory> StorageProviderFunc { get; init; }

    public required Action<IServiceCollection>[] ConfigureServicesActions { get; init; }

    public required Func<IServiceProvider, IInboxSerializerFactory> SerializerFactoryFunc { get; init; }
}