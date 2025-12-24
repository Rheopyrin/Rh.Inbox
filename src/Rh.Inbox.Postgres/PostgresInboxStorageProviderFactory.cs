using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Postgres.Provider;

namespace Rh.Inbox.Postgres;

internal sealed class PostgresInboxStorageProviderFactory(IServiceProvider serviceProvider)
    : IInboxStorageProviderFactory
{
    public IInboxStorageProvider Create(IInboxConfiguration configuration) =>
        configuration.InboxType switch
        {
            InboxType.Fifo or InboxType.FifoBatched =>
                ActivatorUtilities.CreateInstance<PostgresFifoInboxStorageProvider>(serviceProvider, configuration),
            _ =>
                ActivatorUtilities.CreateInstance<PostgresDefaultInboxStorageProvider>(serviceProvider, configuration)
        };
}