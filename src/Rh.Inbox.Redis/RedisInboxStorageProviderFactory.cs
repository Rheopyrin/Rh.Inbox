using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Redis.Provider;

namespace Rh.Inbox.Redis;

internal sealed class RedisInboxStorageProviderFactory(IServiceProvider serviceProvider)
    : IInboxStorageProviderFactory
{
    public IInboxStorageProvider Create(IInboxConfiguration options) =>
        options.InboxType switch
        {
            InboxType.Fifo or InboxType.FifoBatched =>
                ActivatorUtilities.CreateInstance<RedisFifoInboxStorageProvider>(serviceProvider, options),
            _ =>
                ActivatorUtilities.CreateInstance<RedisDefaultInboxStorageProvider>(serviceProvider, options)
        };
}