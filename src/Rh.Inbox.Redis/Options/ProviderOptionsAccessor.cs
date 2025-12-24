using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Redis.Connection;
using Rh.Inbox.Redis.Utility;

namespace Rh.Inbox.Redis.Options;

internal interface IProviderOptionsAccessor
{
    RedisInboxProviderOptions GetForInbox(string inboxName);
}

internal sealed class ProviderOptionsAccessor : IProviderOptionsAccessor
{
    private readonly IRedisConnectionProvider _connectionProvider;
    private readonly IServiceProvider _serviceProvider;

    public ProviderOptionsAccessor(IRedisConnectionProvider connectionProvider, IServiceProvider serviceProvider)
    {
        _connectionProvider = connectionProvider;
        _serviceProvider = serviceProvider;
    }

    public RedisInboxProviderOptions GetForInbox(string inboxName)
    {
        var options = _serviceProvider.GetRequiredKeyedService<RedisInboxOptions>(inboxName);

        return new RedisInboxProviderOptions
        {
            ConnectionProvider = _connectionProvider,
            ConnectionString = options.ConnectionString,
            KeyPrefix = options.KeyPrefix ?? RedisKeyHelper.BuildKeyPrefix(RedisInboxOptions.DefaultKeyPrefix, inboxName),
            MaxMessageLifetime = options.MaxMessageLifetime
        };
    }
}