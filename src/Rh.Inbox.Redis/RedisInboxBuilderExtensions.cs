using Microsoft.Extensions.DependencyInjection.Extensions;
using Rh.Inbox.Abstractions.Builders;
using Rh.Inbox.Exceptions;
using Rh.Inbox.Redis.Connection;
using Rh.Inbox.Redis.Options;
using Rh.Inbox.Redis.Utility;

namespace Rh.Inbox.Redis;

/// <summary>
/// Extension methods for configuring Redis inbox storage.
/// </summary>
public static class RedisInboxBuilderExtensions
{
    /// <summary>
    /// Configures the inbox to use Redis storage with custom options.
    /// </summary>
    /// <typeparam name="TBuilder">The builder type.</typeparam>
    /// <param name="builder">The inbox builder.</param>
    /// <param name="connectionString">The Redis connection string.</param>
    /// <param name="configure">Optional action to configure Redis-specific options.</param>
    /// <returns>The builder for chaining.</returns>
    public static TBuilder UseRedis<TBuilder>(
        this TBuilder builder,
        string connectionString,
        Action<RedisInboxOptions>? configure = null)
        where TBuilder : IInboxBuilderBase<TBuilder>
    {
        var options = new RedisInboxOptions { ConnectionString = connectionString };
        configure?.Invoke(options);

        ValidateRedisOptions(options);

        builder.ConfigureServices(services =>
        {
            services.TryAddSingleton<IRedisConnectionProvider, RedisConnectionProvider>();
            services.TryAddKeyedSingleton(builder.InboxName, options);
            services.TryAddSingleton<IProviderOptionsAccessor, ProviderOptionsAccessor>();
            services.TryAddSingleton<RedisInboxStorageProviderFactory>();
        });

        builder.UseStorageProviderFactory<RedisInboxStorageProviderFactory>();

        return builder;
    }

    private static void ValidateRedisOptions(RedisInboxOptions options)
    {
        var errors = new List<InboxOptionError>();

        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            errors.Add(new InboxOptionError(nameof(options.ConnectionString), "Connection string is required."));
        }

        if (options.KeyPrefix is not null && !RedisKeyHelper.IsValidKeyPrefix(options.KeyPrefix))
        {
            errors.Add(new InboxOptionError(nameof(options.KeyPrefix),
                $"'{options.KeyPrefix}' is not a valid Redis key prefix. " +
                "Must contain only alphanumeric characters, underscores, hyphens, or colons, " +
                $"and be at most {RedisKeyHelper.MaxKeyPrefixLength} characters."));
        }

        if (options.MaxMessageLifetime <= TimeSpan.Zero)
        {
            errors.Add(new InboxOptionError(nameof(options.MaxMessageLifetime), "Must be greater than zero."));
        }

        if (errors.Count > 0)
        {
            throw new InvalidInboxConfigurationException("Invalid Redis inbox configuration.", errors);
        }
    }
}