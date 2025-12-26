using Rh.Inbox.Redis.Connection;
using Rh.Inbox.Resilience;

namespace Rh.Inbox.Redis.Options;

/// <summary>
/// Resolved options passed to the storage provider. All values are non-null.
/// </summary>
internal sealed class RedisInboxProviderOptions
{
    internal required IRedisConnectionProvider ConnectionProvider { get; init; }

    internal required string ConnectionString { get; init; }

    internal required string KeyPrefix { get; init; }

    internal required TimeSpan MaxMessageLifetime { get; init; }

    /// <summary>
    /// Gets or sets the retry options for transient storage failures.
    /// Default enables 3 retries with exponential backoff.
    /// </summary>
    internal RetryOptions Retry { get; init; } = RetryOptions.Default;
}