using StackExchange.Redis;

namespace Rh.Inbox.Redis.Connection;

/// <summary>
/// Provides shared Redis connections by connection string.
/// Same connection string returns the same ConnectionMultiplexer instance.
/// </summary>
internal interface IRedisConnectionProvider : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Gets or creates a ConnectionMultiplexer for the specified connection string.
    /// </summary>
    ValueTask<IConnectionMultiplexer> GetConnectionAsync(string connectionString, CancellationToken cancellationToken = default);
}