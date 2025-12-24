using System.Collections.Concurrent;
using StackExchange.Redis;

namespace Rh.Inbox.Redis.Connection;

/// <summary>
/// Thread-safe Redis connection provider that shares ConnectionMultiplexer instances
/// by connection string. Connections are created lazily and cached.
/// </summary>
/// <remarks>
/// Connection resilience is handled automatically by StackExchange.Redis:
/// <list type="bullet">
///   <item><description>AbortOnConnectFail = false - allows operations to queue during reconnection</description></item>
///   <item><description>ConnectRetry = 3 - initial connection retries</description></item>
///   <item><description>ReconnectRetryPolicy = ExponentialRetry(5000) - automatic reconnection with backoff</description></item>
/// </list>
/// No manual reconnection logic is needed; the library handles transient failures internally.
/// </remarks>
internal sealed class RedisConnectionProvider : IRedisConnectionProvider
{
    private readonly ConcurrentDictionary<string, Lazy<Task<IConnectionMultiplexer>>> _connections = new();
    private bool _disposed;

    /// <inheritdoc />
    public async ValueTask<IConnectionMultiplexer> GetConnectionAsync(string connectionString, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var lazyConnection = _connections.GetOrAdd(
            connectionString,
            cs => CreateLazyConnection(cs));

        try
        {
            return await lazyConnection.Value.ConfigureAwait(false);
        }
        catch
        {
            _connections.TryRemove(connectionString, out _);
            throw;
        }
    }

    private static Lazy<Task<IConnectionMultiplexer>> CreateLazyConnection(string connectionString)
    {
        return new Lazy<Task<IConnectionMultiplexer>>(
            () => CreateConnectionAsync(connectionString),
            LazyThreadSafetyMode.ExecutionAndPublication);
    }

    private static async Task<IConnectionMultiplexer> CreateConnectionAsync(string connectionString)
    {
        var options = ConfigurationOptions.Parse(connectionString);
        options.AbortOnConnectFail = false;
        options.ConnectRetry = 3;
        options.ReconnectRetryPolicy = new ExponentialRetry(5000);

        return await ConnectionMultiplexer.ConnectAsync(options).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        var disposeTasks = new List<Task>();

        foreach (var kvp in _connections)
        {
            if (kvp.Value.IsValueCreated)
            {
                try
                {
                    var connection = await kvp.Value.Value.ConfigureAwait(false);
                    disposeTasks.Add(connection.DisposeAsync().AsTask());
                }
                catch
                {
                    // Connection failed to create, nothing to dispose
                }
            }
        }

        await Task.WhenAll(disposeTasks).ConfigureAwait(false);
        _connections.Clear();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        foreach (var kvp in _connections)
        {
            if (kvp.Value.IsValueCreated && kvp.Value.Value.IsCompletedSuccessfully)
            {
                try
                {
                    kvp.Value.Value.Result.Dispose();
                }
                catch
                {
                    // Ignore disposal errors
                }
            }
        }

        _connections.Clear();
    }
}