using System.Collections.Concurrent;
using Npgsql;

namespace Rh.Inbox.Postgres.Connection;

/// <summary>
/// Thread-safe NpgsqlDataSource provider that shares data source instances
/// by connection string. Data sources are created lazily and cached.
/// </summary>
internal sealed class NpgsqlDataSourceProvider : INpgsqlDataSourceProvider
{
    private readonly ConcurrentDictionary<string, Lazy<NpgsqlDataSource>> _dataSources = new();
    private bool _disposed;

    /// <inheritdoc />
    public NpgsqlDataSource GetDataSource(string connectionString)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var lazyDataSource = _dataSources.GetOrAdd(
            connectionString,
            cs => new Lazy<NpgsqlDataSource>(
                () => NpgsqlDataSource.Create(cs),
                LazyThreadSafetyMode.ExecutionAndPublication));

        return lazyDataSource.Value;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        var disposeTasks = new List<Task>();

        foreach (var kvp in _dataSources)
        {
            if (kvp.Value.IsValueCreated)
            {
                disposeTasks.Add(kvp.Value.Value.DisposeAsync().AsTask());
            }
        }

        await Task.WhenAll(disposeTasks).ConfigureAwait(false);
        _dataSources.Clear();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        foreach (var kvp in _dataSources)
        {
            if (kvp.Value.IsValueCreated)
            {
                try
                {
                    kvp.Value.Value.Dispose();
                }
                catch
                {
                    // Ignore disposal errors
                }
            }
        }

        _dataSources.Clear();
    }
}