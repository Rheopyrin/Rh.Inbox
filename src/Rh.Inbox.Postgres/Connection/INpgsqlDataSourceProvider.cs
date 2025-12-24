using Npgsql;

namespace Rh.Inbox.Postgres.Connection;

/// <summary>
/// Provides shared NpgsqlDataSource instances by connection string.
/// Same connection string returns the same NpgsqlDataSource instance.
/// </summary>
internal interface INpgsqlDataSourceProvider : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Gets or creates a NpgsqlDataSource for the specified connection string.
    /// </summary>
    NpgsqlDataSource GetDataSource(string connectionString);
}