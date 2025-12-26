using Npgsql;
using Rh.Inbox.Resilience;

namespace Rh.Inbox.Postgres.Options;

/// <summary>
/// Resolved options passed to the storage provider. All values are non-null.
/// </summary>
internal sealed class PostgresInboxProviderOptions
{
    public required NpgsqlDataSource DataSource { get; init; }

    public required string TableName { get; init; }

    public required string DeadLetterTableName { get; init; }

    public required string DeduplicationTableName { get; init; }

    public required string GroupLocksTableName { get; init; }

    /// <summary>
    /// Gets or sets the retry options for transient storage failures.
    /// Default enables 3 retries with exponential backoff.
    /// </summary>
    public RetryOptions Retry { get; init; } = RetryOptions.Default;
}