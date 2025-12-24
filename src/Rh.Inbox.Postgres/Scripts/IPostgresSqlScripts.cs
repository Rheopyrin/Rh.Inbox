namespace Rh.Inbox.Postgres.Scripts;

/// <summary>
/// Interface for PostgreSQL inbox SQL scripts.
/// Defines common operations shared between Default and FIFO providers.
/// </summary>
internal interface IPostgresSqlScripts
{
    // Write operations
    string Insert { get; }
    string InsertWithCollapse { get; }
    string DeleteCollapsed { get; }

    // Lifecycle operations
    string Complete { get; }
    string CompleteBatch { get; }
    string Fail { get; }
    string FailBatch { get; }
    string Release { get; }
    string ReleaseBatch { get; }
    string DeleteBatch { get; }

    // Dead letter operations
    string ReadDeadLetters { get; }

    // Health check
    string HealthMetricsWithDlq { get; }
    string HealthMetricsWithoutDlq { get; }

    // Deduplication
    string? TryInsertDeduplication { get; }
    string? InsertDeduplicationBatch { get; }

    // Read and capture - implementation differs between Default and FIFO
    string ReadAndCapture { get; }

    // Lock extension
    string ExtendMessageLocks { get; }

    // Dynamic SQL builders
    string BuildInsertToDeadLetter(string valuesClause);
    string BuildBatchInsert(string valuesClause);
}
