namespace Rh.Inbox.Postgres.Scripts;

/// <summary>
/// Base class for PostgreSQL inbox SQL scripts.
/// Contains shared SQL statements used by both Default and FIFO providers.
/// </summary>
internal abstract class PostgresSqlScriptsBase : IPostgresSqlScripts
{
    private readonly string _tableName;
    private readonly string _deadLetterTableName;

    // Write operations
    public string Insert { get; }
    public string InsertWithCollapse { get; }
    public string DeleteCollapsed { get; }

    // Lifecycle operations
    public string Complete { get; }
    public string CompleteBatch { get; }
    public string Fail { get; }
    public string FailBatch { get; }
    public string Release { get; }
    public string ReleaseBatch { get; }
    public string DeleteBatch { get; }

    // Dead letter operations
    public string ReadDeadLetters { get; }

    // Health check
    public string HealthMetricsWithDlq { get; }
    public string HealthMetricsWithoutDlq { get; }

    // Deduplication
    public string? TryInsertDeduplication { get; }
    public string? InsertDeduplicationBatch { get; }

    // Lock extension
    public string ExtendMessageLocks { get; }

    // Abstract - must be implemented by derived classes
    public abstract string ReadAndCapture { get; }

    protected PostgresSqlScriptsBase(string tableName, string deadLetterTableName, string deduplicationTableName)
    {
        _tableName = tableName;
        _deadLetterTableName = deadLetterTableName;

        // Deduplication scripts
        TryInsertDeduplication = $@"
            INSERT INTO ""{deduplicationTableName}"" (inbox_name, deduplication_id, created_at)
            VALUES (@inboxName, @deduplicationId, @createdAt)
            ON CONFLICT (inbox_name, deduplication_id) DO NOTHING
            RETURNING 1";

        InsertDeduplicationBatch = $@"
            INSERT INTO ""{deduplicationTableName}"" (inbox_name, deduplication_id, created_at)
            SELECT @inboxName, unnest(@deduplicationIds), @createdAt
            ON CONFLICT (inbox_name, deduplication_id) DO NOTHING
            RETURNING deduplication_id";

        // Write scripts
        Insert = $@"
            INSERT INTO ""{tableName}""
                (id, inbox_name, message_type, payload, group_id, collapse_key, deduplication_id, attempts_count, received_at)
            VALUES
                (@id, @inboxName, @messageType, @payload, @groupId, @collapseKey, @deduplicationId, @attemptsCount, @receivedAt)
            ON CONFLICT (inbox_name, deduplication_id) WHERE deduplication_id IS NOT NULL DO NOTHING";

        InsertWithCollapse = $@"
            WITH deleted AS (
                DELETE FROM ""{tableName}""
                WHERE inbox_name = @inboxName AND collapse_key = @collapseKey AND captured_at IS NULL
            )
            {Insert}";

        DeleteCollapsed = $@"
            DELETE FROM ""{tableName}""
            WHERE inbox_name = @inboxName AND collapse_key = ANY(@collapseKeys) AND captured_at IS NULL";

        // Lifecycle scripts
        Complete = $@"DELETE FROM ""{tableName}"" WHERE id = @id";

        CompleteBatch = $@"DELETE FROM ""{tableName}"" WHERE id = ANY(@ids)";

        Fail = $@"
            UPDATE ""{tableName}""
            SET attempts_count = attempts_count + 1, captured_at = NULL, captured_by = NULL
            WHERE id = @id";

        FailBatch = $@"
            UPDATE ""{tableName}""
            SET attempts_count = attempts_count + 1, captured_at = NULL, captured_by = NULL
            WHERE id = ANY(@ids)";

        Release = $@"
            UPDATE ""{tableName}""
            SET captured_at = NULL, captured_by = NULL
            WHERE id = @id";

        ReleaseBatch = $@"
            UPDATE ""{tableName}""
            SET captured_at = NULL, captured_by = NULL
            WHERE id = ANY(@ids)";

        DeleteBatch = $@"DELETE FROM ""{tableName}"" WHERE id = ANY(@ids)";

        // Dead letter scripts
        ReadDeadLetters = $@"
            SELECT id, inbox_name, message_type, payload, group_id, collapse_key,
                   attempts_count, received_at, failure_reason, moved_at
            FROM ""{deadLetterTableName}""
            WHERE inbox_name = @inboxName
            ORDER BY moved_at ASC
            LIMIT @count";

        // Health check scripts - optimized using FILTER clause for single table scan
        HealthMetricsWithDlq = $@"
            SELECT
                COUNT(*) FILTER (WHERE captured_at IS NULL) AS pending_count,
                COUNT(*) FILTER (WHERE captured_at IS NOT NULL) AS captured_count,
                (SELECT COUNT(*) FROM ""{deadLetterTableName}"" WHERE inbox_name = @inboxName) AS dead_letter_count,
                MIN(received_at) FILTER (WHERE captured_at IS NULL) AS oldest_pending_at
            FROM ""{tableName}""
            WHERE inbox_name = @inboxName";

        HealthMetricsWithoutDlq = $@"
            SELECT
                COUNT(*) FILTER (WHERE captured_at IS NULL) AS pending_count,
                COUNT(*) FILTER (WHERE captured_at IS NOT NULL) AS captured_count,
                0::bigint AS dead_letter_count,
                MIN(received_at) FILTER (WHERE captured_at IS NULL) AS oldest_pending_at
            FROM ""{tableName}""
            WHERE inbox_name = @inboxName";

        // Lock extension scripts
        ExtendMessageLocks = $@"
            UPDATE ""{tableName}""
            SET captured_at = @newCapturedAt
            WHERE id = ANY(@messageIds)
              AND captured_by = @processorId
              AND captured_at IS NOT NULL";
    }

    public string BuildInsertToDeadLetter(string valuesClause) => $@"
        INSERT INTO ""{_deadLetterTableName}""
            (id, inbox_name, message_type, payload, group_id, collapse_key, deduplication_id,
             attempts_count, received_at, failure_reason, moved_at)
        SELECT m.id, m.inbox_name, m.message_type, m.payload, m.group_id, m.collapse_key, m.deduplication_id,
               m.attempts_count, m.received_at, r.reason, @movedAt
        FROM ""{_tableName}"" m
        INNER JOIN (VALUES {valuesClause}) AS r(id, reason) ON m.id = r.id";

    public string BuildBatchInsert(string valuesClause) => $@"
        INSERT INTO ""{_tableName}""
            (id, inbox_name, message_type, payload, group_id, collapse_key, deduplication_id, attempts_count, received_at)
        VALUES
            {valuesClause}
        ON CONFLICT (inbox_name, deduplication_id) WHERE deduplication_id IS NOT NULL DO NOTHING";

    /// <summary>
    /// Builds SQL for batch cleanup of expired deduplication records.
    /// </summary>
    public static string BuildDeduplicationCleanup(string tableName) => $@"
        DELETE FROM ""{tableName}"" d
        USING (
            SELECT inbox_name, deduplication_id FROM ""{tableName}""
            WHERE created_at <= @expirationTime
            LIMIT @batchSize
        ) AS expired
        WHERE d.inbox_name = expired.inbox_name
          AND d.deduplication_id = expired.deduplication_id";

    /// <summary>
    /// Builds SQL for batch cleanup of expired group locks.
    /// </summary>
    public static string BuildGroupLocksCleanup(string tableName) => $@"
        DELETE FROM ""{tableName}"" g
        USING (
            SELECT inbox_name, group_id FROM ""{tableName}""
            WHERE locked_at <= @expirationTime
            LIMIT @batchSize
        ) AS expired
        WHERE g.inbox_name = expired.inbox_name
          AND g.group_id = expired.group_id";

    /// <summary>
    /// Builds SQL for batch cleanup of expired dead letter messages.
    /// </summary>
    public static string BuildDeadLetterCleanup(string tableName) => $@"
        DELETE FROM ""{tableName}"" d
        USING (
            SELECT id FROM ""{tableName}""
            WHERE moved_at <= @expirationTime
            LIMIT @batchSize
        ) AS expired
        WHERE d.id = expired.id";
}
