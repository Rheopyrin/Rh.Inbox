namespace Rh.Inbox.Postgres.Scripts;

/// <summary>
/// SQL scripts for FIFO and FifoBatched inbox types.
/// Uses group locking to ensure FIFO ordering per group.
/// </summary>
internal sealed class PostgresFifoSqlScripts : PostgresSqlScriptsBase
{
    public override string ReadAndCapture { get; }

    /// <summary>
    /// Releases group locks after processing completes.
    /// </summary>
    public string ReleaseGroupLocks { get; }

    /// <summary>
    /// Releases messages and group locks in a single statement.
    /// </summary>
    public string ReleaseMessagesAndGroupLocks { get; }

    /// <summary>
    /// Extends message and group locks in a single statement.
    /// </summary>
    public string ExtendMessagesAndGroupLocks { get; }

    public PostgresFifoSqlScripts(
        string tableName,
        string deadLetterTableName,
        string deduplicationTableName,
        string groupLocksTableName)
        : base(tableName, deadLetterTableName, deduplicationTableName)
    {
        // Locking - optimized two-phase approach:
        // 1. Find pending messages from unlocked groups (LEFT JOIN check)
        // 2. Lock groups and capture messages atomically
        ReadAndCapture = $@"
            WITH available AS (
                -- Find pending messages from groups without active locks
                SELECT m.id, m.group_id
                FROM ""{tableName}"" m
                LEFT JOIN ""{groupLocksTableName}"" gl
                    ON gl.inbox_name = @inboxName AND gl.group_id = m.group_id
                WHERE m.inbox_name = @inboxName
                  AND m.group_id IS NOT NULL
                  AND (m.captured_at IS NULL OR m.captured_at <= @maxProcessingTime)
                  AND (gl.group_id IS NULL OR gl.locked_at <= @maxProcessingTime)
                ORDER BY m.received_at, m.group_id, m.id
                LIMIT @batchSize
                FOR UPDATE OF m SKIP LOCKED
            ),
            locked AS (
                -- Lock groups (upsert only if lock is available or expired)
                INSERT INTO ""{groupLocksTableName}"" (inbox_name, group_id, locked_at, locked_by)
                SELECT @inboxName, group_id, @now, @processorId
                FROM available
                GROUP BY group_id
                ON CONFLICT (inbox_name, group_id)
                DO UPDATE SET locked_at = @now, locked_by = @processorId
                WHERE ""{groupLocksTableName}"".locked_at <= @maxProcessingTime
                RETURNING group_id
            )
            UPDATE ""{tableName}"" t
            SET captured_at = @now, captured_by = @processorId
            FROM available a
            JOIN locked l ON a.group_id = l.group_id
            WHERE t.id = a.id
            RETURNING t.id, t.inbox_name, t.message_type, t.payload, t.group_id,
                      t.collapse_key, t.attempts_count, t.received_at, t.captured_at, t.captured_by";

        // Release group locks after processing completes
        ReleaseGroupLocks = $@"
            DELETE FROM ""{groupLocksTableName}""
            WHERE inbox_name = @inboxName AND group_id = ANY(@groupIds)";

        // Release messages and group locks in one statement using CTEs
        ReleaseMessagesAndGroupLocks = $@"
            WITH released_messages AS (
                UPDATE ""{tableName}""
                SET captured_at = NULL, captured_by = NULL
                WHERE id = ANY(@messageIds)
            )
            DELETE FROM ""{groupLocksTableName}""
            WHERE inbox_name = @inboxName AND group_id = ANY(@groupIds)";

        // Extend message and group locks in one statement using CTEs
        ExtendMessagesAndGroupLocks = $@"
            WITH extended_messages AS (
                UPDATE ""{tableName}""
                SET captured_at = @newCapturedAt
                WHERE id = ANY(@messageIds)
                  AND captured_by = @processorId
                  AND captured_at IS NOT NULL
                RETURNING 1
            ),
            extended_groups AS (
                UPDATE ""{groupLocksTableName}""
                SET locked_at = @newCapturedAt
                WHERE inbox_name = @inboxName
                  AND group_id = ANY(@groupIds)
                  AND locked_by = @processorId
            )
            SELECT COUNT(*) FROM extended_messages";
    }
}