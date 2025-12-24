namespace Rh.Inbox.Postgres.Scripts;

/// <summary>
/// SQL scripts for Default and Batched inbox types.
/// Uses simple read and capture without group locking.
/// </summary>
internal sealed class PostgresDefaultSqlScripts : PostgresSqlScriptsBase
{
    public override string ReadAndCapture { get; }

    public PostgresDefaultSqlScripts(string tableName, string deadLetterTableName, string deduplicationTableName)
        : base(tableName, deadLetterTableName, deduplicationTableName)
    {
        ReadAndCapture = $@"
            WITH to_capture AS (
                SELECT id
                FROM ""{tableName}""
                WHERE inbox_name = @inboxName
                  AND (captured_at IS NULL OR captured_at <= @maxProcessingTime)
                ORDER BY received_at ASC
                LIMIT @batchSize
                FOR UPDATE SKIP LOCKED
            )
            UPDATE ""{tableName}"" m
            SET captured_at = @now, captured_by = @processorId
            FROM to_capture tc
            WHERE m.id = tc.id
            RETURNING m.id, m.inbox_name, m.message_type, m.payload, m.group_id,
                      m.collapse_key, m.attempts_count, m.received_at, m.captured_at, m.captured_by";
    }
}