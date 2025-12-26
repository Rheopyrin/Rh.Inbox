using Npgsql;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Postgres.Options;
using Rh.Inbox.Postgres.Scripts;

namespace Rh.Inbox.Postgres.Provider;

/// <summary>
/// Postgres inbox storage provider for Fifo and FifoBatched inbox types.
/// Uses group locking to ensure FIFO ordering per group.
/// </summary>
internal sealed class PostgresFifoInboxStorageProvider : PostgresInboxStorageProviderBase, ISupportGroupLocksReleaseStorageProvider
{
    private readonly PostgresFifoSqlScripts _fifoSql;

    public PostgresFifoInboxStorageProvider(IInboxConfiguration configuration, IProviderOptionsAccessor optionsAccessor)
        : base(configuration, optionsAccessor, CreateSqlScripts(optionsAccessor.GetForInbox(configuration.InboxName)))
    {
        _fifoSql = (PostgresFifoSqlScripts)Sql;
    }

    private static PostgresFifoSqlScripts CreateSqlScripts(PostgresInboxProviderOptions postgresOptions) =>
        new(postgresOptions.TableName,
            postgresOptions.DeadLetterTableName,
            postgresOptions.DeduplicationTableName,
            postgresOptions.GroupLocksTableName);

    public override async Task<IReadOnlyList<InboxMessage>> ReadAndCaptureAsync(string processorId, CancellationToken token)
    {
        await using var connection = await PostgresOptions.DataSource.OpenConnectionAsync(token);

        var now = Configuration.DateTimeProvider.GetUtcNow();
        var maxProcessingTime = now - Configuration.Options.MaxProcessingTime;

        await using var cmd = new NpgsqlCommand(Sql.ReadAndCapture, connection);
        cmd.Parameters.AddWithValue("inboxName", Configuration.InboxName);
        cmd.Parameters.AddWithValue("maxProcessingTime", maxProcessingTime);
        cmd.Parameters.AddWithValue("batchSize", Configuration.Options.ReadBatchSize);
        cmd.Parameters.AddWithValue("now", now);
        cmd.Parameters.AddWithValue("processorId", processorId);

        var messages = new List<InboxMessage>();

        await using var reader = await cmd.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
        {
            messages.Add(ParseMessage(reader));
        }

        return messages;
    }

    /// <summary>
    /// Releases group locks after FIFO processing completes.
    /// This allows other workers to process messages from these groups.
    /// </summary>
    public async Task ReleaseGroupLocksAsync(IReadOnlyList<string> groupIds, CancellationToken token)
    {
        if (groupIds.Count == 0) return;

        await using var connection = await PostgresOptions.DataSource.OpenConnectionAsync(token);
        await using var cmd = new NpgsqlCommand(_fifoSql.ReleaseGroupLocks, connection);
        cmd.Parameters.AddWithValue("inboxName", Configuration.InboxName);
        cmd.Parameters.AddWithValue("groupIds", AsArray(groupIds));
        await cmd.ExecuteNonQueryAsync(token);
    }

    /// <summary>
    /// Releases captured messages and their group locks in a single connection.
    /// Uses a CTE to execute both operations atomically.
    /// </summary>
    public async Task ReleaseMessagesAndGroupLocksAsync(IReadOnlyList<IInboxMessageIdentifiers> messages, CancellationToken token)
    {
        if (messages.Count == 0)
        {
            return;
        }

        var messageIds = new Guid[messages.Count];
        var groupIdSet = new HashSet<string>();

        for (var i = 0; i < messages.Count; i++)
        {
            messageIds[i] = messages[i].Id;
            var groupId = messages[i].GroupId;

            if (!string.IsNullOrEmpty(groupId))
            {
                groupIdSet.Add(groupId);
            }
        }

        var hasGroups = groupIdSet.Count > 0;

        await using var connection = await PostgresOptions.DataSource.OpenConnectionAsync(token);

        if (!hasGroups)
        {
            await using var cmd = new NpgsqlCommand(Sql.ReleaseBatch, connection);
            cmd.Parameters.AddWithValue("ids", messageIds);
            await cmd.ExecuteNonQueryAsync(token);
            return;
        }

        var groupIds = new string[groupIdSet.Count];
        groupIdSet.CopyTo(groupIds);

        await using var combinedCmd = new NpgsqlCommand(_fifoSql.ReleaseMessagesAndGroupLocks, connection);
        combinedCmd.Parameters.AddWithValue("messageIds", messageIds);
        combinedCmd.Parameters.AddWithValue("inboxName", Configuration.InboxName);
        combinedCmd.Parameters.AddWithValue("groupIds", groupIds);
        await combinedCmd.ExecuteNonQueryAsync(token);
    }

    /// <summary>
    /// Extends message and group locks to prevent expiration during long-running processing.
    /// </summary>
    public override async Task<int> ExtendLocksAsync(
        string processorId,
        IReadOnlyList<IInboxMessageIdentifiers> capturedMessages,
        DateTime newCapturedAt,
        CancellationToken token)
    {
        if (capturedMessages.Count == 0)
        {
            return 0;
        }

        var messageIds = new Guid[capturedMessages.Count];
        var groupIdSet = new HashSet<string>();

        for (var i = 0; i < capturedMessages.Count; i++)
        {
            messageIds[i] = capturedMessages[i].Id;
            var groupId = capturedMessages[i].GroupId;

            if (!string.IsNullOrEmpty(groupId))
            {
                groupIdSet.Add(groupId);
            }
        }

        var hasGroups = groupIdSet.Count > 0;

        await using var connection = await PostgresOptions.DataSource.OpenConnectionAsync(token);

        if (!hasGroups)
        {
            await using var cmd = new NpgsqlCommand(Sql.ExtendMessageLocks, connection);
            cmd.Parameters.AddWithValue("messageIds", messageIds);
            cmd.Parameters.AddWithValue("processorId", processorId);
            cmd.Parameters.AddWithValue("newCapturedAt", newCapturedAt);
            return await cmd.ExecuteNonQueryAsync(token);
        }

        var groupIds = new string[groupIdSet.Count];
        groupIdSet.CopyTo(groupIds);

        await using var combinedCmd = new NpgsqlCommand(_fifoSql.ExtendMessagesAndGroupLocks, connection);
        combinedCmd.Parameters.AddWithValue("messageIds", messageIds);
        combinedCmd.Parameters.AddWithValue("processorId", processorId);
        combinedCmd.Parameters.AddWithValue("newCapturedAt", newCapturedAt);
        combinedCmd.Parameters.AddWithValue("inboxName", Configuration.InboxName);
        combinedCmd.Parameters.AddWithValue("groupIds", groupIds);

        var result = await combinedCmd.ExecuteScalarAsync(token);
        return result != null ? Convert.ToInt32(result, System.Globalization.CultureInfo.InvariantCulture) : 0;
    }

    public override async Task MigrateAsync(CancellationToken token)
    {
        await using var connection = await PostgresOptions.DataSource.OpenConnectionAsync(token);
        await using var transaction = await connection.BeginTransactionAsync(token);

        try
        {
            // Create shared tables (inbox, dead letter, deduplication)
            await MigrateBaseTablesAsync(connection, transaction, token);

            // FIFO-specific indexes on main table
            var createFifoIndexes = $@"
                -- Pending messages for FIFO: used by ReadAndCapture (FIFO) with group ordering
                CREATE INDEX IF NOT EXISTS ""idx_{PostgresOptions.TableName}_inbox_pending_fifo""
                    ON ""{PostgresOptions.TableName}"" (inbox_name, received_at, group_id) WHERE captured_at IS NULL;

                -- In-flight grouped messages: used by FIFO group locking
                CREATE INDEX IF NOT EXISTS ""idx_{PostgresOptions.TableName}_inflight""
                    ON ""{PostgresOptions.TableName}"" (inbox_name, group_id, captured_at) WHERE group_id IS NOT NULL;
            ";

            await using var indexCmd = new NpgsqlCommand(createFifoIndexes, connection, transaction);
            await indexCmd.ExecuteNonQueryAsync(token);

            // Group locks table for FIFO ordering
            var createGroupLocksTable = $@"
                CREATE TABLE IF NOT EXISTS ""{PostgresOptions.GroupLocksTableName}"" (
                    inbox_name TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    locked_at TIMESTAMP WITH TIME ZONE,
                    locked_by TEXT,
                    PRIMARY KEY (inbox_name, group_id)
                );

                CREATE INDEX IF NOT EXISTS ""idx_{PostgresOptions.GroupLocksTableName}_available""
                    ON ""{PostgresOptions.GroupLocksTableName}"" (inbox_name, locked_at)
                    WHERE locked_at IS NULL;
            ";

            await using var groupLocksCmd = new NpgsqlCommand(createGroupLocksTable, connection, transaction);
            await groupLocksCmd.ExecuteNonQueryAsync(token);

            await transaction.CommitAsync(token);
        }
        catch
        {
            await transaction.RollbackAsync(token);
            throw;
        }
    }
}