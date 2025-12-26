using Microsoft.Extensions.Logging;
using Npgsql;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Health;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Postgres.Options;
using Rh.Inbox.Postgres.Resilience;
using Rh.Inbox.Postgres.Scripts;
using Rh.Inbox.Resilience;

namespace Rh.Inbox.Postgres.Provider;

internal abstract class PostgresInboxStorageProviderBase : ISupportMigration, ISupportHealthCheck
{
    protected readonly PostgresInboxProviderOptions PostgresOptions;
    protected readonly IInboxConfiguration Configuration;
    protected readonly IPostgresSqlScripts Sql;
    protected readonly RetryExecutor RetryExecutor;

    protected PostgresInboxStorageProviderBase(
        IInboxConfiguration configuration,
        IProviderOptionsAccessor optionsAccessor,
        IPostgresSqlScripts sql,
        ILogger logger)
    {
        PostgresOptions = optionsAccessor.GetForInbox(configuration.InboxName);
        Configuration = configuration;
        Sql = sql;
        RetryExecutor = new RetryExecutor(
            PostgresOptions.Retry,
            new PostgresTransientExceptionClassifier(),
            logger);
    }

    protected bool IsDeduplicationEnabled => Configuration.Options.EnableDeduplication;

    protected static T[] AsArray<T>(IReadOnlyList<T> list) =>
        list as T[] ?? list.ToArray();

    #region Abstract Methods

    public abstract Task<IReadOnlyList<InboxMessage>> ReadAndCaptureAsync(string processorId, CancellationToken token);

    public abstract Task MigrateAsync(CancellationToken token);

    #endregion

    #region Lock Extension

    /// <summary>
    /// Extends the locks for captured messages.
    /// Base implementation extends message capture locks only.
    /// FIFO providers override to also extend group locks.
    /// </summary>
    public virtual async Task<int> ExtendLocksAsync(
        string processorId,
        IReadOnlyList<IInboxMessageIdentifiers> capturedMessages,
        DateTime newCapturedAt,
        CancellationToken token)
    {
        if (capturedMessages.Count == 0)
        {
            return 0;
        }

        return await RetryExecutor.ExecuteAsync(async ct =>
        {
            await using var connection = await PostgresOptions.DataSource.OpenConnectionAsync(ct);
            await using var cmd = new NpgsqlCommand(Sql.ExtendMessageLocks, connection);
            cmd.Parameters.AddWithValue("messageIds", capturedMessages.Select(m => m.Id).ToArray());
            cmd.Parameters.AddWithValue("processorId", processorId);
            cmd.Parameters.AddWithValue("newCapturedAt", newCapturedAt);

            return await cmd.ExecuteNonQueryAsync(ct);
        }, token);
    }

    #endregion

    #region Write Operations

    public async Task WriteAsync(InboxMessage message, CancellationToken token)
    {
        await RetryExecutor.ExecuteAsync(async ct =>
        {
            await using var connection = await PostgresOptions.DataSource.OpenConnectionAsync(ct);

            if (IsDeduplicationEnabled && !string.IsNullOrEmpty(message.DeduplicationId))
            {
                var isDuplicate = await TryInsertDeduplicationRecordAsync(connection, null, message.DeduplicationId, ct);
                if (isDuplicate)
                {
                    return;
                }
            }

            var sql = !string.IsNullOrEmpty(message.CollapseKey)
                ? Sql.InsertWithCollapse
                : Sql.Insert;

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("id", message.Id);
            cmd.Parameters.AddWithValue("inboxName", Configuration.InboxName);
            cmd.Parameters.AddWithValue("messageType", message.MessageType);
            cmd.Parameters.AddWithValue("payload", message.Payload);
            cmd.Parameters.AddWithValue("groupId", (object?)message.GroupId ?? DBNull.Value);
            cmd.Parameters.AddWithValue("collapseKey", (object?)message.CollapseKey ?? DBNull.Value);
            cmd.Parameters.AddWithValue("deduplicationId", (object?)message.DeduplicationId ?? DBNull.Value);
            cmd.Parameters.AddWithValue("attemptsCount", message.AttemptsCount);
            cmd.Parameters.AddWithValue("receivedAt", message.ReceivedAt);

            await cmd.ExecuteNonQueryAsync(ct);
        }, token);
    }

    private async Task<bool> TryInsertDeduplicationRecordAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        string deduplicationId,
        CancellationToken token)
    {
        var now = Configuration.DateTimeProvider.GetUtcNow();

        await using var cmd = transaction is not null
            ? new NpgsqlCommand(Sql.TryInsertDeduplication, connection, transaction)
            : new NpgsqlCommand(Sql.TryInsertDeduplication, connection);

        cmd.Parameters.AddWithValue("inboxName", Configuration.InboxName);
        cmd.Parameters.AddWithValue("deduplicationId", deduplicationId);
        cmd.Parameters.AddWithValue("createdAt", now);

        var result = await cmd.ExecuteScalarAsync(token);
        return result is null;
    }

    public async Task WriteBatchAsync(IEnumerable<InboxMessage> messages, CancellationToken token)
    {
        var messageList = messages as InboxMessage[] ?? messages.ToArray();

        if (messageList.Length == 0)
        {
            return;
        }

        await RetryExecutor.ExecuteAsync(async ct =>
        {
            await using var connection = await PostgresOptions.DataSource.OpenConnectionAsync(ct);
            await using var transaction = await connection.BeginTransactionAsync(ct);

            try
            {
                var filteredBatch = IsDeduplicationEnabled
                    ? await FilterDuplicatesAsync(connection, transaction, messageList, ct)
                    : messageList;

                if (filteredBatch.Length > 0)
                {
                    await DeleteCollapsedMessagesAsync(connection, transaction, filteredBatch, ct);
                    await InsertBatchAsync(connection, transaction, filteredBatch, ct);
                }

                await transaction.CommitAsync(ct);
            }
            catch
            {
                await transaction.RollbackAsync(ct);
                throw;
            }
        }, token);
    }

    private async Task<InboxMessage[]> FilterDuplicatesAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        InboxMessage[] batch,
        CancellationToken token)
    {
        var deduplicationIds = batch
            .Where(m => !string.IsNullOrEmpty(m.DeduplicationId))
            .Select(m => m.DeduplicationId!)
            .Distinct()
            .ToArray();

        if (deduplicationIds.Length == 0)
        {
            return batch;
        }

        var insertedIds = await InsertDeduplicationRecordsAsync(connection, transaction, deduplicationIds, token);

        return batch
            .Where(m => string.IsNullOrEmpty(m.DeduplicationId) || insertedIds.Contains(m.DeduplicationId))
            .ToArray();
    }

    private async Task<HashSet<string>> InsertDeduplicationRecordsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        string[] deduplicationIds,
        CancellationToken token)
    {
        var now = Configuration.DateTimeProvider.GetUtcNow();

        await using var cmd = transaction is not null
            ? new NpgsqlCommand(Sql.InsertDeduplicationBatch, connection, transaction)
            : new NpgsqlCommand(Sql.InsertDeduplicationBatch, connection);

        cmd.Parameters.AddWithValue("inboxName", Configuration.InboxName);
        cmd.Parameters.AddWithValue("deduplicationIds", deduplicationIds);
        cmd.Parameters.AddWithValue("createdAt", now);

        var insertedIds = new HashSet<string>();
        await using var reader = await cmd.ExecuteReaderAsync(token);

        while (await reader.ReadAsync(token))
        {
            insertedIds.Add(reader.GetString(0));
        }

        return insertedIds;
    }

    private async Task InsertBatchAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        InboxMessage[] batch,
        CancellationToken token)
    {
        const int parametersPerRow = 9;
        var parameters = new List<NpgsqlParameter>(batch.Length * parametersPerRow);
        var valuesClauses = new List<string>(batch.Length);

        for (var i = 0; i < batch.Length; i++)
        {
            valuesClauses.Add($"(@id{i}, @inboxName{i}, @messageType{i}, @payload{i}, @groupId{i}, @collapseKey{i}, @deduplicationId{i}, @attemptsCount{i}, @receivedAt{i})");

            parameters.Add(new NpgsqlParameter($"id{i}", batch[i].Id));
            parameters.Add(new NpgsqlParameter($"inboxName{i}", Configuration.InboxName));
            parameters.Add(new NpgsqlParameter($"messageType{i}", batch[i].MessageType));
            parameters.Add(new NpgsqlParameter($"payload{i}", batch[i].Payload));
            parameters.Add(new NpgsqlParameter($"groupId{i}", (object?)batch[i].GroupId ?? DBNull.Value));
            parameters.Add(new NpgsqlParameter($"collapseKey{i}", (object?)batch[i].CollapseKey ?? DBNull.Value));
            parameters.Add(new NpgsqlParameter($"deduplicationId{i}", (object?)batch[i].DeduplicationId ?? DBNull.Value));
            parameters.Add(new NpgsqlParameter($"attemptsCount{i}", batch[i].AttemptsCount));
            parameters.Add(new NpgsqlParameter($"receivedAt{i}", batch[i].ReceivedAt));
        }

        var sql = Sql.BuildBatchInsert(string.Join(", ", valuesClauses));

        await using var cmd = new NpgsqlCommand(sql, connection, transaction);
        cmd.Parameters.AddRange(parameters.ToArray());
        await cmd.ExecuteNonQueryAsync(token);
    }

    private async Task DeleteCollapsedMessagesAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        InboxMessage[] messages,
        CancellationToken token)
    {
        var collapseKeys = messages
            .Where(m => !string.IsNullOrEmpty(m.CollapseKey))
            .Select(m => m.CollapseKey!)
            .Distinct()
            .ToArray();

        if (collapseKeys.Length == 0)
        {
            return;
        }

        await using var cmd = new NpgsqlCommand(Sql.DeleteCollapsed, connection, transaction);
        cmd.Parameters.AddWithValue("inboxName", Configuration.InboxName);
        cmd.Parameters.AddWithValue("collapseKeys", collapseKeys);
        await cmd.ExecuteNonQueryAsync(token);
    }

    #endregion

    #region Lifecycle Operations

    public async Task FailAsync(Guid messageId, CancellationToken token)
    {
        await RetryExecutor.ExecuteAsync(async ct =>
        {
            await using var connection = await PostgresOptions.DataSource.OpenConnectionAsync(ct);
            await using var cmd = new NpgsqlCommand(Sql.Fail, connection);
            cmd.Parameters.AddWithValue("id", messageId);
            await cmd.ExecuteNonQueryAsync(ct);
        }, token);
    }

    public async Task FailBatchAsync(IReadOnlyList<Guid> messageIds, CancellationToken token)
    {
        if (messageIds.Count == 0) return;

        await RetryExecutor.ExecuteAsync(async ct =>
        {
            await using var connection = await PostgresOptions.DataSource.OpenConnectionAsync(ct);
            await using var cmd = new NpgsqlCommand(Sql.FailBatch, connection);
            cmd.Parameters.AddWithValue("ids", AsArray(messageIds));
            await cmd.ExecuteNonQueryAsync(ct);
        }, token);
    }

    public async Task MoveToDeadLetterAsync(Guid messageId, string reason, CancellationToken token)
    {
        await MoveToDeadLetterBatchAsync([(messageId, reason)], token);
    }

    public async Task MoveToDeadLetterBatchAsync(IReadOnlyList<(Guid MessageId, string Reason)> messages, CancellationToken token)
    {
        if (messages.Count == 0) return;

        await RetryExecutor.ExecuteAsync(async ct =>
        {
            await using var connection = await PostgresOptions.DataSource.OpenConnectionAsync(ct);
            await using var transaction = await connection.BeginTransactionAsync(ct);

            try
            {
                var messageIds = messages.Select(m => m.MessageId).ToArray();

                if (Configuration.Options.EnableDeadLetter)
                {
                    await InsertToDeadLetterAsync(connection, transaction, messages, ct);
                }

                await using var deleteCmd = new NpgsqlCommand(Sql.DeleteBatch, connection, transaction);
                deleteCmd.Parameters.AddWithValue("ids", messageIds);
                await deleteCmd.ExecuteNonQueryAsync(ct);

                await transaction.CommitAsync(ct);
            }
            catch
            {
                await transaction.RollbackAsync(ct);
                throw;
            }
        }, token);
    }

    private async Task InsertToDeadLetterAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        IReadOnlyList<(Guid MessageId, string Reason)> messages,
        CancellationToken token)
    {
        const int parametersPerRow = 2;
        var movedAt = Configuration.DateTimeProvider.GetUtcNow();
        var parameters = new List<NpgsqlParameter>(messages.Count * parametersPerRow);
        var valuesClauses = new List<string>(messages.Count);

        for (var i = 0; i < messages.Count; i++)
        {
            valuesClauses.Add($"(@id{i}::uuid, @reason{i})");
            parameters.Add(new NpgsqlParameter($"id{i}", messages[i].MessageId));
            parameters.Add(new NpgsqlParameter($"reason{i}", messages[i].Reason));
        }

        var sql = Sql.BuildInsertToDeadLetter(string.Join(", ", valuesClauses));

        await using var cmd = new NpgsqlCommand(sql, connection, transaction);
        cmd.Parameters.AddRange(parameters.ToArray());
        cmd.Parameters.AddWithValue("movedAt", movedAt);
        await cmd.ExecuteNonQueryAsync(token);
    }

    public async Task ReleaseBatchAsync(IReadOnlyList<Guid> messageIds, CancellationToken token)
    {
        if (messageIds.Count == 0) return;

        await RetryExecutor.ExecuteAsync(async ct =>
        {
            await using var connection = await PostgresOptions.DataSource.OpenConnectionAsync(ct);
            await using var cmd = new NpgsqlCommand(Sql.ReleaseBatch, connection);
            cmd.Parameters.AddWithValue("ids", AsArray(messageIds));
            await cmd.ExecuteNonQueryAsync(ct);
        }, token);
    }

    public async Task ProcessResultsBatchAsync(
        IReadOnlyList<Guid> toComplete,
        IReadOnlyList<Guid> toFail,
        IReadOnlyList<Guid> toRelease,
        IReadOnlyList<(Guid MessageId, string Reason)> toDeadLetter,
        CancellationToken token)
    {
        var hasWork = toComplete.Count > 0 || toFail.Count > 0 || toRelease.Count > 0 || toDeadLetter.Count > 0;
        if (!hasWork) return;

        await RetryExecutor.ExecuteAsync(async ct =>
        {
            await using var connection = await PostgresOptions.DataSource.OpenConnectionAsync(ct);
            await using var transaction = await connection.BeginTransactionAsync(ct);

            try
            {
                if (toComplete.Count > 0)
                {
                    await using var cmd = new NpgsqlCommand(Sql.CompleteBatch, connection, transaction);
                    cmd.Parameters.AddWithValue("ids", AsArray(toComplete));
                    await cmd.ExecuteNonQueryAsync(ct);
                }

                if (toFail.Count > 0)
                {
                    await using var cmd = new NpgsqlCommand(Sql.FailBatch, connection, transaction);
                    cmd.Parameters.AddWithValue("ids", AsArray(toFail));
                    await cmd.ExecuteNonQueryAsync(ct);
                }

                if (toRelease.Count > 0)
                {
                    await using var cmd = new NpgsqlCommand(Sql.ReleaseBatch, connection, transaction);
                    cmd.Parameters.AddWithValue("ids", AsArray(toRelease));
                    await cmd.ExecuteNonQueryAsync(ct);
                }

                if (toDeadLetter.Count > 0)
                {
                    var messageIds = toDeadLetter.Select(m => m.MessageId).ToArray();

                    if (Configuration.Options.EnableDeadLetter)
                    {
                        await InsertToDeadLetterAsync(connection, transaction, toDeadLetter, ct);
                    }

                    await using var deleteCmd = new NpgsqlCommand(Sql.DeleteBatch, connection, transaction);
                    deleteCmd.Parameters.AddWithValue("ids", messageIds);
                    await deleteCmd.ExecuteNonQueryAsync(ct);
                }

                await transaction.CommitAsync(ct);
            }
            catch
            {
                await transaction.RollbackAsync(ct);
                throw;
            }
        }, token);
    }

    public async Task<IReadOnlyList<DeadLetterMessage>> ReadDeadLettersAsync(int count, CancellationToken token)
    {
        if (!Configuration.Options.EnableDeadLetter)
        {
            return [];
        }

        return await RetryExecutor.ExecuteAsync(async ct =>
        {
            await using var connection = await PostgresOptions.DataSource.OpenConnectionAsync(ct);
            await using var cmd = new NpgsqlCommand(Sql.ReadDeadLetters, connection);
            cmd.Parameters.AddWithValue("inboxName", Configuration.InboxName);
            cmd.Parameters.AddWithValue("count", count);

            var result = new List<DeadLetterMessage>();

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                result.Add(new DeadLetterMessage
                {
                    Id = reader.GetGuid(0),
                    InboxName = reader.GetString(1),
                    MessageType = reader.GetString(2),
                    Payload = reader.GetString(3),
                    GroupId = reader.IsDBNull(4) ? null : reader.GetString(4),
                    CollapseKey = reader.IsDBNull(5) ? null : reader.GetString(5),
                    AttemptsCount = reader.GetInt32(6),
                    ReceivedAt = reader.GetDateTime(7),
                    FailureReason = reader.GetString(8),
                    MovedAt = reader.GetDateTime(9)
                });
            }

            return result;
        }, token);
    }

    #endregion

    #region Health Check

    public async Task<InboxHealthMetrics> GetHealthMetricsAsync(CancellationToken token)
    {
        return await RetryExecutor.ExecuteAsync(async ct =>
        {
            await using var connection = await PostgresOptions.DataSource.OpenConnectionAsync(ct);

            var sql = Configuration.Options.EnableDeadLetter
                ? Sql.HealthMetricsWithDlq
                : Sql.HealthMetricsWithoutDlq;

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("inboxName", Configuration.Options.InboxName);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                return new InboxHealthMetrics(
                    reader.GetInt64(0),
                    reader.GetInt64(1),
                    reader.GetInt64(2),
                    reader.IsDBNull(3) ? null : reader.GetDateTime(3));
            }

            return new InboxHealthMetrics(0, 0, 0, null);
        }, token);
    }

    #endregion

    #region Message Parsing Helper

    protected static InboxMessage ParseMessage(NpgsqlDataReader reader)
    {
        return new InboxMessage
        {
            Id = reader.GetGuid(0),
            MessageType = reader.GetString(2),
            Payload = reader.GetString(3),
            GroupId = reader.IsDBNull(4) ? null : reader.GetString(4),
            CollapseKey = reader.IsDBNull(5) ? null : reader.GetString(5),
            AttemptsCount = reader.GetInt32(6),
            ReceivedAt = reader.GetDateTime(7),
            CapturedAt = reader.IsDBNull(8) ? null : reader.GetDateTime(8),
            CapturedBy = reader.IsDBNull(9) ? null : reader.GetString(9)
        };
    }

    #endregion

    #region Migration Helpers

    protected async Task MigrateBaseTablesAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken token)
    {
        // Main inbox table with common indexes
        var createInboxTable = $@"
            CREATE TABLE IF NOT EXISTS ""{PostgresOptions.TableName}"" (
                id UUID PRIMARY KEY,
                inbox_name TEXT NOT NULL,
                message_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                group_id TEXT,
                collapse_key TEXT,
                deduplication_id TEXT,
                attempts_count INTEGER NOT NULL DEFAULT 0,
                received_at TIMESTAMP WITH TIME ZONE NOT NULL,
                captured_at TIMESTAMP WITH TIME ZONE,
                captured_by TEXT
            );

            -- Pending messages: used by ReadAndCapture, Health check
            CREATE INDEX IF NOT EXISTS ""idx_{PostgresOptions.TableName}_inbox_pending""
                ON ""{PostgresOptions.TableName}"" (inbox_name, received_at) WHERE captured_at IS NULL;

            -- Captured messages: used by Health check
            CREATE INDEX IF NOT EXISTS ""idx_{PostgresOptions.TableName}_captured""
                ON ""{PostgresOptions.TableName}"" (inbox_name, captured_at) WHERE captured_at IS NOT NULL;

            -- Collapse key dedup: used by Write operations
            CREATE INDEX IF NOT EXISTS ""idx_{PostgresOptions.TableName}_inbox_collapse""
                ON ""{PostgresOptions.TableName}"" (inbox_name, collapse_key) WHERE collapse_key IS NOT NULL AND captured_at IS NULL;

            -- Deduplication: used for deduplication on write
            CREATE UNIQUE INDEX IF NOT EXISTS ""idx_{PostgresOptions.TableName}_deduplication""
                ON ""{PostgresOptions.TableName}"" (inbox_name, deduplication_id) WHERE deduplication_id IS NOT NULL;
        ";

        await using var cmd = new NpgsqlCommand(createInboxTable, connection, transaction);
        await cmd.ExecuteNonQueryAsync(token);

        // Dead letter table
        if (Configuration.Options.EnableDeadLetter)
        {
            var createDlqTable = $@"
                CREATE TABLE IF NOT EXISTS ""{PostgresOptions.DeadLetterTableName}"" (
                    id UUID PRIMARY KEY,
                    inbox_name TEXT NOT NULL,
                    message_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    group_id TEXT,
                    collapse_key TEXT,
                    deduplication_id TEXT,
                    attempts_count INTEGER NOT NULL DEFAULT 0,
                    received_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    failure_reason TEXT NOT NULL,
                    moved_at TIMESTAMP WITH TIME ZONE NOT NULL
                );

                CREATE INDEX IF NOT EXISTS ""idx_{PostgresOptions.DeadLetterTableName}_inbox_moved""
                    ON ""{PostgresOptions.DeadLetterTableName}"" (inbox_name, moved_at);
            ";

            await using var dlqCmd = new NpgsqlCommand(createDlqTable, connection, transaction);
            await dlqCmd.ExecuteNonQueryAsync(token);
        }

        // Deduplication table
        if (Configuration.Options.EnableDeduplication && Configuration.Options.DeduplicationInterval > TimeSpan.Zero)
        {
            var createDeduplicationTable = $@"
                CREATE TABLE IF NOT EXISTS ""{PostgresOptions.DeduplicationTableName}"" (
                    inbox_name TEXT NOT NULL,
                    deduplication_id TEXT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    PRIMARY KEY (inbox_name, deduplication_id)
                );

                CREATE INDEX IF NOT EXISTS ""idx_{PostgresOptions.DeduplicationTableName}_cleanup""
                    ON ""{PostgresOptions.DeduplicationTableName}"" (created_at);
            ";

            await using var dedupCmd = new NpgsqlCommand(createDeduplicationTable, connection, transaction);
            await dedupCmd.ExecuteNonQueryAsync(token);
        }
    }

    #endregion
}