using Microsoft.Extensions.Logging;
using Npgsql;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Postgres.Options;
using Rh.Inbox.Postgres.Scripts;

namespace Rh.Inbox.Postgres.Provider;

/// <summary>
/// Postgres inbox storage provider for Default and Batched inbox types.
/// Does not use group locking - messages are processed in order of receipt without FIFO guarantees per group.
/// </summary>
internal sealed class PostgresDefaultInboxStorageProvider : PostgresInboxStorageProviderBase
{
    public PostgresDefaultInboxStorageProvider(
        IInboxConfiguration configuration,
        IProviderOptionsAccessor optionsAccessor,
        ILogger<PostgresDefaultInboxStorageProvider> logger)
        : base(configuration, optionsAccessor, CreateSqlScripts(optionsAccessor.GetForInbox(configuration.InboxName)), logger)
    {
    }

    private static PostgresDefaultSqlScripts CreateSqlScripts(PostgresInboxProviderOptions postgresOptions) =>
        new(postgresOptions.TableName, postgresOptions.DeadLetterTableName, postgresOptions.DeduplicationTableName);

    public override async Task<IReadOnlyList<InboxMessage>> ReadAndCaptureAsync(string processorId, CancellationToken token)
    {
        return await RetryExecutor.ExecuteAsync(async ct =>
        {
            await using var connection = await PostgresOptions.DataSource.OpenConnectionAsync(ct);

            var now = Configuration.DateTimeProvider.GetUtcNow();
            var maxProcessingTime = now - Configuration.Options.MaxProcessingTime;

            await using var cmd = new NpgsqlCommand(Sql.ReadAndCapture, connection);
            cmd.Parameters.AddWithValue("inboxName", Configuration.InboxName);
            cmd.Parameters.AddWithValue("maxProcessingTime", maxProcessingTime);
            cmd.Parameters.AddWithValue("batchSize", Configuration.Options.ReadBatchSize);
            cmd.Parameters.AddWithValue("now", now);
            cmd.Parameters.AddWithValue("processorId", processorId);

            var messages = new List<InboxMessage>();

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                messages.Add(ParseMessage(reader));
            }

            return messages;
        }, token);
    }

    public override async Task MigrateAsync(CancellationToken token)
    {
        await RetryExecutor.ExecuteAsync(async ct =>
        {
            await using var connection = await PostgresOptions.DataSource.OpenConnectionAsync(ct);
            await using var transaction = await connection.BeginTransactionAsync(ct);

            try
            {
                await MigrateBaseTablesAsync(connection, transaction, ct);
                await transaction.CommitAsync(ct);
            }
            catch
            {
                await transaction.RollbackAsync(ct);
                throw;
            }
        }, token);
    }
}