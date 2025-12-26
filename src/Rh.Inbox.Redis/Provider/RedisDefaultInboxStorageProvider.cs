using Microsoft.Extensions.Logging;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Redis.Options;
using Rh.Inbox.Redis.Provider.Scripts;
using StackExchange.Redis;

namespace Rh.Inbox.Redis.Provider;

/// <summary>
/// Redis inbox storage provider for Default and Batched inbox types.
/// Does not use group locking - messages are processed in order of receipt without FIFO guarantees per group.
/// </summary>
internal sealed class RedisDefaultInboxStorageProvider : RedisInboxStorageProviderBase
{
    public RedisDefaultInboxStorageProvider(
        IProviderOptionsAccessor optionsAccessor,
        IInboxConfiguration configuration,
        ILogger<RedisDefaultInboxStorageProvider> logger)
        : base(optionsAccessor, configuration, logger)
    {
    }

    public override async Task<IReadOnlyList<InboxMessage>> ReadAndCaptureAsync(string processorId, CancellationToken token)
    {
        return await RetryExecutor.ExecuteAsync(async ct =>
        {
            var db = await GetDatabaseAsync(ct).ConfigureAwait(false);

            var result = await db.ScriptEvaluateAsync(RedisScripts.ReadDefault, CreateReadParameters(processorId)).ConfigureAwait(false);

            return ParseReadResult(result);
        }, token);
    }

    private object CreateReadParameters(string processorId)
    {
        var now = Configuration.DateTimeProvider.GetUtcNow();
        var expiredThreshold = now - Configuration.Options.MaxProcessingTime;

        return new
        {
            pendingKey = (RedisKey)Keys.PendingKey,
            capturedKey = (RedisKey)Keys.CapturedKey,
            msgKeyBase = Keys.MsgKeyBase,
            batchSize = Configuration.Options.ReadBatchSize,
            now = ToUnixMilliseconds(now),
            nowStr = ToIsoString(now),
            expiredThreshold = ToUnixMilliseconds(expiredThreshold),
            processorId
        };
    }
}