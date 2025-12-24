using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Redis.Options;
using Rh.Inbox.Redis.Provider.Scripts;
using StackExchange.Redis;

namespace Rh.Inbox.Redis.Provider;

/// <summary>
/// Redis inbox storage provider for FIFO and FifoBatched inbox types.
/// Ensures strict message ordering within each group using individual lock keys with TTL.
/// </summary>
/// <remarks>
/// This provider implements <see cref="ISupportGroupLocksReleaseStorageProvider"/> for explicit group lock release.
///
/// Lock mechanism:
/// <list type="bullet">
///   <item><description>Each group gets an individual lock key: {prefix}:lock:{groupId}</description></item>
///   <item><description>Locks have TTL equal to MaxProcessingTime option</description></item>
///   <item><description>Multiple messages from the same group can be captured by the same worker in one batch</description></item>
///   <item><description>Other workers are blocked from processing a group until the lock is released or expires</description></item>
/// </list>
///
/// Lock release:
/// <list type="bullet">
///   <item><description>Explicit: Call <see cref="ReleaseGroupLocksAsync"/> after all messages from a group are processed</description></item>
///   <item><description>Automatic: TTL expiration handles crashed workers (no cleanup job needed)</description></item>
/// </list>
/// </remarks>
internal sealed class RedisFifoInboxStorageProvider : RedisInboxStorageProviderBase, ISupportGroupLocksReleaseStorageProvider
{
    public RedisFifoInboxStorageProvider(IProviderOptionsAccessor optionsAccessor, IInboxConfiguration configuration)
        : base(optionsAccessor, configuration)
    {
    }

    public override async Task<IReadOnlyList<InboxMessage>> ReadAndCaptureAsync(string processorId, CancellationToken token)
    {
        var db = await GetDatabaseAsync(token).ConfigureAwait(false);

        var result = await db.ScriptEvaluateAsync(RedisScripts.ReadFifo, CreateReadParameters(processorId)).ConfigureAwait(false);

        return ParseReadResult(result);
    }

    /// <summary>
    /// Releases group locks after all messages from these groups are processed.
    /// This allows other workers to process messages from these groups.
    /// If not called, locks expire automatically after MaxProcessingTime (TTL).
    /// </summary>
    public async Task ReleaseGroupLocksAsync(IReadOnlyList<string> groupIds, CancellationToken token)
    {
        if (groupIds.Count == 0) return;

        var db = await GetDatabaseAsync(token).ConfigureAwait(false);
        await ExecuteReleaseGroupLocksAsync(db, groupIds).ConfigureAwait(false);
    }

    /// <summary>
    /// Releases captured messages and their group locks in a single batch operation.
    /// Uses Redis pipeline for optimal performance.
    /// </summary>
    public async Task ReleaseMessagesAndGroupLocksAsync(IReadOnlyList<IInboxMessageIdentifiers> messages, CancellationToken token)
    {
        if (messages.Count == 0) return;

        var messageIds = messages.Select(m => m.Id).ToList();
        var groupIds = messages
            .Where(m => !string.IsNullOrEmpty(m.GroupId))
            .Select(m => m.GroupId!)
            .Distinct()
            .ToList();

        var hasGroups = groupIds.Count > 0;

        var db = await GetDatabaseAsync(token).ConfigureAwait(false);

        if (!hasGroups)
        {
            await ReleaseBatchAsync(messageIds, token).ConfigureAwait(false);
            return;
        }

        // Execute both operations in a single batch/pipeline
        var batch = db.CreateBatch();
        var tasks = new List<Task>(2);

        // Release messages
        var releaseArgv = BuildReleaseArgv(messageIds);
        tasks.Add(batch.ScriptEvaluateAsync(RedisScripts.ProcessRelease, Array.Empty<RedisKey>(), releaseArgv));

        // Release group locks
        var groupLocksArgv = BuildReleaseGroupLocksArgv(groupIds);
        tasks.Add(batch.ScriptEvaluateAsync(RedisScripts.ReleaseGroupLocks, Array.Empty<RedisKey>(), groupLocksArgv));

        batch.Execute();
        await Task.WhenAll(tasks).ConfigureAwait(false);
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
            return 0;

        var groupIds = capturedMessages
            .Where(m => !string.IsNullOrEmpty(m.GroupId))
            .Select(m => m.GroupId!)
            .Distinct()
            .ToList();

        var hasGroups = groupIds.Count > 0;

        var db = await GetDatabaseAsync(token).ConfigureAwait(false);

        // If no groups, just extend message locks
        if (!hasGroups)
        {
            var argv = BuildExtendMessageLocksArgv(processorId, capturedMessages, newCapturedAt);
            var result = await db.ScriptEvaluateAsync(RedisScripts.ExtendMessageLocks, Array.Empty<RedisKey>(), argv).ConfigureAwait(false);
            return (int)result;
        }

        // Extend both message and group locks in a single batch
        var batch = db.CreateBatch();
        var tasks = new List<Task<RedisResult>>(2);

        var msgArgv = BuildExtendMessageLocksArgv(processorId, capturedMessages, newCapturedAt);
        tasks.Add(batch.ScriptEvaluateAsync(RedisScripts.ExtendMessageLocks, Array.Empty<RedisKey>(), msgArgv));

        var groupArgv = BuildExtendGroupLocksArgv(processorId, groupIds);
        tasks.Add(batch.ScriptEvaluateAsync(RedisScripts.ExtendGroupLocks, Array.Empty<RedisKey>(), groupArgv));

        batch.Execute();
        var results = await Task.WhenAll(tasks).ConfigureAwait(false);

        // Return count of extended message locks
        return (int)results[0];
    }

    private RedisValue[] BuildExtendGroupLocksArgv(string processorId, IReadOnlyList<string> groupIds)
    {
        // ARGV: [lockKeyBase, processorId, lockTtlSeconds, ...groupIds]
        var lockTtlSeconds = (long)Configuration.Options.MaxProcessingTime.TotalSeconds;
        var argv = new RedisValue[3 + groupIds.Count];
        argv[0] = Keys.LockKeyBase;
        argv[1] = processorId;
        argv[2] = lockTtlSeconds;

        for (var i = 0; i < groupIds.Count; i++)
            argv[3 + i] = groupIds[i];

        return argv;
    }

    private async Task ExecuteReleaseGroupLocksAsync(IDatabase db, IReadOnlyList<string> groupIds)
    {
        var argv = BuildReleaseGroupLocksArgv(groupIds);
        await db.ScriptEvaluateAsync(RedisScripts.ReleaseGroupLocks, Array.Empty<RedisKey>(), argv).ConfigureAwait(false);
    }

    private RedisValue[] BuildReleaseArgv(IReadOnlyList<Guid> messageIds)
    {
        // ARGV: [capturedKey, msgKeyBase, ...ids]
        var argv = new RedisValue[2 + messageIds.Count];
        argv[0] = Keys.CapturedKey;
        argv[1] = Keys.MsgKeyBase;

        for (var i = 0; i < messageIds.Count; i++)
            argv[2 + i] = messageIds[i].ToString();

        return argv;
    }

    private RedisValue[] BuildReleaseGroupLocksArgv(IReadOnlyList<string> groupIds)
    {
        var argv = new RedisValue[1 + groupIds.Count];
        argv[0] = Keys.LockKeyBase;

        for (var i = 0; i < groupIds.Count; i++)
            argv[1 + i] = groupIds[i];

        return argv;
    }

    private object CreateReadParameters(string processorId)
    {
        var now = Configuration.DateTimeProvider.GetUtcNow();
        var expiredThreshold = now - Configuration.Options.MaxProcessingTime;
        var lockTtlSeconds = (long)Configuration.Options.MaxProcessingTime.TotalSeconds;

        return new
        {
            pendingKey = (RedisKey)Keys.PendingKey,
            capturedKey = (RedisKey)Keys.CapturedKey,
            lockKeyBase = Keys.LockKeyBase,
            msgKeyBase = Keys.MsgKeyBase,
            batchSize = Configuration.Options.ReadBatchSize,
            now = ToUnixMilliseconds(now),
            nowStr = ToIsoString(now),
            expiredThreshold = ToUnixMilliseconds(expiredThreshold),
            lockTtlSeconds,
            processorId
        };
    }
}