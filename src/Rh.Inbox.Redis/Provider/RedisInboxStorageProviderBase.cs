using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Health;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Redis.Options;
using Rh.Inbox.Redis.Provider.Scripts;
using Rh.Inbox.Redis.Utility;
using StackExchange.Redis;

namespace Rh.Inbox.Redis.Provider;

internal abstract class RedisInboxStorageProviderBase : IInboxStorageProvider, ISupportHealthCheck, IAsyncDisposable, IDisposable
{
    protected readonly RedisInboxProviderOptions RedisOptions;
    protected readonly RedisIdentifiers Keys;
    protected readonly IInboxConfiguration Configuration;

    private bool _disposed;

    protected long TtlSeconds => (long)RedisOptions.MaxMessageLifetime.TotalSeconds;

    protected long DedupTtlSeconds => Configuration.Options.EnableDeduplication
        ? (long)Configuration.Options.DeduplicationInterval.TotalSeconds
        : 0;

    protected long DlqTtlSeconds => Configuration.Options.EnableDeadLetter
        ? (long)Configuration.Options.DeadLetterMaxMessageLifetime.TotalSeconds
        : TtlSeconds;

    protected string EnableDeadLetterFlag => Configuration.Options.EnableDeadLetter ? "1" : "0";

    protected RedisInboxStorageProviderBase(IProviderOptionsAccessor optionsAccessor, IInboxConfiguration configuration)
    {
        RedisOptions = optionsAccessor.GetForInbox(configuration.InboxName);
        Keys = new RedisIdentifiers(RedisOptions.KeyPrefix);
        Configuration = configuration;
    }

    /// <summary>
    /// Gets a Redis database instance. The connection is managed by the connection provider
    /// which handles reconnection for failed or unhealthy connections.
    /// </summary>
    protected async ValueTask<IDatabase> GetDatabaseAsync(CancellationToken cancellationToken = default)
    {
        var connection = await RedisOptions.ConnectionProvider
            .GetConnectionAsync(RedisOptions.ConnectionString, cancellationToken)
            .ConfigureAwait(false);

        return connection.GetDatabase();
    }

    protected static long ToUnixMilliseconds(DateTime dateTime)
    {
        // Convert to UTC using standard .NET behavior:
        // - Utc: used as-is
        // - Local: converted to UTC
        // - Unspecified: treated as Local and converted to UTC (matches DateTime.ToUniversalTime behavior)
        var utc = dateTime.Kind == DateTimeKind.Utc
            ? dateTime
            : dateTime.ToUniversalTime();

        return new DateTimeOffset(utc).ToUnixTimeMilliseconds();
    }

    protected static DateTime FromUnixMilliseconds(long milliseconds) =>
        DateTimeOffset.FromUnixTimeMilliseconds(milliseconds).UtcDateTime;

    protected static string ToIsoString(DateTime dateTime) =>
        dateTime.ToString("O", System.Globalization.CultureInfo.InvariantCulture);

    #region Abstract Methods

    public abstract Task<IReadOnlyList<InboxMessage>> ReadAndCaptureAsync(string processorId, CancellationToken token);

    #endregion

    #region Write Operations

    public async Task WriteAsync(InboxMessage message, CancellationToken token)
    {
        var db = await GetDatabaseAsync(token).ConfigureAwait(false);
        await db.ScriptEvaluateAsync(RedisScripts.Write, CreateWriteParameters(message)).ConfigureAwait(false);
    }

    public async Task WriteBatchAsync(IEnumerable<InboxMessage> messages, CancellationToken token)
    {
        var messageList = messages as IList<InboxMessage> ?? messages.ToList();

        if (messageList.Count == 0)
            return;

        var db = await GetDatabaseAsync(token).ConfigureAwait(false);

        var simpleMessages = new List<InboxMessage>();
        var collapseMessages = new List<InboxMessage>();

        foreach (var message in messageList)
        {
            if (string.IsNullOrEmpty(message.CollapseKey))
                simpleMessages.Add(message);
            else
                collapseMessages.Add(message);
        }

        if (simpleMessages.Count > 0)
        {
            await ExecuteWriteBatchAsync(db, simpleMessages).ConfigureAwait(false);
        }

        if (collapseMessages.Count > 0)
        {
            await ExecuteWriteBatchCollapseAsync(db, collapseMessages).ConfigureAwait(false);
        }
    }

    private async Task ExecuteWriteBatchAsync(IDatabase db, List<InboxMessage> messages)
    {
        var argv = new RedisValue[ScriptLayout.WriteBatchHeaderSize + (messages.Count * ScriptLayout.WriteBatchFieldsPerMessage)];

        argv[0] = Keys.PendingKey;
        argv[1] = Keys.MsgKeyBase;
        argv[2] = Configuration.InboxName;
        argv[3] = TtlSeconds;
        argv[4] = DedupTtlSeconds;

        for (var i = 0; i < messages.Count; i++)
        {
            var baseIndex = ScriptLayout.WriteBatchHeaderSize + (i * ScriptLayout.WriteBatchFieldsPerMessage);
            var param = BuildWriteParams(messages[i], sequenceOffset: i);
            param.WriteToArgv(argv, baseIndex);
        }

        await db.ScriptEvaluateAsync(RedisScripts.WriteBatch, Array.Empty<RedisKey>(), argv).ConfigureAwait(false);
    }

    private async Task ExecuteWriteBatchCollapseAsync(IDatabase db, List<InboxMessage> messages)
    {
        var argv = new RedisValue[ScriptLayout.WriteBatchWithCollapseHeaderSize + (messages.Count * ScriptLayout.WriteBatchWithCollapseFieldsPerMessage)];

        // Header
        argv[0] = Keys.PendingKey;
        argv[1] = Keys.CapturedKey;
        argv[2] = Keys.CollapseKey;
        argv[3] = Keys.MsgKeyBase;
        argv[4] = Configuration.InboxName;
        argv[5] = TtlSeconds;
        argv[6] = DedupTtlSeconds;

        for (var i = 0; i < messages.Count; i++)
        {
            var message = messages[i];
            var baseIndex = ScriptLayout.WriteBatchWithCollapseHeaderSize + (i * ScriptLayout.WriteBatchWithCollapseFieldsPerMessage);

            var deduplicationId = message.DeduplicationId ?? "";
            var dedupKey = !string.IsNullOrEmpty(deduplicationId) && DedupTtlSeconds > 0
                ? Keys.DeduplicationKey(deduplicationId)
                : "";

            argv[baseIndex] = message.Id.ToString();
            argv[baseIndex + 1] = message.MessageType;
            argv[baseIndex + 2] = message.Payload;
            argv[baseIndex + 3] = message.GroupId ?? "";
            argv[baseIndex + 4] = message.CollapseKey ?? "";
            argv[baseIndex + 5] = deduplicationId;
            argv[baseIndex + 6] = message.AttemptsCount;
            argv[baseIndex + 7] = ToIsoString(message.ReceivedAt);
            argv[baseIndex + 8] = ToUnixMilliseconds(message.ReceivedAt) + i;
            argv[baseIndex + 9] = dedupKey;
        }

        await db.ScriptEvaluateAsync(RedisScripts.WriteBatchWithCollapse, Array.Empty<RedisKey>(), argv).ConfigureAwait(false);
    }

    private WriteMessageParams BuildWriteParams(InboxMessage message, int sequenceOffset = 0)
    {
        var deduplicationId = message.DeduplicationId ?? "";
        var dedupKey = !string.IsNullOrEmpty(deduplicationId) && DedupTtlSeconds > 0
            ? Keys.DeduplicationKey(deduplicationId)
            : "";

        return new WriteMessageParams
        {
            Id = message.Id.ToString(),
            InboxName = Configuration.InboxName,
            MessageType = message.MessageType,
            Payload = message.Payload,
            GroupId = message.GroupId ?? "",
            CollapseKeyValue = message.CollapseKey ?? "",
            DeduplicationId = deduplicationId,
            AttemptsCount = message.AttemptsCount,
            ReceivedAt = ToIsoString(message.ReceivedAt),
            ReceivedAtScore = ToUnixMilliseconds(message.ReceivedAt) + sequenceOffset,
            DedupKey = dedupKey
        };
    }

    private object CreateWriteParameters(InboxMessage message)
    {
        var p = BuildWriteParams(message);

        return new
        {
            pendingKey = (RedisKey)Keys.PendingKey,
            capturedKey = (RedisKey)Keys.CapturedKey,
            collapseKey = (RedisKey)Keys.CollapseKey,
            messageKey = (RedisKey)Keys.MessageKey(message.Id),
            msgKeyBase = Keys.MsgKeyBase,
            id = p.Id,
            inboxName = p.InboxName,
            messageType = p.MessageType,
            payload = p.Payload,
            groupId = p.GroupId,
            collapseKeyValue = p.CollapseKeyValue,
            deduplicationId = p.DeduplicationId,
            attemptsCount = p.AttemptsCount,
            receivedAt = p.ReceivedAt,
            receivedAtScore = p.ReceivedAtScore,
            ttlSeconds = TtlSeconds,
            dedupKey = (RedisKey)p.DedupKey,
            dedupTtlSeconds = DedupTtlSeconds
        };
    }

    private readonly struct WriteMessageParams
    {
        public required string Id { get; init; }
        public required string InboxName { get; init; }
        public required string MessageType { get; init; }
        public required string Payload { get; init; }
        public required string GroupId { get; init; }
        public required string CollapseKeyValue { get; init; }
        public required string DeduplicationId { get; init; }
        public required int AttemptsCount { get; init; }
        public required string ReceivedAt { get; init; }
        public required long ReceivedAtScore { get; init; }
        public required string DedupKey { get; init; }

        public void WriteToArgv(RedisValue[] argv, int baseIndex)
        {
            argv[baseIndex] = Id;
            argv[baseIndex + 1] = MessageType;
            argv[baseIndex + 2] = Payload;
            argv[baseIndex + 3] = GroupId;
            argv[baseIndex + 4] = DeduplicationId;
            argv[baseIndex + 5] = AttemptsCount;
            argv[baseIndex + 6] = ReceivedAt;
            argv[baseIndex + 7] = ReceivedAtScore;
            argv[baseIndex + 8] = DedupKey;
        }
    }

    #endregion

    #region Lifecycle Operations

    public Task FailAsync(Guid messageId, CancellationToken token) =>
        ProcessResultsBatchAsync([], [messageId], [], [], token);

    public Task FailBatchAsync(IReadOnlyList<Guid> messageIds, CancellationToken token) =>
        ProcessResultsBatchAsync([], messageIds, [], [], token);

    public Task ReleaseBatchAsync(IReadOnlyList<Guid> messageIds, CancellationToken token) =>
        ProcessResultsBatchAsync([], [], messageIds, [], token);

    public Task MoveToDeadLetterAsync(Guid messageId, string reason, CancellationToken token) =>
        ProcessResultsBatchAsync([], [], [], [(messageId, reason)], token);

    public Task MoveToDeadLetterBatchAsync(IReadOnlyList<(Guid MessageId, string Reason)> messages, CancellationToken token) =>
        ProcessResultsBatchAsync([], [], [], messages, token);

    public async Task ProcessResultsBatchAsync(
        IReadOnlyList<Guid> toComplete,
        IReadOnlyList<Guid> toFail,
        IReadOnlyList<Guid> toRelease,
        IReadOnlyList<(Guid MessageId, string Reason)> toDeadLetter,
        CancellationToken token)
    {
        var hasWork = toComplete.Count > 0 || toFail.Count > 0 || toRelease.Count > 0 || toDeadLetter.Count > 0;
        if (!hasWork) return;

        var db = await GetDatabaseAsync(token).ConfigureAwait(false);
        var tasks = new List<Task>(4);
        var batch = db.CreateBatch();

        if (toComplete.Count > 0)
        {
            var argv = BuildCompleteArgv(toComplete);
            tasks.Add(batch.ScriptEvaluateAsync(RedisScripts.ProcessComplete, Array.Empty<RedisKey>(), argv));
        }

        if (toFail.Count > 0)
        {
            var argv = BuildFailArgv(toFail);
            tasks.Add(batch.ScriptEvaluateAsync(RedisScripts.ProcessFail, Array.Empty<RedisKey>(), argv));
        }

        if (toRelease.Count > 0)
        {
            var argv = BuildReleaseArgv(toRelease);
            tasks.Add(batch.ScriptEvaluateAsync(RedisScripts.ProcessRelease, Array.Empty<RedisKey>(), argv));
        }

        if (toDeadLetter.Count > 0)
        {
            var movedAt = Configuration.DateTimeProvider.GetUtcNow();
            var argv = BuildDeadLetterArgv(toDeadLetter, movedAt);
            tasks.Add(batch.ScriptEvaluateAsync(RedisScripts.ProcessDeadLetter, Array.Empty<RedisKey>(), argv));
        }

        batch.Execute();
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<DeadLetterMessage>> ReadDeadLettersAsync(int count, CancellationToken token)
    {
        if (!Configuration.Options.EnableDeadLetter)
            return [];

        var db = await GetDatabaseAsync(token).ConfigureAwait(false);
        var ids = await db.SortedSetRangeByRankAsync(Keys.DeadLetterKey, 0, count - 1).ConfigureAwait(false);

        if (ids.Length == 0)
            return [];

        var batch = db.CreateBatch();
        var hashTasks = ids
            .Select(idValue => batch.HashGetAllAsync(Keys.DeadLetterMessageKey(Guid.Parse(idValue.ToString()))))
            .ToList();
        batch.Execute();

        var results = await Task.WhenAll(hashTasks).ConfigureAwait(false);

        var messages = new List<DeadLetterMessage>(results.Length);
        foreach (var hash in results)
        {
            if (hash.Length == 0)
                continue;

            var message = ParseDeadLetterHashData(hash);
            if (message != null)
                messages.Add(message);
        }

        return messages;
    }

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
            return 0;

        var db = await GetDatabaseAsync(token).ConfigureAwait(false);
        var argv = BuildExtendMessageLocksArgv(processorId, capturedMessages, newCapturedAt);
        var result = await db.ScriptEvaluateAsync(RedisScripts.ExtendMessageLocks, Array.Empty<RedisKey>(), argv).ConfigureAwait(false);
        return (int)result;
    }

    protected RedisValue[] BuildExtendMessageLocksArgv(
        string processorId,
        IReadOnlyList<IInboxMessageIdentifiers> capturedMessages,
        DateTime newCapturedAt)
    {
        // ARGV: [capturedKey, msgKeyBase, processorId, newCapturedAtStr, newCapturedAtScore, ...ids]
        var argv = new RedisValue[5 + capturedMessages.Count];
        argv[0] = Keys.CapturedKey;
        argv[1] = Keys.MsgKeyBase;
        argv[2] = processorId;
        argv[3] = ToIsoString(newCapturedAt);
        argv[4] = ToUnixMilliseconds(newCapturedAt);

        for (var i = 0; i < capturedMessages.Count; i++)
            argv[5 + i] = capturedMessages[i].Id.ToString();

        return argv;
    }

    private RedisValue[] BuildCompleteArgv(IReadOnlyList<Guid> messageIds)
    {
        // ARGV: [pendingKey, capturedKey, collapseKey, msgKeyBase, ...ids]
        var argv = new RedisValue[4 + messageIds.Count];
        argv[0] = Keys.PendingKey;
        argv[1] = Keys.CapturedKey;
        argv[2] = Keys.CollapseKey;
        argv[3] = Keys.MsgKeyBase;

        for (var i = 0; i < messageIds.Count; i++)
            argv[4 + i] = messageIds[i].ToString();

        return argv;
    }

    private RedisValue[] BuildFailArgv(IReadOnlyList<Guid> messageIds)
    {
        // ARGV: [capturedKey, msgKeyBase, ttlSeconds, ...ids]
        var argv = new RedisValue[3 + messageIds.Count];
        argv[0] = Keys.CapturedKey;
        argv[1] = Keys.MsgKeyBase;
        argv[2] = TtlSeconds;

        for (var i = 0; i < messageIds.Count; i++)
            argv[3 + i] = messageIds[i].ToString();

        return argv;
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

    private RedisValue[] BuildDeadLetterArgv(IReadOnlyList<(Guid MessageId, string Reason)> messages, DateTime movedAt)
    {
        // ARGV: [pendingKey, capturedKey, collapseKey, dlqKey, msgKeyBase, dlqKeyBase,
        //        ttlSeconds, enableDlq, movedAt, movedAtScore, ...(id, reason) pairs]
        var argv = new RedisValue[10 + (messages.Count * 2)];
        argv[0] = Keys.PendingKey;
        argv[1] = Keys.CapturedKey;
        argv[2] = Keys.CollapseKey;
        argv[3] = Keys.DeadLetterKey;
        argv[4] = Keys.MsgKeyBase;
        argv[5] = Keys.DlqKeyBase;
        argv[6] = DlqTtlSeconds;
        argv[7] = EnableDeadLetterFlag;
        argv[8] = ToIsoString(movedAt);
        argv[9] = ToUnixMilliseconds(movedAt);

        for (var i = 0; i < messages.Count; i++)
        {
            var baseIndex = 10 + (i * 2);
            argv[baseIndex] = messages[i].MessageId.ToString();
            argv[baseIndex + 1] = messages[i].Reason;
        }

        return argv;
    }

    private static DeadLetterMessage? ParseDeadLetterHashData(HashEntry[] hash)
    {
        var reader = RedisHashReader.Create(hash);

        if (!reader.TryGetGuid("id", out var id) ||
            !reader.TryGetString("inbox_name", out var inboxName) ||
            !reader.TryGetString("message_type", out var messageType) ||
            !reader.TryGetString("payload", out var payload) ||
            !reader.TryGetString("failure_reason", out var failureReason))
            return null;

        return new DeadLetterMessage
        {
            Id = id,
            InboxName = inboxName,
            MessageType = messageType,
            Payload = payload,
            GroupId = reader.GetString("group_id"),
            CollapseKey = reader.GetString("collapse_key"),
            AttemptsCount = reader.GetInt("attempts_count"),
            ReceivedAt = reader.GetDateTime("received_at"),
            FailureReason = failureReason,
            MovedAt = reader.GetDateTime("moved_at")
        };
    }

    #endregion

    #region Health Check

    public async Task<InboxHealthMetrics> GetHealthMetricsAsync(CancellationToken token)
    {
        var db = await GetDatabaseAsync(token).ConfigureAwait(false);

        var now = Configuration.DateTimeProvider.GetUtcNow();
        var expiredCaptureThreshold = now - Configuration.Options.MaxProcessingTime;

        var parameters = new
        {
            pendingKey = (RedisKey)Keys.PendingKey,
            capturedKey = (RedisKey)Keys.CapturedKey,
            dlqKey = (RedisKey)Keys.DeadLetterKey,
            expiredCaptureThreshold = ToUnixMilliseconds(expiredCaptureThreshold).ToString(System.Globalization.CultureInfo.InvariantCulture),
            enableDeadLetter = EnableDeadLetterFlag
        };

        var result = await db.ScriptEvaluateAsync(RedisScripts.GetHealthMetrics, parameters).ConfigureAwait(false);
        var array = (RedisResult[]?)result;

        if (array == null || array.Length < 4)
        {
            return new InboxHealthMetrics(0, 0, 0, null);
        }

        var actualPending = (long)array[0];
        var capturedCount = (long)array[1];
        var deadLetterCount = (long)array[2];
        DateTime? oldestPendingAt = null;

        if (!array[3].IsNull)
        {
            var oldestScore = (long)array[3];
            oldestPendingAt = FromUnixMilliseconds(oldestScore);
        }

        return new InboxHealthMetrics(
            actualPending,
            capturedCount,
            deadLetterCount,
            oldestPendingAt);
    }

    #endregion

    #region Read Result Parsing

    protected static List<InboxMessage> ParseReadResult(RedisResult result)
    {
        var array = (RedisResult[]?)result;
        if (array == null || array.Length == 0)
            return [];

        var messages = new List<InboxMessage>(array.Length / 2);

        for (var i = 0; i < array.Length; i += 2)
        {
            if (i + 1 >= array.Length)
                break;

            var hashData = (RedisResult[]?)array[i + 1];
            if (hashData == null || hashData.Length == 0)
                continue;

            var message = ParseHashData(hashData);
            if (message != null)
                messages.Add(message);
        }

        return messages;
    }

    private static InboxMessage? ParseHashData(RedisResult[] hashData)
    {
        var reader = RedisHashReader.Create(hashData);

        if (!reader.TryGetGuid("id", out var id) ||
            !reader.TryGetString("message_type", out var messageType) ||
            !reader.TryGetString("payload", out var payload))
            return null;

        return new InboxMessage
        {
            Id = id,
            MessageType = messageType,
            Payload = payload,
            GroupId = reader.GetString("group_id"),
            CollapseKey = reader.GetString("collapse_key"),
            AttemptsCount = reader.GetInt("attempts_count"),
            ReceivedAt = reader.GetDateTime("received_at"),
            CapturedAt = reader.GetNullableDateTime("captured_at"),
            CapturedBy = reader.GetString("captured_by")
        };
    }

    #endregion

    #region Dispose

    public ValueTask DisposeAsync()
    {
        if (_disposed)
            return ValueTask.CompletedTask;

        _disposed = true;
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        GC.SuppressFinalize(this);
    }

    #endregion
}