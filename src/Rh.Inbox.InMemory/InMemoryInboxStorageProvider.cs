using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Health;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.InMemory.Collections;
using Rh.Inbox.InMemory.Options;

namespace Rh.Inbox.InMemory;

internal sealed class InMemoryInboxStorageProvider : IInboxStorageProvider, ISupportHealthCheck, ISupportGroupLocksReleaseStorageProvider, IDisposable
{
    private readonly IInboxConfiguration _configuration;
    private readonly InMemoryInboxProviderOptions _inMemoryOptions;

    private readonly IndexedSortedCollection<Guid, InboxMessage, DateTime> _messages = new(
        keySelector: m => m.Id,
        sortKeySelector: m => m.ReceivedAt);

    private readonly Dictionary<string, DateTime> _lockedGroups = new();
    private readonly SemaphoreSlim _lock = new(1, 1);

    private bool IsDeduplicationEnabled => _configuration.Options.EnableDeduplication;

    public InMemoryInboxStorageProvider(IProviderOptionsAccessor optionsAccessor, IInboxConfiguration configuration)
    {
        _configuration = configuration;
        _inMemoryOptions = optionsAccessor.GetForInbox(configuration.InboxName);
    }

    public async Task WriteAsync(InboxMessage message, CancellationToken token)
    {
        await _lock.WaitAsync(token);
        try
        {
            // Check deduplication if enabled
            if (IsDeduplicationEnabled && !string.IsNullOrEmpty(message.DeduplicationId))
            {
                var now = _configuration.DateTimeProvider.GetUtcNow();
                var expirationTime = now - _configuration.Options.DeduplicationInterval;

                if (_inMemoryOptions.DeduplicationStore!.Exists(message.DeduplicationId, expirationTime))
                {
                    // Duplicate detected, skip insertion
                    return;
                }

                _inMemoryOptions.DeduplicationStore.AddOrUpdate(message.DeduplicationId, now);
            }

            if (!string.IsNullOrEmpty(message.CollapseKey))
            {
                RemoveMessagesWithCollapseKey(message.CollapseKey);
            }

            _messages.TryAdd(message);
        }
        finally
        {
            _lock.Release();
        }
    }

    private void RemoveMessagesWithCollapseKey(string collapseKey)
    {
        var messagesToRemove = _messages
            .Where(m => m.CollapseKey == collapseKey && m.CapturedAt == null)
            .Select(m => m.Id)
            .ToList();

        foreach (var id in messagesToRemove)
        {
            _messages.TryRemove(id, out _);
        }
    }

    public async Task WriteBatchAsync(IEnumerable<InboxMessage> messages, CancellationToken token)
    {
        await _lock.WaitAsync(token);
        try
        {
            var messageList = messages.ToList();

            // Filter duplicates if deduplication is enabled
            if (IsDeduplicationEnabled)
            {
                messageList = FilterDuplicates(messageList);
            }

            foreach (var message in messageList)
            {
                if (!string.IsNullOrEmpty(message.CollapseKey))
                {
                    RemoveMessagesWithCollapseKey(message.CollapseKey);
                }

                _messages.TryAdd(message);
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    private List<InboxMessage> FilterDuplicates(List<InboxMessage> messages)
    {
        var now = _configuration.DateTimeProvider.GetUtcNow();
        var expirationTime = now - _configuration.Options.DeduplicationInterval;

        var deduplicationIds = messages
            .Where(m => !string.IsNullOrEmpty(m.DeduplicationId))
            .Select(m => m.DeduplicationId!)
            .Distinct()
            .ToArray();

        if (deduplicationIds.Length == 0)
        {
            return messages;
        }

        var existingIds = _inMemoryOptions.DeduplicationStore!.GetExisting(deduplicationIds, expirationTime);
        var newIds = deduplicationIds.Except(existingIds).ToHashSet();

        if (newIds.Count > 0)
        {
            _inMemoryOptions.DeduplicationStore.AddOrUpdateBatch(newIds, now);
        }

        return messages
            .Where(m => string.IsNullOrEmpty(m.DeduplicationId) || newIds.Contains(m.DeduplicationId))
            .ToList();
    }

    public async Task<IReadOnlyList<InboxMessage>> ReadAndCaptureAsync(string processorId, CancellationToken token)
    {
        await _lock.WaitAsync(token);
        try
        {
            var now = _configuration.DateTimeProvider.GetUtcNow();
            var isFifo = _configuration.InboxType is InboxType.Fifo or InboxType.FifoBatched;

            var lockedGroups = isFifo ? GetLockedGroups(now) : null;
            var candidates = GetCandidateMessages(now, isFifo);

            return CaptureMessages(candidates, processorId, now, isFifo, lockedGroups);
        }
        finally
        {
            _lock.Release();
        }
    }

    private HashSet<string> GetLockedGroups(DateTime now)
    {
        var lockedGroups = new HashSet<string>();
        var expiredGroups = new List<string>();

        foreach (var (groupId, capturedAt) in _lockedGroups)
        {
            if ((now - capturedAt) <= _configuration.Options.MaxProcessingTime)
            {
                lockedGroups.Add(groupId);
            }
            else
            {
                expiredGroups.Add(groupId);
            }
        }

        // Clean up expired locks
        foreach (var groupId in expiredGroups)
        {
            _lockedGroups.Remove(groupId);
        }

        return lockedGroups;
    }

    private List<InboxMessage> GetCandidateMessages(DateTime now, bool isFifo)
    {
        // Already sorted by ReceivedAt, then InsertionOrder - no additional sorting needed!
        // The strategy will group by GroupId and maintain order within each group.
        return _messages
            .Where(m => m.CapturedAt == null || (now - m.CapturedAt.Value) > _configuration.Options.MaxProcessingTime)
            .Take(_configuration.Options.ReadBatchSize)
            .ToList();
    }

    private List<InboxMessage> CaptureMessages(
        List<InboxMessage> candidates,
        string processorId,
        DateTime now,
        bool isFifo,
        HashSet<string>? lockedGroups)
    {
        var groupsBeingCaptured = new HashSet<string>();
        var result = new List<InboxMessage>(_configuration.Options.ReadBatchSize);
        var capturedCollapseKeys = new Dictionary<string, InboxMessage>();

        foreach (var message in candidates)
        {
            if (result.Count + capturedCollapseKeys.Count >= _configuration.Options.ReadBatchSize)
            {
                break;
            }

            if (!CanCaptureMessage(message, isFifo, lockedGroups, groupsBeingCaptured))
            {
                continue;
            }

            MarkMessageAsCaptured(message, processorId, now);
            LockGroup(message.GroupId, now);
            AddToResults(message, result, capturedCollapseKeys);
        }

        result.AddRange(capturedCollapseKeys.Values);
        return result;
    }

    private void LockGroup(string? groupId, DateTime capturedAt)
    {
        if (!string.IsNullOrEmpty(groupId))
        {
            _lockedGroups[groupId] = capturedAt;
        }
    }

    private static bool CanCaptureMessage(
        InboxMessage message,
        bool isFifo,
        HashSet<string>? lockedGroups,
        HashSet<string> groupsBeingCaptured)
    {
        if (!isFifo || string.IsNullOrEmpty(message.GroupId))
            return true;

        // Group is locked by another worker - can't capture
        if (lockedGroups!.Contains(message.GroupId))
            return false;

        // Group is being captured in this batch - allow multiple messages from same group
        // (this matches Redis/Postgres behavior)
        if (groupsBeingCaptured.Contains(message.GroupId))
            return true;

        groupsBeingCaptured.Add(message.GroupId);
        return true;
    }

    private static void MarkMessageAsCaptured(InboxMessage message, string processorId, DateTime now)
    {
        message.CapturedAt = now;
        message.CapturedBy = processorId;
    }

    private static void AddToResults(
        InboxMessage message,
        List<InboxMessage> result,
        Dictionary<string, InboxMessage> capturedCollapseKeys)
    {
        if (!string.IsNullOrEmpty(message.CollapseKey))
        {
            capturedCollapseKeys[message.CollapseKey] = message;
        }
        else
        {
            result.Add(message);
        }
    }

    public async Task CompleteAsync(Guid messageId, CancellationToken token)
    {
        await _lock.WaitAsync(token);
        try
        {
            _messages.TryRemove(messageId, out _);
            // Note: Group lock is NOT released here - it's released via ReleaseGroupLocksAsync
            // or expires via TTL (matches Redis/Postgres behavior)
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task CompleteBatchAsync(IReadOnlyList<Guid> messageIds, CancellationToken token)
    {
        if (messageIds.Count == 0) return;

        await _lock.WaitAsync(token);
        try
        {
            foreach (var messageId in messageIds)
            {
                _messages.TryRemove(messageId, out _);
            }
            // Note: Group locks are NOT released here - released via ReleaseGroupLocksAsync
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task FailAsync(Guid messageId, CancellationToken token)
    {
        await _lock.WaitAsync(token);
        try
        {
            if (_messages.TryGetValue(messageId, out var message))
            {
                message.AttemptsCount++;
                message.CapturedAt = null;
                message.CapturedBy = null;
            }
            // Note: Group lock is NOT released here - released via ReleaseGroupLocksAsync
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task FailBatchAsync(IReadOnlyList<Guid> messageIds, CancellationToken token)
    {
        if (messageIds.Count == 0) return;

        await _lock.WaitAsync(token);
        try
        {
            foreach (var messageId in messageIds)
            {
                if (_messages.TryGetValue(messageId, out var message))
                {
                    message.AttemptsCount++;
                    message.CapturedAt = null;
                    message.CapturedBy = null;
                }
            }
            // Note: Group locks are NOT released here - released via ReleaseGroupLocksAsync
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task MoveToDeadLetterAsync(Guid messageId, string reason, CancellationToken token)
    {
        await _lock.WaitAsync(token);
        try
        {
            MoveToDeadLetterInternal(messageId, reason);
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task MoveToDeadLetterBatchAsync(IReadOnlyList<(Guid MessageId, string Reason)> messages, CancellationToken token)
    {
        if (messages.Count == 0) return;

        await _lock.WaitAsync(token);
        try
        {
            foreach (var (messageId, reason) in messages)
            {
                MoveToDeadLetterInternal(messageId, reason);
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    private void MoveToDeadLetterInternal(Guid messageId, string reason)
    {
        if (_messages.TryRemove(messageId, out var message))
        {
            // Note: Group lock is NOT released here - released via ReleaseGroupLocksAsync

            if (_configuration.Options.EnableDeadLetter)
            {
                var deadLetter = new DeadLetterMessage
                {
                    Id = message.Id,
                    InboxName = _configuration.InboxName,
                    MessageType = message.MessageType,
                    Payload = message.Payload,
                    GroupId = message.GroupId,
                    CollapseKey = message.CollapseKey,
                    AttemptsCount = message.AttemptsCount,
                    ReceivedAt = message.ReceivedAt,
                    FailureReason = reason,
                    MovedAt = _configuration.DateTimeProvider.GetUtcNow()
                };
                _inMemoryOptions.DeadLetterStore.Add(deadLetter);
            }
        }
    }

    public async Task ReleaseAsync(Guid messageId, CancellationToken token)
    {
        await _lock.WaitAsync(token);
        try
        {
            if (_messages.TryGetValue(messageId, out var message))
            {
                message.CapturedAt = null;
                message.CapturedBy = null;
            }
            // Note: Group lock is NOT released here - released via ReleaseGroupLocksAsync
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task ReleaseBatchAsync(IReadOnlyList<Guid> messageIds, CancellationToken token)
    {
        if (messageIds.Count == 0) return;

        await _lock.WaitAsync(token);
        try
        {
            foreach (var messageId in messageIds)
            {
                if (_messages.TryGetValue(messageId, out var message))
                {
                    message.CapturedAt = null;
                    message.CapturedBy = null;
                }
            }
            // Note: Group locks are NOT released here - released via ReleaseGroupLocksAsync
        }
        finally
        {
            _lock.Release();
        }
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

        await _lock.WaitAsync(token);
        try
        {
            foreach (var messageId in toComplete)
            {
                _messages.TryRemove(messageId, out _);
            }

            foreach (var messageId in toFail)
            {
                if (_messages.TryGetValue(messageId, out var message))
                {
                    message.AttemptsCount++;
                    message.CapturedAt = null;
                    message.CapturedBy = null;
                }
            }

            foreach (var messageId in toRelease)
            {
                if (_messages.TryGetValue(messageId, out var message))
                {
                    message.CapturedAt = null;
                    message.CapturedBy = null;
                }
            }

            foreach (var (messageId, reason) in toDeadLetter)
            {
                MoveToDeadLetterInternal(messageId, reason);
            }

            // Note: Group locks are NOT released here - released via ReleaseGroupLocksAsync
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task<IReadOnlyList<DeadLetterMessage>> ReadDeadLettersAsync(int count, CancellationToken token)
    {
        await _lock.WaitAsync(token);
        try
        {
            // Already sorted by MovedAt - no OrderBy needed!
            return _inMemoryOptions.DeadLetterStore.Read(count);
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task<InboxHealthMetrics> GetHealthMetricsAsync(CancellationToken token)
    {
        await _lock.WaitAsync(token);
        try
        {
            var pendingCount = 0;
            var capturedCount = 0;
            DateTime? oldestPendingAt = null;

            // Single pass through messages for all metrics
            // Collection is sorted by ReceivedAt, so first pending message is oldest
            foreach (var message in _messages)
            {
                if (message.CapturedAt == null)
                {
                    pendingCount++;
                    oldestPendingAt ??= message.ReceivedAt;
                }
                else
                {
                    capturedCount++;
                }
            }

            return new InboxHealthMetrics(
                pendingCount,
                capturedCount,
                _inMemoryOptions.DeadLetterStore.Count,
                oldestPendingAt);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Extends the locks for captured messages.
    /// Updates CapturedAt for messages and group lock timestamps.
    /// </summary>
    public async Task<int> ExtendLocksAsync(
        string processorId,
        IReadOnlyList<IInboxMessageIdentifiers> capturedMessages,
        DateTime newCapturedAt,
        CancellationToken token)
    {
        if (capturedMessages.Count == 0)
        {
            return 0;
        }

        await _lock.WaitAsync(token);
        try
        {
            var extended = 0;

            foreach (var capturedMessage in capturedMessages)
            {
                if (_messages.TryGetValue(capturedMessage.Id, out var message) &&
                    message.CapturedBy == processorId &&
                    message.CapturedAt != null)
                {
                    // Extend message lock
                    message.CapturedAt = newCapturedAt;
                    extended++;

                    // Extend group lock if applicable
                    if (!string.IsNullOrEmpty(message.GroupId) && _lockedGroups.ContainsKey(message.GroupId))
                    {
                        _lockedGroups[message.GroupId] = newCapturedAt;
                    }
                }
            }

            return extended;
        }
        finally
        {
            _lock.Release();
        }
    }

    #region ISupportGroupLocksReleaseStorageProvider

    public async Task ReleaseGroupLocksAsync(IReadOnlyList<string> groupIds, CancellationToken token)
    {
        if (groupIds.Count == 0) return;

        await _lock.WaitAsync(token);
        try
        {
            foreach (var groupId in groupIds)
            {
                _lockedGroups.Remove(groupId);
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task ReleaseMessagesAndGroupLocksAsync(IReadOnlyList<IInboxMessageIdentifiers> messages, CancellationToken token)
    {
        if (messages.Count == 0) return;

        var messageIds = messages.Select(m => m.Id).ToList();
        var groupIds = messages
            .Where(m => !string.IsNullOrEmpty(m.GroupId))
            .Select(m => m.GroupId!)
            .Distinct()
            .ToList();

        await _lock.WaitAsync(token);
        try
        {
            // Release messages
            foreach (var messageId in messageIds)
            {
                if (_messages.TryGetValue(messageId, out var message))
                {
                    message.CapturedAt = null;
                    message.CapturedBy = null;
                }
            }

            // Release group locks
            foreach (var groupId in groupIds)
            {
                _lockedGroups.Remove(groupId);
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    #endregion

    public void Dispose()
    {
        // Cleanup service is now managed by IInboxLifecycleHook
        _lock.Dispose();
        GC.SuppressFinalize(this);
    }
}