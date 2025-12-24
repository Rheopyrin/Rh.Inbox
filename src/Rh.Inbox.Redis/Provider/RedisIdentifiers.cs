namespace Rh.Inbox.Redis.Provider;

/// <summary>
/// Provides Redis key identifiers for an inbox.
/// All keys use a hash tag for Redis Cluster slot co-location.
/// </summary>
internal sealed class RedisIdentifiers
{
    // Key suffixes
    private const string MsgKeySuffix = ":msg:";
    private const string DlqMsgKeySuffix = ":dlq:msg:";

    /// <summary>
    /// Hash tag prefix for all keys (ensures cluster slot co-location).
    /// </summary>
    public string HashTag { get; }

    /// <summary>
    /// Pre-computed message key base for scripts.
    /// </summary>
    public string MsgKeyBase { get; }

    /// <summary>
    /// Pre-computed dead letter key base for scripts.
    /// </summary>
    public string DlqKeyBase { get; }

    /// <summary>
    /// Pending messages sorted set key.
    /// </summary>
    public string PendingKey { get; }

    /// <summary>
    /// Captured messages sorted set key.
    /// </summary>
    public string CapturedKey { get; }

    /// <summary>
    /// Lock key base for FIFO group locks (individual keys with TTL).
    /// Format: {hashtag}:lock:{groupId}
    /// </summary>
    public string LockKeyBase { get; }

    /// <summary>
    /// Collapse key index hash key.
    /// </summary>
    public string CollapseKey { get; }

    /// <summary>
    /// Dead letter queue sorted set key.
    /// </summary>
    public string DeadLetterKey { get; }

    public RedisIdentifiers(string keyPrefix)
    {
        HashTag = $"{{{keyPrefix}}}";
        MsgKeyBase = $"{HashTag}{MsgKeySuffix}";
        DlqKeyBase = $"{HashTag}{DlqMsgKeySuffix}";
        PendingKey = $"{HashTag}:pending";
        CapturedKey = $"{HashTag}:captured";
        LockKeyBase = $"{HashTag}:lock:";
        CollapseKey = $"{HashTag}:collapse";
        DeadLetterKey = $"{HashTag}:dlq";
    }

    /// <summary>
    /// Gets the message hash key for a specific message ID.
    /// </summary>
    public string MessageKey(Guid id) => $"{MsgKeyBase}{id}";

    /// <summary>
    /// Gets the dead letter message hash key for a specific message ID.
    /// </summary>
    public string DeadLetterMessageKey(Guid id) => $"{DlqKeyBase}{id}";

    /// <summary>
    /// Gets the deduplication key for a specific deduplication ID.
    /// </summary>
    public string DeduplicationKey(string deduplicationId) =>
        $"{HashTag}:dedup:{SanitizeKeySegment(deduplicationId)}";

    /// <summary>
    /// Gets the group lock key for FIFO processing.
    /// Uses individual keys with TTL for automatic expiration.
    /// </summary>
    public string GroupLockKey(string groupId) =>
        $"{LockKeyBase}{SanitizeKeySegment(groupId)}";

    /// <summary>
    /// Sanitizes a key segment to prevent hash tag override in Redis Cluster.
    /// Replaces curly braces with safe characters.
    /// </summary>
    private static string SanitizeKeySegment(string segment)
    {
        if (!segment.Contains('{') && !segment.Contains('}'))
            return segment;
        return segment.Replace("{", "(").Replace("}", ")");
    }
}
