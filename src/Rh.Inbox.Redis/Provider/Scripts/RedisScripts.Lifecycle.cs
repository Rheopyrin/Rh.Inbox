namespace Rh.Inbox.Redis.Provider.Scripts;

internal static partial class RedisScripts
{
    /// <summary>
    /// Complete messages - remove from pending/captured sets, cleanup collapse index, delete message hash.
    /// </summary>
    /// <remarks>
    /// Group locks are NOT released here. For FIFO processing, call ReleaseGroupLocks explicitly
    /// after all messages from a group are processed, or rely on TTL expiration as a fallback.
    /// ARGV: [pendingKey, capturedKey, collapseKey, msgKeyBase, ...ids]
    /// Returns count of processed messages.
    /// </remarks>
    internal const string ProcessComplete = @"
        local pendingKey, capturedKey, collapseKey, msgKeyBase = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
        local processed = 0

        for i = 5, #ARGV do
            local id = ARGV[i]
            local msgKey = msgKeyBase .. id

            local collapseKeyValue = redis.call('HGET', msgKey, 'collapse_key')

            if collapseKeyValue and collapseKeyValue ~= '' then
                local indexed = redis.call('HGET', collapseKey, collapseKeyValue)
                if indexed == id then
                    redis.call('HDEL', collapseKey, collapseKeyValue)
                end
            end

            redis.call('ZREM', pendingKey, id)
            redis.call('ZREM', capturedKey, id)
            redis.call('DEL', msgKey)
            processed = processed + 1
        end
        return processed
    ";

    /// <summary>
    /// Fail messages - increment attempts counter, clear capture fields, refresh message TTL.
    /// </summary>
    /// <remarks>
    /// Group locks are NOT released here. For FIFO processing, call ReleaseGroupLocks explicitly
    /// after all messages from a group are processed, or rely on TTL expiration as a fallback.
    /// ARGV: [capturedKey, msgKeyBase, ttlSeconds, ...ids]
    /// Returns count of processed messages.
    /// </remarks>
    internal const string ProcessFail = @"
        local capturedKey, msgKeyBase = ARGV[1], ARGV[2]
        local ttl = tonumber(ARGV[3])
        local processed = 0

        for i = 4, #ARGV do
            local id = ARGV[i]
            local msgKey = msgKeyBase .. id

            redis.call('HINCRBY', msgKey, 'attempts_count', 1)
            redis.call('HDEL', msgKey, 'captured_at', 'captured_by')
            redis.call('ZREM', capturedKey, id)
            redis.call('EXPIRE', msgKey, ttl)
            processed = processed + 1
        end
        return processed
    ";

    /// <summary>
    /// Release messages - clear capture fields without incrementing attempts or refreshing TTL.
    /// </summary>
    /// <remarks>
    /// Use this for transient failures where the message should be retried immediately.
    /// Group locks are NOT released here. For FIFO processing, call ReleaseGroupLocks explicitly
    /// after all messages from a group are processed, or rely on TTL expiration as a fallback.
    /// ARGV: [capturedKey, msgKeyBase, ...ids]
    /// Returns count of processed messages.
    /// </remarks>
    internal const string ProcessRelease = @"
        local capturedKey, msgKeyBase = ARGV[1], ARGV[2]
        local processed = 0

        for i = 3, #ARGV do
            local id = ARGV[i]
            local msgKey = msgKeyBase .. id

            redis.call('HDEL', msgKey, 'captured_at', 'captured_by')
            redis.call('ZREM', capturedKey, id)
            processed = processed + 1
        end
        return processed
    ";

    /// <summary>
    /// Move messages to dead letter queue (or delete if DLQ is disabled).
    /// </summary>
    /// <remarks>
    /// If EnableDeadLetter is true, copies message data to DLQ hash with failure reason.
    /// If EnableDeadLetter is false, simply deletes the message.
    /// Group locks are NOT released here. For FIFO processing, call ReleaseGroupLocks explicitly
    /// after all messages from a group are processed, or rely on TTL expiration as a fallback.
    /// ARGV: [pendingKey, capturedKey, collapseKey, dlqKey, msgKeyBase, dlqKeyBase,
    ///        ttlSeconds, enableDlq, movedAt, movedAtScore, ...(id, reason) pairs]
    /// Returns count of processed messages.
    /// </remarks>
    internal const string ProcessDeadLetter = @"
        local pendingKey, capturedKey, collapseKey = ARGV[1], ARGV[2], ARGV[3]
        local dlqKey, msgKeyBase, dlqKeyBase = ARGV[4], ARGV[5], ARGV[6]
        local ttl = tonumber(ARGV[7])
        local enableDlq = ARGV[8]
        local movedAt, movedAtScore = ARGV[9], ARGV[10]
        local processed = 0

        for i = 11, #ARGV, 2 do
            local id = ARGV[i]
            local reason = ARGV[i + 1]
            local msgKey = msgKeyBase .. id

            local hashData = redis.call('HGETALL', msgKey)
            if #hashData > 0 then
                local collapseKeyValue = nil
                for j = 1, #hashData, 2 do
                    if hashData[j] == 'collapse_key' then
                        collapseKeyValue = hashData[j + 1]
                        break
                    end
                end

                if collapseKeyValue and collapseKeyValue ~= '' then
                    local indexed = redis.call('HGET', collapseKey, collapseKeyValue)
                    if indexed == id then
                        redis.call('HDEL', collapseKey, collapseKeyValue)
                    end
                end

                if enableDlq == '1' then
                    local dlqMsgKey = dlqKeyBase .. id
                    redis.call('HSET', dlqMsgKey, unpack(hashData))
                    redis.call('HSET', dlqMsgKey, 'failure_reason', reason, 'moved_at', movedAt)
                    redis.call('EXPIRE', dlqMsgKey, ttl)
                    redis.call('ZADD', dlqKey, movedAtScore, id)
                end

                redis.call('ZREM', pendingKey, id)
                redis.call('ZREM', capturedKey, id)
                redis.call('DEL', msgKey)
                processed = processed + 1
            end
        end
        return processed
    ";

    /// <summary>
    /// Release group locks explicitly for FIFO processing.
    /// </summary>
    /// <remarks>
    /// Call this after all messages from a group have been processed (completed, failed, or dead-lettered).
    /// This allows other workers to immediately capture and process new messages from these groups.
    /// If not called, locks will expire automatically after MaxProcessingTime (TTL).
    /// This operation is idempotent - releasing already-released locks is safe.
    /// ARGV: [lockKeyBase, ...groupIds]
    /// Returns count of released locks (0 if locks were already released or expired).
    /// </remarks>
    internal const string ReleaseGroupLocks = @"
        local lockKeyBase = ARGV[1]
        local released = 0

        for i = 2, #ARGV do
            local groupId = ARGV[i]
            local lockKey = lockKeyBase .. groupId
            released = released + redis.call('DEL', lockKey)
        end
        return released
    ";

    /// <summary>
    /// Extend message locks by updating captured_at timestamp and refreshing captured sorted set score.
    /// </summary>
    /// <remarks>
    /// Only extends locks for messages that are still captured by the specified processor.
    /// ARGV: [capturedKey, msgKeyBase, processorId, newCapturedAtStr, newCapturedAtScore, ...ids]
    /// Returns count of extended message locks.
    /// </remarks>
    internal const string ExtendMessageLocks = @"
        local capturedKey, msgKeyBase = ARGV[1], ARGV[2]
        local processorId = ARGV[3]
        local newCapturedAtStr = ARGV[4]
        local newCapturedAtScore = tonumber(ARGV[5])
        local extended = 0

        for i = 6, #ARGV do
            local id = ARGV[i]
            local msgKey = msgKeyBase .. id
            local owner = redis.call('HGET', msgKey, 'captured_by')
            if owner == processorId then
                redis.call('HSET', msgKey, 'captured_at', newCapturedAtStr)
                redis.call('ZADD', capturedKey, newCapturedAtScore, id)
                extended = extended + 1
            end
        end
        return extended
    ";

    /// <summary>
    /// Extend group locks by refreshing their TTL.
    /// </summary>
    /// <remarks>
    /// Only extends locks owned by the specified processor (SET NX value check).
    /// ARGV: [lockKeyBase, processorId, lockTtlSeconds, ...groupIds]
    /// Returns count of extended group locks.
    /// </remarks>
    internal const string ExtendGroupLocks = @"
        local lockKeyBase = ARGV[1]
        local processorId = ARGV[2]
        local lockTtl = tonumber(ARGV[3])
        local extended = 0

        for i = 4, #ARGV do
            local groupId = ARGV[i]
            local lockKey = lockKeyBase .. groupId
            local owner = redis.call('GET', lockKey)
            if owner == processorId then
                redis.call('EXPIRE', lockKey, lockTtl)
                extended = extended + 1
            end
        end
        return extended
    ";
}