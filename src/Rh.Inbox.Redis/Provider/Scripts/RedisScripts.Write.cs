using StackExchange.Redis;

namespace Rh.Inbox.Redis.Provider.Scripts;

internal static partial class RedisScripts
{
    /// <summary>
    /// Write single message with optional deduplication and collapse key support.
    /// Returns 1 if inserted, 0 if deduplicated.
    /// </summary>
    internal  static readonly LuaScript Write = LuaScript.Prepare(@"
        -- 1. Deduplication check (if dedupKey provided)
        if @dedupKey ~= '' then
            local set = redis.call('SET', @dedupKey, '1', 'NX', 'EX', @dedupTtlSeconds)
            if not set then return 0 end
        end

        -- 2. Collapse key handling (if collapseKeyValue provided)
        if @collapseKeyValue ~= '' then
            local existingId = redis.call('HGET', @collapseKey, @collapseKeyValue)
            if existingId then
                local existingMsgKey = @msgKeyBase .. existingId
                -- Only remove if not captured (uncaptured message)
                local capturedScore = redis.call('ZSCORE', @capturedKey, existingId)
                if not capturedScore then
                    redis.call('ZREM', @pendingKey, existingId)
                    redis.call('DEL', existingMsgKey)
                end
            end
            redis.call('HSET', @collapseKey, @collapseKeyValue, @id)
            redis.call('EXPIRE', @collapseKey, @ttlSeconds)
        end

        -- 3. Store message
        redis.call('HSET', @messageKey,
            'id', @id,
            'inbox_name', @inboxName,
            'message_type', @messageType,
            'payload', @payload,
            'group_id', @groupId,
            'collapse_key', @collapseKeyValue,
            'deduplication_id', @deduplicationId,
            'attempts_count', @attemptsCount,
            'received_at', @receivedAt)
        redis.call('EXPIRE', @messageKey, @ttlSeconds)
        redis.call('ZADD', @pendingKey, @receivedAtScore, @id)
        return 1
    ");

    /// <summary>
    /// Batch write for simple messages (no collapse key).
    /// ARGV layout: [pendingKey, msgKeyBase, inboxName, ttlSeconds, dedupTtlSeconds,
    ///               ...per message: id, messageType, payload, groupId, dedupId, attempts, receivedAt, score, dedupKey]
    /// Returns count of messages inserted.
    /// </summary>
    internal  const string WriteBatch = @"
        local pendingKey, msgKeyBase, inboxName = ARGV[1], ARGV[2], ARGV[3]
        local ttl, dedupTtl = tonumber(ARGV[4]), tonumber(ARGV[5])
        local inserted = 0
        local headerSize = 5
        local fieldsPerMessage = 9

        for i = headerSize + 1, #ARGV, fieldsPerMessage do
            local id = ARGV[i]
            local msgType = ARGV[i + 1]
            local payload = ARGV[i + 2]
            local groupId = ARGV[i + 3]
            local dedupId = ARGV[i + 4]
            local attempts = ARGV[i + 5]
            local receivedAt = ARGV[i + 6]
            local score = ARGV[i + 7]
            local dedupKey = ARGV[i + 8]

            -- Dedup check using SET NX (atomic set-if-not-exists)
            local shouldInsert = true
            if dedupKey ~= '' then
                local set = redis.call('SET', dedupKey, '1', 'NX', 'EX', dedupTtl)
                if not set then
                    shouldInsert = false
                end
            end

            if shouldInsert then
                local msgKey = msgKeyBase .. id
                redis.call('HSET', msgKey,
                    'id', id,
                    'inbox_name', inboxName,
                    'message_type', msgType,
                    'payload', payload,
                    'group_id', groupId,
                    'collapse_key', '',
                    'deduplication_id', dedupId,
                    'attempts_count', attempts,
                    'received_at', receivedAt)
                redis.call('EXPIRE', msgKey, ttl)
                redis.call('ZADD', pendingKey, score, id)
                inserted = inserted + 1
            end
        end
        return inserted
    ";

    /// <summary>
    /// Batch write for messages with collapse keys. Handles collapse key semantics atomically.
    /// ARGV layout: [pendingKey, capturedKey, collapseKey, msgKeyBase, inboxName, ttlSeconds, dedupTtlSeconds,
    ///               ...per message: id, messageType, payload, groupId, collapseKeyValue, dedupId, attempts, receivedAt, score, dedupKey]
    /// Returns count of messages inserted.
    /// </summary>
    internal  const string WriteBatchWithCollapse = @"
        local pendingKey, capturedKey, collapseKey = ARGV[1], ARGV[2], ARGV[3]
        local msgKeyBase, inboxName = ARGV[4], ARGV[5]
        local ttl, dedupTtl = tonumber(ARGV[6]), tonumber(ARGV[7])
        local inserted = 0
        local headerSize = 7
        local fieldsPerMessage = 10

        for i = headerSize + 1, #ARGV, fieldsPerMessage do
            local id = ARGV[i]
            local msgType = ARGV[i + 1]
            local payload = ARGV[i + 2]
            local groupId = ARGV[i + 3]
            local collapseKeyValue = ARGV[i + 4]
            local dedupId = ARGV[i + 5]
            local attempts = ARGV[i + 6]
            local receivedAt = ARGV[i + 7]
            local score = ARGV[i + 8]
            local dedupKey = ARGV[i + 9]

            -- Dedup check using SET NX (atomic set-if-not-exists)
            local shouldInsert = true
            if dedupKey ~= '' then
                local set = redis.call('SET', dedupKey, '1', 'NX', 'EX', dedupTtl)
                if not set then
                    shouldInsert = false
                end
            end

            if shouldInsert then
                -- Collapse key handling (if collapseKeyValue provided)
                if collapseKeyValue ~= '' then
                    local existingId = redis.call('HGET', collapseKey, collapseKeyValue)
                    if existingId then
                        local existingMsgKey = msgKeyBase .. existingId
                        -- Only remove if not captured (uncaptured message)
                        local capturedScore = redis.call('ZSCORE', capturedKey, existingId)
                        if not capturedScore then
                            redis.call('ZREM', pendingKey, existingId)
                            redis.call('DEL', existingMsgKey)
                        end
                    end
                    redis.call('HSET', collapseKey, collapseKeyValue, id)
                    redis.call('EXPIRE', collapseKey, ttl)
                end

                -- Store message
                local msgKey = msgKeyBase .. id
                redis.call('HSET', msgKey,
                    'id', id,
                    'inbox_name', inboxName,
                    'message_type', msgType,
                    'payload', payload,
                    'group_id', groupId,
                    'collapse_key', collapseKeyValue,
                    'deduplication_id', dedupId,
                    'attempts_count', attempts,
                    'received_at', receivedAt)
                redis.call('EXPIRE', msgKey, ttl)
                redis.call('ZADD', pendingKey, score, id)
                inserted = inserted + 1
            end
        end
        return inserted
    ";
}