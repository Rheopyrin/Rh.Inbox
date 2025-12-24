using StackExchange.Redis;

namespace Rh.Inbox.Redis.Provider.Scripts;

internal static partial class RedisScripts
{
    /// <summary>
    /// Read and capture for Default/Batched modes (no group locking).
    /// Optimized: Uses ZRANGE, single HGETALL per message.
    /// Returns [id, hashData, id, hashData, ...].
    /// </summary>
    public static readonly LuaScript ReadDefault = LuaScript.Prepare($@"
        local results = {{}}
        local count = 0
        local maxCount = tonumber(@batchSize)
        local now = tonumber(@now)
        local expiredThreshold = tonumber(@expiredThreshold)
        local scanLimit = maxCount * {DefaultScanMultiplier}

        local pending = redis.call('ZRANGE', @pendingKey, 0, scanLimit - 1)

        for _, msgId in ipairs(pending) do
            if count >= maxCount then break end

            local msgKey = @msgKeyBase .. msgId
            local hashData = redis.call('HGETALL', msgKey)

            if #hashData > 0 then
                local capturedScore = redis.call('ZSCORE', @capturedKey, msgId)
                local canCapture = not capturedScore or tonumber(capturedScore) <= expiredThreshold

                if canCapture then
                    redis.call('ZADD', @capturedKey, now, msgId)
                    redis.call('HSET', msgKey, 'captured_at', @nowStr, 'captured_by', @processorId)

                    table.insert(results, msgId)
                    table.insert(results, hashData)
                    count = count + 1
                end
            else
                redis.call('ZREM', @pendingKey, msgId)
            end
        end
        return results
    ");

    /// <summary>
    /// Read and capture messages for FIFO/FifoBatched modes with group locking.
    /// </summary>
    /// <remarks>
    /// Group locking ensures strict FIFO ordering within each group:
    /// <list type="bullet">
    ///   <item><description>Each group has an individual lock key ({lockKeyBase}{groupId}) with TTL</description></item>
    ///   <item><description>O(1) lock check per group using EXISTS command</description></item>
    ///   <item><description>Multiple messages from the same group can be captured by the same worker</description></item>
    ///   <item><description>Other workers are blocked from capturing a locked group</description></item>
    ///   <item><description>Locks are set with TTL at the end of the script for all captured groups</description></item>
    /// </list>
    /// TTL equals MaxProcessingTime and handles crashed workers automatically.
    /// Returns [id, hashData, id, hashData, ...].
    /// </remarks>
    public static readonly LuaScript ReadFifo = LuaScript.Prepare($@"
        local results = {{}}
        local count = 0
        local maxCount = tonumber(@batchSize)
        local now = tonumber(@now)
        local expiredThreshold = tonumber(@expiredThreshold)
        local lockTtl = tonumber(@lockTtlSeconds)
        local scanLimit = maxCount * {FifoScanMultiplier}

        -- Track groups locked in this batch (to prevent same group twice)
        local groupsLockedThisBatch = {{}}

        local pending = redis.call('ZRANGE', @pendingKey, 0, scanLimit - 1)

        for _, msgId in ipairs(pending) do
            if count >= maxCount then break end

            local msgKey = @msgKeyBase .. msgId
            local hashData = redis.call('HGETALL', msgKey)

            if #hashData > 0 then
                -- Extract group_id from hash data
                local groupId = nil
                for i = 1, #hashData, 2 do
                    if hashData[i] == 'group_id' then
                        groupId = hashData[i + 1]
                        break
                    end
                end

                local capturedScore = redis.call('ZSCORE', @capturedKey, msgId)
                local canCapture = not capturedScore or tonumber(capturedScore) <= expiredThreshold

                if canCapture and groupId and groupId ~= '' then
                    -- O(1) lock check using individual key with TTL
                    -- Only check external lock (from other workers), not groupsLockedThisBatch
                    -- We WANT multiple messages from same group in one batch (processed sequentially)
                    if not groupsLockedThisBatch[groupId] then
                        local lockKey = @lockKeyBase .. groupId
                        if redis.call('EXISTS', lockKey) == 1 then
                            canCapture = false
                        end
                    end
                    -- If groupsLockedThisBatch[groupId] is true, we already own the lock, so canCapture stays true
                end

                if canCapture then
                    redis.call('ZADD', @capturedKey, now, msgId)
                    redis.call('HSET', msgKey, 'captured_at', @nowStr, 'captured_by', @processorId)

                    if groupId and groupId ~= '' then
                        groupsLockedThisBatch[groupId] = true
                    end

                    table.insert(results, msgId)
                    table.insert(results, hashData)
                    count = count + 1
                end
            else
                redis.call('ZREM', @pendingKey, msgId)
            end
        end

        -- Lock all captured groups with TTL (auto-expire, no cleanup needed!)
        for groupId in pairs(groupsLockedThisBatch) do
            local lockKey = @lockKeyBase .. groupId
            redis.call('SET', lockKey, @processorId, 'EX', lockTtl)
        end

        return results
    ");
}
