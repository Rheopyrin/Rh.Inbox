using StackExchange.Redis;

namespace Rh.Inbox.Redis.Provider.Scripts;

internal static partial class RedisScripts
{
    /// <summary>
    /// Get health metrics atomically.
    /// Returns [actualPending, capturedCount, deadLetterCount, oldestPendingScore].
    /// </summary>
    internal static readonly LuaScript GetHealthMetrics = LuaScript.Prepare(@"
        local expiredCaptureThreshold = tonumber(@expiredCaptureThreshold)

        -- Get counts
        local pendingCount = redis.call('ZCARD', @pendingKey)
        local capturedCount = redis.call('ZCOUNT', @capturedKey, expiredCaptureThreshold, '+inf')
        local deadLetterCount = 0

        if @enableDeadLetter == '1' then
            deadLetterCount = redis.call('ZCARD', @dlqKey)
        end

        -- Calculate actual pending (not captured or capture expired)
        local actualPending = pendingCount - capturedCount
        if actualPending < 0 then
            actualPending = 0
        end

        -- Get oldest pending message timestamp
        local oldestPendingScore = nil
        local oldest = redis.call('ZRANGE', @pendingKey, 0, 0, 'WITHSCORES')

        if #oldest >= 2 then
            local oldestId = oldest[1]
            local oldestScore = oldest[2]

            -- Check if this message is actually pending (not captured or capture expired)
            local capturedScore = redis.call('ZSCORE', @capturedKey, oldestId)
            if not capturedScore or tonumber(capturedScore) <= expiredCaptureThreshold then
                oldestPendingScore = oldestScore
            end
        end

        return {actualPending, capturedCount, deadLetterCount, oldestPendingScore}
    ");
}