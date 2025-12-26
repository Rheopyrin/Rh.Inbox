using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Providers;
using Rh.Inbox.Resilience;

namespace Rh.Inbox.Redis.Options;

/// <summary>
/// Configuration options for Redis inbox storage provider.
/// </summary>
/// <remarks>
/// Redis storage uses sorted sets for message ordering, hashes for message data,
/// and individual keys with TTL for FIFO group locks.
///
/// Key structure:
/// <list type="bullet">
///   <item><description>{prefix}:pending - Sorted set of pending message IDs (score = timestamp)</description></item>
///   <item><description>{prefix}:captured - Sorted set of captured message IDs (score = capture time)</description></item>
///   <item><description>{prefix}:msg:{id} - Hash containing message data</description></item>
///   <item><description>{prefix}:collapse - Hash mapping collapse keys to message IDs</description></item>
///   <item><description>{prefix}:dedup:{id} - String with TTL for deduplication</description></item>
///   <item><description>{prefix}:lock:{groupId} - String with TTL for FIFO group locks</description></item>
///   <item><description>{prefix}:dlq - Sorted set for dead letter queue</description></item>
///   <item><description>{prefix}:dlq:{id} - Hash containing dead letter message data</description></item>
/// </list>
/// </remarks>
public class RedisInboxOptions
{
    /// <summary>
    /// The default key prefix for Redis keys.
    /// </summary>
    public const string DefaultKeyPrefix = "inbox";

    /// <summary>
    /// Redis connection string (required).
    /// </summary>
    public required string ConnectionString { get; init; }

    /// <summary>
    /// Key prefix for Redis keys. If null, will be auto-generated from inbox name.
    /// </summary>
    public string? KeyPrefix { get; set; }

    /// <summary>
    /// Maximum lifetime for messages in the inbox.
    /// Messages (including pending, captured) older than this will have TTL set.
    /// Default is 24 hours.
    /// </summary>
    public TimeSpan MaxMessageLifetime { get; set; } = TimeSpan.FromHours(24);

    /// <summary>
    /// Gets or sets the retry options for transient storage failures.
    /// Default enables 3 retries with exponential backoff.
    /// Set to <see cref="RetryOptions.None"/> to disable retries.
    /// </summary>
    public RetryOptions Retry { get; set; } = RetryOptions.Default;
}