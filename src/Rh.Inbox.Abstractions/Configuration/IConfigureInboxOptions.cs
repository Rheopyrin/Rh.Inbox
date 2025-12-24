using Rh.Inbox.Abstractions.Providers;

namespace Rh.Inbox.Abstractions.Configuration;

/// <summary>
/// Interface for configuring inbox options during builder configuration.
/// </summary>
public interface IConfigureInboxOptions
{
    /// <summary>
    /// Gets or sets the number of messages to read in a single batch.
    /// </summary>
    int ReadBatchSize { get; set; }

    /// <summary>
    /// Gets or sets the number of messages to write in a single batch.
    /// </summary>
    int WriteBatchSize { get; set; }

    /// <summary>
    /// Gets or sets the maximum time allowed for processing a batch before messages are considered abandoned.
    /// </summary>
    TimeSpan MaxProcessingTime { get; set; }

    /// <summary>
    /// Gets or sets the interval between polling for new messages.
    /// </summary>
    TimeSpan PollingInterval { get; set; }

    /// <summary>
    /// Gets or sets the delay before reading the next batch after processing.
    /// </summary>
    TimeSpan ReadDelay { get; set; }

    /// <summary>
    /// Gets or sets the timeout for graceful shutdown.
    /// </summary>
    TimeSpan ShutdownTimeout { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of processing attempts before a message is considered failed.
    /// </summary>
    int MaxAttempts { get; set; }

    /// <summary>
    /// Gets or sets whether to move failed messages to a dead letter queue after max attempts.
    /// </summary>
    bool EnableDeadLetter { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of concurrent handler executions during message processing.
    /// </summary>
    int MaxProcessingThreads { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of concurrent write operations when writing message batches.
    /// </summary>
    int MaxWriteThreads { get; set; }

    /// <summary>
    /// Gets or sets the time interval for which deduplication records are kept.
    /// </summary>
    TimeSpan DeduplicationInterval { get; set; }

    /// <summary>
    /// Gets or sets the date/time provider.
    /// </summary>
    IDateTimeProvider DateTimeProvider { get; set; }

    /// <summary>
    /// Gets or sets whether automatic lock extension is enabled during batch processing.
    /// When enabled, message and group locks are periodically extended to prevent
    /// expiration before processing completes. Default is false (disabled).
    /// </summary>
    bool EnableLockExtension { get; set; }

    /// <summary>
    /// Gets or sets the threshold percentage of MaxProcessingTime after which locks are extended.
    /// Value must be between 0.1 and 0.9. Default is 0.5 (50%).
    /// For example, with MaxProcessingTime=5min and LockExtensionThreshold=0.5,
    /// locks will be extended every 2.5 minutes.
    /// </summary>
    double LockExtensionThreshold { get; set; }

    /// <summary>
    /// Gets or sets the maximum lifetime for dead letter messages.
    /// Only used when <see cref="EnableDeadLetter"/> is true and value is greater than zero.
    /// When set to a positive value, dead letter messages older than this value will be automatically cleaned up.
    /// Default is zero (no automatic cleanup).
    /// </summary>
    TimeSpan DeadLetterMaxMessageLifetime { get; set; }

    /// <summary>
    /// Gets or sets whether deduplication is enabled.
    /// When enabled, messages with the same DeduplicationId within the DeduplicationInterval will be skipped.
    /// Default is false (disabled).
    /// </summary>
    bool EnableDeduplication { get; set; }
}