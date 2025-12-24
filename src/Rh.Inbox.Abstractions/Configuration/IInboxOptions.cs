namespace Rh.Inbox.Abstractions.Configuration;

/// <summary>
/// Read-only interface for accessing inbox configuration options.
/// </summary>
public interface IInboxOptions
{
    /// <summary>
    /// Gets or sets the name of the inbox.
    /// </summary>
    string InboxName { get; }

    /// <summary>
    /// Gets or sets the number of messages to read in a single batch. Default is 100.
    /// </summary>
    int ReadBatchSize { get; }

    /// <summary>
    /// Gets or sets the number of messages to write in a single batch. Default is 100.
    /// </summary>
    int WriteBatchSize { get; }

    /// <summary>
    /// Gets or sets the maximum time allowed for processing a batch before messages are considered abandoned. Default is 5 minutes.
    /// </summary>
    TimeSpan MaxProcessingTime { get; }

    /// <summary>
    /// Gets or sets the interval between polling for new messages. Default is 5 seconds.
    /// </summary>
    TimeSpan PollingInterval { get; }

    /// <summary>
    /// Gets or sets the delay before reading the next batch after processing. Default is zero.
    /// </summary>
    TimeSpan ReadDelay { get; }

    /// <summary>
    /// Gets or sets the timeout for graceful shutdown. Default is 30 seconds.
    /// </summary>
    TimeSpan ShutdownTimeout { get; }

    /// <summary>
    /// Gets or sets the maximum number of processing attempts before a message is considered failed. Default is 3.
    /// </summary>
    int MaxAttempts { get; }

    /// <summary>
    /// Gets or sets whether to move failed messages to a dead letter queue after max attempts. Default is false.
    /// </summary>
    bool EnableDeadLetter { get; }

    /// <summary>
    /// Gets or sets the maximum number of concurrent handler executions during message processing.
    /// Default is 1 (sequential processing). Set higher for parallel processing.
    /// </summary>
    int MaxProcessingThreads { get; }

    /// <summary>
    /// Gets or sets the maximum number of concurrent write operations when writing message batches.
    /// Default is 1 (sequential writes). Set higher to parallelize chunk writes for large batches.
    /// </summary>
    int MaxWriteThreads { get; }

    /// <summary>
    /// Time interval for which deduplication records are kept.
    /// When set to a positive value, enables deduplication tracking in a separate table.
    /// Messages with the same DeduplicationId within this interval will be skipped.
    /// </summary>
    TimeSpan DeduplicationInterval { get; }

    /// <summary>
    /// Gets whether deduplication is enabled.
    /// When enabled, messages with the same DeduplicationId within the DeduplicationInterval will be skipped.
    /// Default is false (disabled).
    /// </summary>
    bool EnableDeduplication { get; }

    /// <summary>
    /// Gets whether automatic lock extension is enabled during batch processing.
    /// When enabled, message and group locks are periodically extended to prevent
    /// expiration before processing completes. Default is false (disabled).
    /// </summary>
    bool EnableLockExtension { get; }

    /// <summary>
    /// Gets the threshold percentage of MaxProcessingTime after which locks are extended.
    /// Value must be between 0.1 and 0.9. Default is 0.5 (50%).
    /// For example, with MaxProcessingTime=5min and LockExtensionThreshold=0.5,
    /// locks will be extended every 2.5 minutes.
    /// </summary>
    double LockExtensionThreshold { get; }

    /// <summary>
    /// Gets the maximum lifetime for dead letter messages.
    /// Only used when <see cref="EnableDeadLetter"/> is true and value is greater than zero.
    /// When set to a positive value, dead letter messages older than this value will be automatically cleaned up.
    /// Default is zero (no automatic cleanup).
    /// </summary>
    TimeSpan DeadLetterMaxMessageLifetime { get; }
}