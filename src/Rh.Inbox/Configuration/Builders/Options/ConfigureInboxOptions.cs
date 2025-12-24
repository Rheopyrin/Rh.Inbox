using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Providers;

namespace Rh.Inbox.Configuration.Builders.Options;

public class ConfigureInboxOptions : IConfigureInboxOptions
{
    /// <summary>
    /// Gets or sets the number of messages to read in a single batch. Default is 100.
    /// </summary>
    public int ReadBatchSize { get; set; } = 100;

    /// <summary>
    /// Gets or sets the number of messages to write in a single batch. Default is 100.
    /// </summary>
    public int WriteBatchSize { get; set; } = 100;

    /// <summary>
    /// Gets or sets the maximum time allowed for processing a batch before messages are considered abandoned. Default is 5 minutes.
    /// </summary>
    public TimeSpan MaxProcessingTime { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Gets or sets the interval between polling for new messages. Default is 5 seconds.
    /// </summary>
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets the delay before reading the next batch after processing. Default is zero.
    /// </summary>
    public TimeSpan ReadDelay { get; set; } = TimeSpan.Zero;

    /// <summary>
    /// Gets or sets the timeout for graceful shutdown. Default is 30 seconds.
    /// </summary>
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the maximum number of processing attempts before a message is considered failed. Default is 3.
    /// </summary>
    public int MaxAttempts { get; set; } = 3;

    /// <summary>
    /// Gets or sets whether to move failed messages to a dead letter queue after max attempts. Default is false.
    /// </summary>
    public bool EnableDeadLetter { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of concurrent handler executions during message processing.
    /// Default is 1 (sequential processing). Set higher for parallel processing.
    /// </summary>
    public int MaxProcessingThreads { get; set; } = 1;

    /// <summary>
    /// Gets or sets the maximum number of concurrent write operations when writing message batches.
    /// Default is 1 (sequential writes). Set higher to parallelize chunk writes for large batches.
    /// </summary>
    public int MaxWriteThreads { get; set; } = 1;

    /// <summary>
    /// Gets or sets whether deduplication is enabled.
    /// When enabled, messages with the same DeduplicationId within the DeduplicationInterval will be skipped.
    /// Default is false (disabled).
    /// </summary>
    public bool EnableDeduplication { get; set; }

    /// <summary>
    /// Gets or sets the time interval for which deduplication records are kept.
    /// Only used when <see cref="EnableDeduplication"/> is true.
    /// Messages with the same DeduplicationId within this interval will be skipped.
    /// Default is zero (no automatic cleanup of deduplication records).
    /// </summary>
    public TimeSpan DeduplicationInterval { get; set; } = TimeSpan.Zero;

    /// <summary>
    /// Gets or sets the date/time provider used for timestamping operations.
    /// Default is <see cref="Providers.DateTimeProvider"/>.
    /// </summary>
    public IDateTimeProvider DateTimeProvider { get; set; } = new DateTimeProvider();

    /// <summary>
    /// Gets or sets whether automatic lock extension is enabled during batch processing.
    /// When enabled, message and group locks are periodically extended to prevent
    /// expiration before processing completes. Default is false (disabled).
    /// </summary>
    public bool EnableLockExtension { get; set; }

    /// <summary>
    /// Gets or sets the threshold percentage of MaxProcessingTime after which locks are extended.
    /// Value must be between 0.1 and 0.9. Default is 0.5 (50%).
    /// For example, with MaxProcessingTime=5min and LockExtensionThreshold=0.5,
    /// locks will be extended every 2.5 minutes.
    /// </summary>
    public double LockExtensionThreshold { get; set; } = 0.5;

    /// <summary>
    /// Gets or sets the maximum lifetime for dead letter messages.
    /// Only used when <see cref="EnableDeadLetter"/> is true and value is greater than zero.
    /// When set to a positive value, dead letter messages older than this value will be automatically cleaned up.
    /// Default is zero (no automatic cleanup).
    /// </summary>
    public TimeSpan DeadLetterMaxMessageLifetime { get; set; } = TimeSpan.Zero;
}