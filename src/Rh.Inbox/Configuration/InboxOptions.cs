using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Providers;

namespace Rh.Inbox.Configuration;

/// <summary>
/// Configuration options for an inbox.
/// </summary>
public sealed class InboxOptions : IInboxOptions
{
    /// <summary>
    /// The default inbox name used when no name is specified.
    /// </summary>
    public const string DefaultInboxName = "default";

    /// <summary>
    /// Gets or sets the name of the inbox.
    /// </summary>
    public string InboxName { get; init; } = DefaultInboxName;

    /// <summary>
    /// Gets or sets the number of messages to read in a single batch.
    /// </summary>
    public required int ReadBatchSize { get; init; }

    /// <summary>
    /// Gets or sets the number of messages to write in a single batch.
    /// </summary>
    public required int WriteBatchSize { get; init; }

    /// <summary>
    /// Gets or sets the maximum time allowed for processing a batch before messages are considered abandoned.
    /// </summary>
    public required TimeSpan MaxProcessingTime { get; init; }

    /// <summary>
    /// Gets or sets the interval between polling for new messages.
    /// </summary>
    public required TimeSpan PollingInterval { get; init; }

    /// <summary>
    /// Gets or sets the delay before reading the next batch after processing.
    /// </summary>
    public required TimeSpan ReadDelay { get; init; }

    /// <summary>
    /// Gets or sets the timeout for graceful shutdown.
    /// </summary>
    public required TimeSpan ShutdownTimeout { get; init; }

    /// <summary>
    /// Gets or sets the maximum number of processing attempts before a message is considered failed.
    /// </summary>
    public required int MaxAttempts { get; init; }

    /// <summary>
    /// Gets or sets whether to move failed messages to a dead letter queue after max attempts.
    /// </summary>
    public required bool EnableDeadLetter { get; init; }

    /// <summary>
    /// Gets or sets the maximum number of concurrent handler executions during message processing.
    /// </summary>
    public required int MaxProcessingThreads { get; init; }

    /// <summary>
    /// Gets or sets the maximum number of concurrent write operations when writing message batches.
    /// </summary>
    public required int MaxWriteThreads { get; init; }

    /// <summary>
    /// Gets or sets the time interval for which deduplication records are kept.
    /// When set to a positive value, enables deduplication tracking.
    /// </summary>
    public required TimeSpan DeduplicationInterval { get; init; }

    /// <summary>
    /// Gets or sets the date/time provider.
    /// </summary>
    public required IDateTimeProvider DateTimeProvider { get; init; }

    /// <summary>
    /// Gets or sets whether automatic lock extension is enabled during batch processing.
    /// When enabled, message and group locks are periodically extended to prevent
    /// expiration before processing completes.
    /// </summary>
    public required bool EnableLockExtension { get; init; }

    /// <summary>
    /// Gets or sets the threshold percentage of MaxProcessingTime after which locks are extended.
    /// Value must be between 0.1 and 0.9.
    /// For example, with MaxProcessingTime=5min and LockExtensionThreshold=0.5,
    /// locks will be extended every 2.5 minutes.
    /// </summary>
    public required double LockExtensionThreshold { get; init; }

    /// <summary>
    /// Gets or sets the maximum lifetime for dead letter messages.
    /// When set, dead letter messages older than this value will be automatically cleaned up.
    /// </summary>
    public required TimeSpan DeadLetterMaxMessageLifetime { get; init; }

    /// <summary>
    /// Gets or sets whether deduplication is enabled.
    /// When enabled, messages with the same DeduplicationId within the DeduplicationInterval will be skipped.
    /// </summary>
    public required bool EnableDeduplication { get; init; }
}