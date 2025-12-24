using Rh.Inbox.Abstractions.Messages;

namespace Rh.Inbox.Abstractions.Storage;

/// <summary>
/// Storage provider that supports explicit group lock release for FIFO processing.
/// Implemented by FIFO-capable storage providers (Redis, PostgreSQL).
/// </summary>
/// <remarks>
/// In FIFO mode, group locks ensure only one worker processes messages from a group at a time.
/// Locks are acquired when messages are captured via <see cref="IInboxStorageProvider.ReadAndCaptureAsync"/>.
///
/// Locks can be released in two ways:
/// <list type="bullet">
///   <item><description>Explicitly via <see cref="ReleaseGroupLocksAsync"/> - call after all messages from a group are processed</description></item>
///   <item><description>Automatically via TTL expiration (based on MaxProcessingTime option) - safety fallback</description></item>
/// </list>
///
/// The processing loop should track which groups have been fully processed and release them
/// explicitly for optimal performance. TTL expiration serves as a safety net for crashed workers.
/// </remarks>
public interface ISupportGroupLocksReleaseStorageProvider : IInboxStorageProvider
{
    /// <summary>
    /// Releases group locks after all messages from these groups have been processed.
    /// This allows other workers to immediately process new messages from these groups.
    /// </summary>
    /// <remarks>
    /// Call this method when all messages from a group have been processed (completed, failed, or moved to DLQ).
    /// If not called, locks will expire automatically after MaxProcessingTime.
    ///
    /// This method is idempotent - calling it for already-released groups is safe.
    /// </remarks>
    /// <param name="groupIds">The group IDs whose locks should be released.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ReleaseGroupLocksAsync(IReadOnlyList<string> groupIds, CancellationToken token = default);

    /// <summary>
    /// Releases captured messages and their group locks in a single optimized operation.
    /// </summary>
    /// <remarks>
    /// This method combines <see cref="IInboxStorageProvider.ReleaseBatchAsync"/> and <see cref="ReleaseGroupLocksAsync"/>
    /// into a single call for better performance:
    /// <list type="bullet">
    ///   <item><description>PostgreSQL: Uses a single connection/transaction</description></item>
    ///   <item><description>Redis: Uses a single batch/pipeline</description></item>
    /// </list>
    /// Use this method during graceful shutdown to release in-flight messages and their group locks atomically.
    /// The provider extracts message IDs and distinct group IDs from the provided messages internally.
    /// </remarks>
    /// <param name="messages">The messages to release. Provider extracts IDs and group IDs internally.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ReleaseMessagesAndGroupLocksAsync(IReadOnlyList<IInboxMessageIdentifiers> messages, CancellationToken token = default);
}
