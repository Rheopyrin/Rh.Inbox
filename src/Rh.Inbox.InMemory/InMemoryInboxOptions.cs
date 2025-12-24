using Rh.Inbox.InMemory.Options;

namespace Rh.Inbox.InMemory;

/// <summary>
/// Configuration options for in-memory inbox storage.
/// </summary>
public class InMemoryInboxOptions
{
    /// <summary>
    /// Gets or sets the options for the dead letter cleanup task.
    /// </summary>
    public CleanupTaskOptions DeadLetterCleanup { get; set; } = new();

    /// <summary>
    /// Gets or sets the options for the deduplication cleanup task.
    /// </summary>
    public CleanupTaskOptions DeduplicationCleanup { get; set; } = new();
}