namespace Rh.Inbox.InMemory.Options;

/// <summary>
/// Resolved options passed to the storage provider. All values are non-null.
/// </summary>
internal sealed class InMemoryInboxProviderOptions
{
    /// <summary>
    /// Per-inbox deduplication store.
    /// </summary>
    public required InMemoryDeduplicationStore DeduplicationStore { get; init; }

    /// <summary>
    /// Per-inbox dead letter store.
    /// </summary>
    public required InMemoryDeadLetterStore DeadLetterStore { get; init; }
}