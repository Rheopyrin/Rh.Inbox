using System.Collections.Concurrent;

namespace Rh.Inbox.InMemory;

/// <summary>
/// Thread-safe storage for deduplication records for a single inbox.
/// </summary>
internal sealed class InMemoryDeduplicationStore
{
    private readonly ConcurrentDictionary<string, DateTime> _records = new();

    /// <summary>
    /// Checks if a deduplication record exists and is not expired.
    /// </summary>
    public bool Exists(string deduplicationId, DateTime expirationTime)
    {
        return _records.TryGetValue(deduplicationId, out var createdAt) && createdAt > expirationTime;
    }

    /// <summary>
    /// Checks which deduplication IDs already exist (not expired) from the given list.
    /// </summary>
    public HashSet<string> GetExisting(IEnumerable<string> deduplicationIds, DateTime expirationTime)
    {
        var existing = new HashSet<string>();

        foreach (var deduplicationId in deduplicationIds)
        {
            if (Exists(deduplicationId, expirationTime))
            {
                existing.Add(deduplicationId);
            }
        }

        return existing;
    }

    /// <summary>
    /// Adds or updates a deduplication record with the current timestamp.
    /// </summary>
    public void AddOrUpdate(string deduplicationId, DateTime createdAt)
    {
        _records.AddOrUpdate(deduplicationId, createdAt, (_, _) => createdAt);
    }

    /// <summary>
    /// Adds or updates multiple deduplication records.
    /// </summary>
    public void AddOrUpdateBatch(IEnumerable<string> deduplicationIds, DateTime createdAt)
    {
        foreach (var deduplicationId in deduplicationIds)
        {
            AddOrUpdate(deduplicationId, createdAt);
        }
    }

    /// <summary>
    /// Removes all expired deduplication records.
    /// </summary>
    /// <returns>The number of records removed.</returns>
    public int CleanupExpired(DateTime expirationTime)
    {
        var keysToRemove = _records
            .Where(kvp => kvp.Value <= expirationTime)
            .Select(kvp => kvp.Key)
            .ToList();

        var removedCount = 0;
        foreach (var key in keysToRemove)
        {
            if (_records.TryRemove(key, out _))
            {
                removedCount++;
            }
        }

        return removedCount;
    }

    /// <summary>
    /// Gets the total count of deduplication records (for diagnostics).
    /// </summary>
    public int Count => _records.Count;
}
