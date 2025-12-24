using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.InMemory.Collections;

namespace Rh.Inbox.InMemory;

/// <summary>
/// Storage for dead letter messages for a single inbox.
/// Thread safety is provided by the caller (provider or cleanup service).
/// </summary>
internal sealed class InMemoryDeadLetterStore : IDisposable
{
    private readonly IndexedSortedCollection<Guid, DeadLetterMessage, DateTime> _deadLetters = new(
        keySelector: m => m.Id,
        sortKeySelector: m => m.MovedAt);

    private readonly SemaphoreSlim _lock = new(1, 1);

    /// <summary>
    /// Adds a dead letter message to the store.
    /// </summary>
    public void Add(DeadLetterMessage message)
    {
        _deadLetters.TryAdd(message);
    }

    /// <summary>
    /// Reads dead letter messages ordered by MovedAt.
    /// </summary>
    public IReadOnlyList<DeadLetterMessage> Read(int count)
    {
        return _deadLetters.Take(count).ToList();
    }

    /// <summary>
    /// Removes dead letter messages older than the specified expiration time.
    /// Uses internal locking - safe to call from cleanup service.
    /// </summary>
    /// <param name="expirationTime">Messages with MovedAt before this time will be removed.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>The number of dead letter messages removed.</returns>
    public async Task<int> CleanupExpiredAsync(DateTime expirationTime, CancellationToken token)
    {
        await _lock.WaitAsync(token);
        try
        {
            var expiredIds = _deadLetters
                .Where(m => m.MovedAt <= expirationTime)
                .Select(m => m.Id)
                .ToList();

            foreach (var id in expiredIds)
            {
                _deadLetters.TryRemove(id, out _);
            }

            return expiredIds.Count;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Gets the total count of dead letter messages (for diagnostics).
    /// </summary>
    public int Count => _deadLetters.Count;

    public void Dispose()
    {
        _lock.Dispose();
    }
}
