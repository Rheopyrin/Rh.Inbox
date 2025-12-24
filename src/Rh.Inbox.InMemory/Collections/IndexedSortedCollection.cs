using System.Collections;
using System.Diagnostics.CodeAnalysis;

namespace Rh.Inbox.InMemory.Collections;

/// <summary>
/// A collection that provides O(1) lookup by key and O(k) ordered iteration by sort key.
/// Uses insertion order as tie-breaker when sort keys are equal, preserving FIFO ordering.
/// Not thread-safe - external synchronization required.
/// </summary>
/// <typeparam name="TKey">The type of the primary key.</typeparam>
/// <typeparam name="TItem">The type of items in the collection.</typeparam>
/// <typeparam name="TSortKey">The type of the sort key.</typeparam>
internal sealed class IndexedSortedCollection<TKey, TItem, TSortKey> : IEnumerable<TItem>
    where TKey : notnull
{
    private readonly Dictionary<TKey, (TItem Item, long InsertionOrder)> _byKey;
    private readonly SortedSet<(TItem Item, long InsertionOrder)> _sorted;
    private readonly Func<TItem, TKey> _keySelector;
    private long _insertionCounter;

    public IndexedSortedCollection(
        Func<TItem, TKey> keySelector,
        Func<TItem, TSortKey> sortKeySelector,
        IComparer<TSortKey>? sortKeyComparer = null)
    {
        _keySelector = keySelector;
        _byKey = new();
        _sorted = new(new ItemComparer(sortKeySelector, sortKeyComparer));
    }

    public int Count => _byKey.Count;

    public bool TryGetValue(TKey key, [MaybeNullWhen(false)] out TItem item)
    {
        if (_byKey.TryGetValue(key, out var entry))
        {
            item = entry.Item;
            return true;
        }
        item = default;
        return false;
    }

    public bool ContainsKey(TKey key) => _byKey.ContainsKey(key);

    public bool TryAdd(TItem item)
    {
        var key = _keySelector(item);
        var insertionOrder = _insertionCounter++;
        var entry = (item, insertionOrder);

        if (!_byKey.TryAdd(key, entry))
            return false;

        _sorted.Add(entry);
        return true;
    }

    public bool TryRemove(TKey key, [MaybeNullWhen(false)] out TItem item)
    {
        if (!_byKey.Remove(key, out var entry))
        {
            item = default;
            return false;
        }

        _sorted.Remove(entry);
        item = entry.Item;
        return true;
    }

    public void Clear()
    {
        _byKey.Clear();
        _sorted.Clear();
        _insertionCounter = 0;
    }

    /// <summary>
    /// Returns items in sorted order.
    /// </summary>
    public IEnumerator<TItem> GetEnumerator() =>
        _sorted.Select(e => e.Item).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private sealed class ItemComparer : IComparer<(TItem Item, long InsertionOrder)>
    {
        private readonly Func<TItem, TSortKey> _sortKeySelector;
        private readonly IComparer<TSortKey> _sortKeyComparer;

        public ItemComparer(
            Func<TItem, TSortKey> sortKeySelector,
            IComparer<TSortKey>? sortKeyComparer)
        {
            _sortKeySelector = sortKeySelector;
            _sortKeyComparer = sortKeyComparer ?? Comparer<TSortKey>.Default;
        }

        public int Compare((TItem Item, long InsertionOrder) x, (TItem Item, long InsertionOrder) y)
        {
            var sortCompare = _sortKeyComparer.Compare(_sortKeySelector(x.Item), _sortKeySelector(y.Item));
            if (sortCompare != 0) return sortCompare;

            // Tie-breaker using insertion order to preserve FIFO when sort keys are equal
            return x.InsertionOrder.CompareTo(y.InsertionOrder);
        }
    }
}