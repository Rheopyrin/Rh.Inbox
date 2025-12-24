using FluentAssertions;
using Rh.Inbox.InMemory.Collections;
using Xunit;

namespace Rh.Inbox.Tests.Unit.InMemory;

public class IndexedSortedCollectionTests
{
    private record TestItem(int Id, string Name, DateTime CreatedAt);

    #region TryAdd Tests

    [Fact]
    public void TryAdd_NewItem_ReturnsTrue()
    {
        var collection = CreateCollection();
        var item = new TestItem(1, "Item1", DateTime.UtcNow);

        var result = collection.TryAdd(item);

        result.Should().BeTrue();
    }

    [Fact]
    public void TryAdd_DuplicateKey_ReturnsFalse()
    {
        var collection = CreateCollection();
        var item1 = new TestItem(1, "Item1", DateTime.UtcNow);
        var item2 = new TestItem(1, "Item2", DateTime.UtcNow.AddMinutes(1));

        collection.TryAdd(item1);
        var result = collection.TryAdd(item2);

        result.Should().BeFalse();
    }

    [Fact]
    public void TryAdd_MultipleItems_AllAdded()
    {
        var collection = CreateCollection();
        var item1 = new TestItem(1, "Item1", DateTime.UtcNow);
        var item2 = new TestItem(2, "Item2", DateTime.UtcNow.AddMinutes(1));
        var item3 = new TestItem(3, "Item3", DateTime.UtcNow.AddMinutes(2));

        var result1 = collection.TryAdd(item1);
        var result2 = collection.TryAdd(item2);
        var result3 = collection.TryAdd(item3);

        result1.Should().BeTrue();
        result2.Should().BeTrue();
        result3.Should().BeTrue();
        collection.Count.Should().Be(3);
    }

    [Fact]
    public void TryAdd_IncreasesCount()
    {
        var collection = CreateCollection();
        var item = new TestItem(1, "Item1", DateTime.UtcNow);

        collection.Count.Should().Be(0);
        collection.TryAdd(item);
        collection.Count.Should().Be(1);
    }

    #endregion

    #region TryGetValue Tests

    [Fact]
    public void TryGetValue_ExistingKey_ReturnsTrueAndItem()
    {
        var collection = CreateCollection();
        var item = new TestItem(1, "Item1", DateTime.UtcNow);
        collection.TryAdd(item);

        var result = collection.TryGetValue(1, out var retrievedItem);

        result.Should().BeTrue();
        retrievedItem.Should().BeEquivalentTo(item);
    }

    [Fact]
    public void TryGetValue_NonExistingKey_ReturnsFalse()
    {
        var collection = CreateCollection();

        var result = collection.TryGetValue(1, out var retrievedItem);

        result.Should().BeFalse();
        retrievedItem.Should().BeNull();
    }

    [Fact]
    public void TryGetValue_EmptyCollection_ReturnsFalse()
    {
        var collection = CreateCollection();

        var result = collection.TryGetValue(999, out var retrievedItem);

        result.Should().BeFalse();
        retrievedItem.Should().BeNull();
    }

    #endregion

    #region ContainsKey Tests

    [Fact]
    public void ContainsKey_ExistingKey_ReturnsTrue()
    {
        var collection = CreateCollection();
        var item = new TestItem(1, "Item1", DateTime.UtcNow);
        collection.TryAdd(item);

        var result = collection.ContainsKey(1);

        result.Should().BeTrue();
    }

    [Fact]
    public void ContainsKey_NonExistingKey_ReturnsFalse()
    {
        var collection = CreateCollection();
        var item = new TestItem(1, "Item1", DateTime.UtcNow);
        collection.TryAdd(item);

        var result = collection.ContainsKey(999);

        result.Should().BeFalse();
    }

    #endregion

    #region TryRemove Tests

    [Fact]
    public void TryRemove_ExistingKey_ReturnsTrueAndItem()
    {
        var collection = CreateCollection();
        var item = new TestItem(1, "Item1", DateTime.UtcNow);
        collection.TryAdd(item);

        var result = collection.TryRemove(1, out var removedItem);

        result.Should().BeTrue();
        removedItem.Should().BeEquivalentTo(item);
    }

    [Fact]
    public void TryRemove_NonExistingKey_ReturnsFalse()
    {
        var collection = CreateCollection();

        var result = collection.TryRemove(999, out var removedItem);

        result.Should().BeFalse();
        removedItem.Should().BeNull();
    }

    [Fact]
    public void TryRemove_DecreasesCount()
    {
        var collection = CreateCollection();
        var item = new TestItem(1, "Item1", DateTime.UtcNow);
        collection.TryAdd(item);

        collection.Count.Should().Be(1);
        collection.TryRemove(1, out _);
        collection.Count.Should().Be(0);
    }

    [Fact]
    public void TryRemove_ItemNoLongerInEnumeration()
    {
        var collection = CreateCollection();
        var item1 = new TestItem(1, "Item1", DateTime.UtcNow);
        var item2 = new TestItem(2, "Item2", DateTime.UtcNow.AddMinutes(1));
        var item3 = new TestItem(3, "Item3", DateTime.UtcNow.AddMinutes(2));

        collection.TryAdd(item1);
        collection.TryAdd(item2);
        collection.TryAdd(item3);

        collection.TryRemove(2, out _);

        var items = collection.ToList();
        items.Should().HaveCount(2);
        items.Should().Contain(item1);
        items.Should().Contain(item3);
        items.Should().NotContain(item2);
    }

    #endregion

    #region Clear Tests

    [Fact]
    public void Clear_RemovesAllItems()
    {
        var collection = CreateCollection();
        var item1 = new TestItem(1, "Item1", DateTime.UtcNow);
        var item2 = new TestItem(2, "Item2", DateTime.UtcNow.AddMinutes(1));
        var item3 = new TestItem(3, "Item3", DateTime.UtcNow.AddMinutes(2));

        collection.TryAdd(item1);
        collection.TryAdd(item2);
        collection.TryAdd(item3);

        collection.Clear();

        collection.ContainsKey(1).Should().BeFalse();
        collection.ContainsKey(2).Should().BeFalse();
        collection.ContainsKey(3).Should().BeFalse();
    }

    [Fact]
    public void Clear_ResetsCount()
    {
        var collection = CreateCollection();
        collection.TryAdd(new TestItem(1, "Item1", DateTime.UtcNow));
        collection.TryAdd(new TestItem(2, "Item2", DateTime.UtcNow.AddMinutes(1)));
        collection.TryAdd(new TestItem(3, "Item3", DateTime.UtcNow.AddMinutes(2)));

        collection.Count.Should().Be(3);
        collection.Clear();
        collection.Count.Should().Be(0);
    }

    [Fact]
    public void Clear_EnumerationReturnsEmpty()
    {
        var collection = CreateCollection();
        collection.TryAdd(new TestItem(1, "Item1", DateTime.UtcNow));
        collection.TryAdd(new TestItem(2, "Item2", DateTime.UtcNow.AddMinutes(1)));

        collection.Clear();

        var items = collection.ToList();
        items.Should().BeEmpty();
    }

    #endregion

    #region Count Tests

    [Fact]
    public void Count_EmptyCollection_ReturnsZero()
    {
        var collection = CreateCollection();

        collection.Count.Should().Be(0);
    }

    [Fact]
    public void Count_AfterAdds_ReturnsCorrectCount()
    {
        var collection = CreateCollection();

        collection.Count.Should().Be(0);
        collection.TryAdd(new TestItem(1, "Item1", DateTime.UtcNow));
        collection.Count.Should().Be(1);
        collection.TryAdd(new TestItem(2, "Item2", DateTime.UtcNow.AddMinutes(1)));
        collection.Count.Should().Be(2);
        collection.TryAdd(new TestItem(3, "Item3", DateTime.UtcNow.AddMinutes(2)));
        collection.Count.Should().Be(3);
    }

    #endregion

    #region Enumeration/Sorting Tests

    [Fact]
    public void GetEnumerator_ReturnsSortedByKey()
    {
        var collection = CreateCollection();
        var now = DateTime.UtcNow;
        var item1 = new TestItem(1, "Item1", now.AddMinutes(10));
        var item2 = new TestItem(2, "Item2", now.AddMinutes(5));
        var item3 = new TestItem(3, "Item3", now);

        // Add in non-sorted order
        collection.TryAdd(item2);
        collection.TryAdd(item1);
        collection.TryAdd(item3);

        var items = collection.ToList();

        items.Should().HaveCount(3);
        items[0].Should().Be(item3); // Earliest CreatedAt
        items[1].Should().Be(item2);
        items[2].Should().Be(item1); // Latest CreatedAt
    }

    [Fact]
    public void GetEnumerator_SameSortKey_PreservesFifoOrder()
    {
        var collection = CreateCollection();
        var now = DateTime.UtcNow;
        var sameTime = now;

        // All items have the same CreatedAt time - insertion order should be preserved
        var item1 = new TestItem(1, "Item1", sameTime);
        var item2 = new TestItem(2, "Item2", sameTime);
        var item3 = new TestItem(3, "Item3", sameTime);
        var item4 = new TestItem(4, "Item4", sameTime);

        collection.TryAdd(item1);
        collection.TryAdd(item2);
        collection.TryAdd(item3);
        collection.TryAdd(item4);

        var items = collection.ToList();

        items.Should().HaveCount(4);
        items[0].Should().Be(item1); // First inserted
        items[1].Should().Be(item2);
        items[2].Should().Be(item3);
        items[3].Should().Be(item4); // Last inserted
    }

    [Fact]
    public void GetEnumerator_CustomComparer_UsesComparer()
    {
        // Create collection with descending order comparer
        var collection = new IndexedSortedCollection<int, TestItem, DateTime>(
            item => item.Id,
            item => item.CreatedAt,
            Comparer<DateTime>.Create((a, b) => b.CompareTo(a))); // Descending

        var now = DateTime.UtcNow;
        var item1 = new TestItem(1, "Item1", now);
        var item2 = new TestItem(2, "Item2", now.AddMinutes(5));
        var item3 = new TestItem(3, "Item3", now.AddMinutes(10));

        // Add in non-sorted order
        collection.TryAdd(item2);
        collection.TryAdd(item1);
        collection.TryAdd(item3);

        var items = collection.ToList();

        items.Should().HaveCount(3);
        items[0].Should().Be(item3); // Latest CreatedAt (descending)
        items[1].Should().Be(item2);
        items[2].Should().Be(item1); // Earliest CreatedAt
    }

    [Fact]
    public void GetEnumerator_EmptyCollection_ReturnsEmpty()
    {
        var collection = CreateCollection();

        var items = collection.ToList();

        items.Should().BeEmpty();
    }

    [Fact]
    public void NonGenericGetEnumerator_ReturnsItems()
    {
        var collection = CreateCollection();
        var item1 = new TestItem(1, "Item1", DateTime.UtcNow);
        var item2 = new TestItem(2, "Item2", DateTime.UtcNow.AddMinutes(1));

        collection.TryAdd(item1);
        collection.TryAdd(item2);

        // Use non-generic IEnumerable interface
        var enumerator = ((System.Collections.IEnumerable)collection).GetEnumerator();
        var items = new List<TestItem>();

        while (enumerator.MoveNext())
        {
            items.Add((TestItem)enumerator.Current);
        }

        items.Should().HaveCount(2);
        items.Should().Contain(item1);
        items.Should().Contain(item2);
    }

    #endregion

    #region Complex Scenarios

    [Fact]
    public void ComplexScenario_AddRemoveAdd_MaintainsCorrectState()
    {
        var collection = CreateCollection();
        var now = DateTime.UtcNow;
        var item1 = new TestItem(1, "Item1", now);
        var item2 = new TestItem(2, "Item2", now.AddMinutes(1));
        var item3 = new TestItem(3, "Item3", now.AddMinutes(2));

        // Add all items
        collection.TryAdd(item1);
        collection.TryAdd(item2);
        collection.TryAdd(item3);
        collection.Count.Should().Be(3);

        // Remove middle item
        collection.TryRemove(2, out _);
        collection.Count.Should().Be(2);

        // Verify remaining items are in correct order
        var items = collection.ToList();
        items[0].Should().Be(item1);
        items[1].Should().Be(item3);

        // Add a new item with same key as removed item
        var item2New = new TestItem(2, "Item2New", now.AddMinutes(0.5));
        collection.TryAdd(item2New);
        collection.Count.Should().Be(3);

        // Verify order with re-added item
        items = collection.ToList();
        items[0].Should().Be(item1);
        items[1].Should().Be(item2New); // Should be between item1 and item3 by sort key
        items[2].Should().Be(item3);
    }

    [Fact]
    public void ComplexScenario_SameSortKeyDifferentInsertionOrder_PreservesFifo()
    {
        var collection = CreateCollection();
        var now = DateTime.UtcNow;
        var sameTime = now;

        // Create items with same sort key but different IDs
        var item1 = new TestItem(1, "First", sameTime);
        var item2 = new TestItem(2, "Second", sameTime);
        var item3 = new TestItem(3, "Third", sameTime);

        collection.TryAdd(item1);
        collection.TryAdd(item2);
        collection.TryAdd(item3);

        // Remove middle one
        collection.TryRemove(2, out _);

        // Add new item with same sort key
        var item4 = new TestItem(4, "Fourth", sameTime);
        collection.TryAdd(item4);

        var items = collection.ToList();

        // Should maintain insertion order: item1, item3, item4 (item2 was removed)
        items.Should().HaveCount(3);
        items[0].Should().Be(item1);
        items[1].Should().Be(item3);
        items[2].Should().Be(item4);
    }

    [Fact]
    public void ComplexScenario_MixedSortKeysWithFifo()
    {
        var collection = CreateCollection();
        var now = DateTime.UtcNow;

        // Group 1: Early time (will be first in sorted order)
        var item1 = new TestItem(1, "Item1", now);
        var item2 = new TestItem(2, "Item2", now);

        // Group 2: Middle time
        var item3 = new TestItem(3, "Item3", now.AddMinutes(5));
        var item4 = new TestItem(4, "Item4", now.AddMinutes(5));

        // Group 3: Late time (will be last in sorted order)
        var item5 = new TestItem(5, "Item5", now.AddMinutes(10));
        var item6 = new TestItem(6, "Item6", now.AddMinutes(10));

        // Add in mixed order
        collection.TryAdd(item4);
        collection.TryAdd(item1);
        collection.TryAdd(item6);
        collection.TryAdd(item3);
        collection.TryAdd(item5);
        collection.TryAdd(item2);

        var items = collection.ToList();

        // Within each time group, insertion order should be preserved
        items.Should().HaveCount(6);
        items[0].Should().Be(item1); // now group - inserted first
        items[1].Should().Be(item2); // now group - inserted second
        items[2].Should().Be(item4); // +5min group - inserted first
        items[3].Should().Be(item3); // +5min group - inserted second
        items[4].Should().Be(item6); // +10min group - inserted first
        items[5].Should().Be(item5); // +10min group - inserted second
    }

    #endregion

    #region Helper Methods

    private static IndexedSortedCollection<int, TestItem, DateTime> CreateCollection()
    {
        return new IndexedSortedCollection<int, TestItem, DateTime>(
            item => item.Id,
            item => item.CreatedAt);
    }

    #endregion
}
