using FluentAssertions;
using Rh.Inbox.InMemory;
using Xunit;

namespace Rh.Inbox.Tests.Unit.InMemory;

public class InMemoryDeduplicationStoreTests
{
    private readonly InMemoryDeduplicationStore _store;

    public InMemoryDeduplicationStoreTests()
    {
        _store = new InMemoryDeduplicationStore();
    }

    #region AddOrUpdate Tests

    [Fact]
    public void AddOrUpdate_NewKey_AddsEntry()
    {
        var key = "test-key";
        var timestamp = DateTime.UtcNow;

        _store.AddOrUpdate(key, timestamp);

        _store.Count.Should().Be(1);
        _store.Exists(key, timestamp.AddMinutes(-1)).Should().BeTrue();
    }

    [Fact]
    public void AddOrUpdate_ExistingKey_UpdatesTimestamp()
    {
        var key = "test-key";
        var oldTimestamp = DateTime.UtcNow.AddMinutes(-10);
        var newTimestamp = DateTime.UtcNow;

        _store.AddOrUpdate(key, oldTimestamp);
        _store.AddOrUpdate(key, newTimestamp);

        _store.Count.Should().Be(1);
        // Should exist with expiration time before new timestamp
        _store.Exists(key, newTimestamp.AddMinutes(-1)).Should().BeTrue();
        // Should NOT exist with expiration time after new timestamp
        _store.Exists(key, newTimestamp.AddMinutes(1)).Should().BeFalse();
    }

    [Fact]
    public void AddOrUpdate_MultipleKeys_AllAdded()
    {
        var timestamp = DateTime.UtcNow;

        _store.AddOrUpdate("key1", timestamp);
        _store.AddOrUpdate("key2", timestamp);
        _store.AddOrUpdate("key3", timestamp);

        _store.Count.Should().Be(3);
        _store.Exists("key1", timestamp.AddMinutes(-1)).Should().BeTrue();
        _store.Exists("key2", timestamp.AddMinutes(-1)).Should().BeTrue();
        _store.Exists("key3", timestamp.AddMinutes(-1)).Should().BeTrue();
    }

    #endregion

    #region AddOrUpdateBatch Tests

    [Fact]
    public void AddOrUpdateBatch_EmptyList_NoChanges()
    {
        var timestamp = DateTime.UtcNow;

        _store.AddOrUpdateBatch(Array.Empty<string>(), timestamp);

        _store.Count.Should().Be(0);
    }

    [Fact]
    public void AddOrUpdateBatch_MultipleKeys_AllAdded()
    {
        var keys = new[] { "key1", "key2", "key3" };
        var timestamp = DateTime.UtcNow;

        _store.AddOrUpdateBatch(keys, timestamp);

        _store.Count.Should().Be(3);
        _store.Exists("key1", timestamp.AddMinutes(-1)).Should().BeTrue();
        _store.Exists("key2", timestamp.AddMinutes(-1)).Should().BeTrue();
        _store.Exists("key3", timestamp.AddMinutes(-1)).Should().BeTrue();
    }

    [Fact]
    public void AddOrUpdateBatch_ExistingKeys_UpdatesTimestamps()
    {
        var keys = new[] { "key1", "key2" };
        var oldTimestamp = DateTime.UtcNow.AddMinutes(-10);
        var newTimestamp = DateTime.UtcNow;

        _store.AddOrUpdateBatch(keys, oldTimestamp);
        _store.AddOrUpdateBatch(keys, newTimestamp);

        _store.Count.Should().Be(2);
        _store.Exists("key1", newTimestamp.AddMinutes(-1)).Should().BeTrue();
        _store.Exists("key2", newTimestamp.AddMinutes(-1)).Should().BeTrue();
    }

    #endregion

    #region Exists Tests

    [Fact]
    public void Exists_KeyExists_ReturnsTrue()
    {
        var key = "test-key";
        var timestamp = DateTime.UtcNow;
        var expirationTime = timestamp.AddMinutes(-5);

        _store.AddOrUpdate(key, timestamp);

        _store.Exists(key, expirationTime).Should().BeTrue();
    }

    [Fact]
    public void Exists_KeyNotExists_ReturnsFalse()
    {
        var expirationTime = DateTime.UtcNow.AddMinutes(-5);

        _store.Exists("non-existent-key", expirationTime).Should().BeFalse();
    }

    [Fact]
    public void Exists_ExpiredKey_ReturnsFalse()
    {
        var key = "test-key";
        var timestamp = DateTime.UtcNow.AddMinutes(-10);
        var expirationTime = DateTime.UtcNow.AddMinutes(-5);

        _store.AddOrUpdate(key, timestamp);

        _store.Exists(key, expirationTime).Should().BeFalse();
    }

    [Fact]
    public void Exists_NotExpiredKey_ReturnsTrue()
    {
        var key = "test-key";
        var timestamp = DateTime.UtcNow;
        var expirationTime = DateTime.UtcNow.AddMinutes(-5);

        _store.AddOrUpdate(key, timestamp);

        _store.Exists(key, expirationTime).Should().BeTrue();
    }

    [Fact]
    public void Exists_ExactExpirationTime_ReturnsFalse()
    {
        var key = "test-key";
        var timestamp = DateTime.UtcNow;

        _store.AddOrUpdate(key, timestamp);

        // When timestamp equals expiration time, it should be considered expired (not greater than)
        _store.Exists(key, timestamp).Should().BeFalse();
    }

    #endregion

    #region GetExisting Tests

    [Fact]
    public void GetExisting_EmptyList_ReturnsEmptySet()
    {
        var expirationTime = DateTime.UtcNow.AddMinutes(-5);

        var result = _store.GetExisting(Array.Empty<string>(), expirationTime);

        result.Should().BeEmpty();
    }

    [Fact]
    public void GetExisting_NoExistingKeys_ReturnsEmptySet()
    {
        var keys = new[] { "key1", "key2", "key3" };
        var expirationTime = DateTime.UtcNow.AddMinutes(-5);

        var result = _store.GetExisting(keys, expirationTime);

        result.Should().BeEmpty();
    }

    [Fact]
    public void GetExisting_AllKeysExist_ReturnsAllKeys()
    {
        var keys = new[] { "key1", "key2", "key3" };
        var timestamp = DateTime.UtcNow;
        var expirationTime = timestamp.AddMinutes(-5);

        foreach (var key in keys)
        {
            _store.AddOrUpdate(key, timestamp);
        }

        var result = _store.GetExisting(keys, expirationTime);

        result.Should().HaveCount(3);
        result.Should().Contain(keys);
    }

    [Fact]
    public void GetExisting_MixedExistingAndNonExisting_ReturnsOnlyExisting()
    {
        var timestamp = DateTime.UtcNow;
        var expirationTime = timestamp.AddMinutes(-5);

        _store.AddOrUpdate("key1", timestamp);
        _store.AddOrUpdate("key3", timestamp);

        var keys = new[] { "key1", "key2", "key3", "key4" };
        var result = _store.GetExisting(keys, expirationTime);

        result.Should().HaveCount(2);
        result.Should().Contain(new[] { "key1", "key3" });
        result.Should().NotContain(new[] { "key2", "key4" });
    }

    [Fact]
    public void GetExisting_ExpiredKeys_NotReturned()
    {
        var oldTimestamp = DateTime.UtcNow.AddMinutes(-10);
        var newTimestamp = DateTime.UtcNow;
        var expirationTime = DateTime.UtcNow.AddMinutes(-5);

        _store.AddOrUpdate("expired-key", oldTimestamp);
        _store.AddOrUpdate("valid-key", newTimestamp);

        var keys = new[] { "expired-key", "valid-key" };
        var result = _store.GetExisting(keys, expirationTime);

        result.Should().HaveCount(1);
        result.Should().Contain("valid-key");
        result.Should().NotContain("expired-key");
    }

    #endregion

    #region CleanupExpired Tests

    [Fact]
    public void CleanupExpired_NoExpiredKeys_ReturnsZero()
    {
        var timestamp = DateTime.UtcNow;
        var expirationTime = DateTime.UtcNow.AddMinutes(-5);

        _store.AddOrUpdate("key1", timestamp);
        _store.AddOrUpdate("key2", timestamp);

        var removed = _store.CleanupExpired(expirationTime);

        removed.Should().Be(0);
        _store.Count.Should().Be(2);
    }

    [Fact]
    public void CleanupExpired_AllExpired_RemovesAll()
    {
        var timestamp = DateTime.UtcNow.AddMinutes(-10);
        var expirationTime = DateTime.UtcNow.AddMinutes(-5);

        _store.AddOrUpdate("key1", timestamp);
        _store.AddOrUpdate("key2", timestamp);
        _store.AddOrUpdate("key3", timestamp);

        var removed = _store.CleanupExpired(expirationTime);

        removed.Should().Be(3);
        _store.Count.Should().Be(0);
    }

    [Fact]
    public void CleanupExpired_MixedKeys_OnlyRemovesExpired()
    {
        var expiredTimestamp = DateTime.UtcNow.AddMinutes(-10);
        var validTimestamp = DateTime.UtcNow;
        var expirationTime = DateTime.UtcNow.AddMinutes(-5);

        _store.AddOrUpdate("expired1", expiredTimestamp);
        _store.AddOrUpdate("expired2", expiredTimestamp);
        _store.AddOrUpdate("valid1", validTimestamp);
        _store.AddOrUpdate("valid2", validTimestamp);

        var removed = _store.CleanupExpired(expirationTime);

        removed.Should().Be(2);
        _store.Count.Should().Be(2);
        _store.Exists("valid1", expirationTime).Should().BeTrue();
        _store.Exists("valid2", expirationTime).Should().BeTrue();
        _store.Exists("expired1", expirationTime).Should().BeFalse();
        _store.Exists("expired2", expirationTime).Should().BeFalse();
    }

    [Fact]
    public void CleanupExpired_EmptyStore_ReturnsZero()
    {
        var expirationTime = DateTime.UtcNow.AddMinutes(-5);

        var removed = _store.CleanupExpired(expirationTime);

        removed.Should().Be(0);
        _store.Count.Should().Be(0);
    }

    [Fact]
    public void CleanupExpired_ExactExpirationTime_RemovesEntry()
    {
        var expirationTime = DateTime.UtcNow;

        _store.AddOrUpdate("key1", expirationTime);

        var removed = _store.CleanupExpired(expirationTime);

        removed.Should().Be(1);
        _store.Count.Should().Be(0);
    }

    [Fact]
    public void CleanupExpired_MultipleCallsWithSameExpiration_OnlyRemovesOnce()
    {
        var expiredTimestamp = DateTime.UtcNow.AddMinutes(-10);
        var expirationTime = DateTime.UtcNow.AddMinutes(-5);

        _store.AddOrUpdate("key1", expiredTimestamp);
        _store.AddOrUpdate("key2", expiredTimestamp);

        var firstRemoved = _store.CleanupExpired(expirationTime);
        var secondRemoved = _store.CleanupExpired(expirationTime);

        firstRemoved.Should().Be(2);
        secondRemoved.Should().Be(0);
        _store.Count.Should().Be(0);
    }

    #endregion

    #region Count Tests

    [Fact]
    public void Count_EmptyStore_ReturnsZero()
    {
        _store.Count.Should().Be(0);
    }

    [Fact]
    public void Count_AfterAddOrUpdate_ReturnsCorrectCount()
    {
        var timestamp = DateTime.UtcNow;

        _store.AddOrUpdate("key1", timestamp);
        _store.Count.Should().Be(1);

        _store.AddOrUpdate("key2", timestamp);
        _store.Count.Should().Be(2);

        _store.AddOrUpdate("key3", timestamp);
        _store.Count.Should().Be(3);
    }

    [Fact]
    public void Count_AfterCleanup_ReturnsCorrectCount()
    {
        var expiredTimestamp = DateTime.UtcNow.AddMinutes(-10);
        var validTimestamp = DateTime.UtcNow;
        var expirationTime = DateTime.UtcNow.AddMinutes(-5);

        _store.AddOrUpdate("expired1", expiredTimestamp);
        _store.AddOrUpdate("expired2", expiredTimestamp);
        _store.AddOrUpdate("valid1", validTimestamp);

        _store.Count.Should().Be(3);

        _store.CleanupExpired(expirationTime);

        _store.Count.Should().Be(1);
    }

    [Fact]
    public void Count_AfterUpdatingExistingKey_RemainsUnchanged()
    {
        var key = "test-key";
        var oldTimestamp = DateTime.UtcNow.AddMinutes(-10);
        var newTimestamp = DateTime.UtcNow;

        _store.AddOrUpdate(key, oldTimestamp);
        _store.Count.Should().Be(1);

        _store.AddOrUpdate(key, newTimestamp);
        _store.Count.Should().Be(1);
    }

    #endregion

    #region Thread Safety Tests

    [Fact]
    public void ConcurrentAddOrUpdate_HandlesCorrectly()
    {
        var keys = Enumerable.Range(0, 100).Select(i => $"key-{i}").ToList();
        var timestamp = DateTime.UtcNow;

        Parallel.ForEach(keys, key =>
        {
            _store.AddOrUpdate(key, timestamp);
        });

        _store.Count.Should().Be(100);
    }

    [Fact]
    public void ConcurrentExistsAndCleanup_HandlesCorrectly()
    {
        var validTimestamp = DateTime.UtcNow;
        var expiredTimestamp = DateTime.UtcNow.AddMinutes(-10);
        var expirationTime = DateTime.UtcNow.AddMinutes(-5);

        // Add some valid and expired keys
        for (var i = 0; i < 50; i++)
        {
            _store.AddOrUpdate($"valid-{i}", validTimestamp);
            _store.AddOrUpdate($"expired-{i}", expiredTimestamp);
        }

        var exceptions = new List<Exception>();

        // Perform concurrent operations
        Parallel.Invoke(
            () =>
            {
                try
                {
                    for (var i = 0; i < 50; i++)
                    {
                        _store.Exists($"valid-{i}", expirationTime);
                    }
                }
                catch (Exception ex)
                {
                    lock (exceptions)
                    {
                        exceptions.Add(ex);
                    }
                }
            },
            () =>
            {
                try
                {
                    _store.CleanupExpired(expirationTime);
                }
                catch (Exception ex)
                {
                    lock (exceptions)
                    {
                        exceptions.Add(ex);
                    }
                }
            },
            () =>
            {
                try
                {
                    for (var i = 50; i < 100; i++)
                    {
                        _store.AddOrUpdate($"new-{i}", validTimestamp);
                    }
                }
                catch (Exception ex)
                {
                    lock (exceptions)
                    {
                        exceptions.Add(ex);
                    }
                }
            }
        );

        exceptions.Should().BeEmpty();
        _store.Count.Should().BeGreaterThan(0);
    }

    [Fact]
    public void ConcurrentAddOrUpdateSameKey_LastWriteWins()
    {
        var key = "test-key";
        var timestamps = Enumerable.Range(0, 100)
            .Select(i => DateTime.UtcNow.AddSeconds(i))
            .ToList();

        Parallel.ForEach(timestamps, timestamp =>
        {
            _store.AddOrUpdate(key, timestamp);
        });

        _store.Count.Should().Be(1);

        // The key should exist with one of the timestamps (we can't predict which one due to concurrency)
        var expirationTime = DateTime.UtcNow.AddSeconds(-1);
        _store.Exists(key, expirationTime).Should().BeTrue();
    }

    [Fact]
    public void ConcurrentGetExisting_HandlesCorrectly()
    {
        var timestamp = DateTime.UtcNow;
        var expirationTime = timestamp.AddMinutes(-5);

        // Add initial keys
        for (var i = 0; i < 50; i++)
        {
            _store.AddOrUpdate($"key-{i}", timestamp);
        }

        var results = new List<HashSet<string>>();
        var lockObject = new object();

        Parallel.For(0, 10, _ =>
        {
            var keys = Enumerable.Range(0, 50).Select(i => $"key-{i}").ToList();
            var result = _store.GetExisting(keys, expirationTime);

            lock (lockObject)
            {
                results.Add(result);
            }
        });

        results.Should().HaveCount(10);
        results.Should().OnlyContain(r => r.Count == 50);
    }

    [Fact]
    public void ConcurrentAddOrUpdateBatch_HandlesCorrectly()
    {
        var timestamp = DateTime.UtcNow;
        var batches = Enumerable.Range(0, 10)
            .Select(i => Enumerable.Range(i * 10, 10).Select(j => $"key-{j}").ToList())
            .ToList();

        Parallel.ForEach(batches, batch =>
        {
            _store.AddOrUpdateBatch(batch, timestamp);
        });

        _store.Count.Should().Be(100);
    }

    #endregion
}
