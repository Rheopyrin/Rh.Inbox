using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Providers;
using Rh.Inbox.Redis.Connection;
using Rh.Inbox.Redis.Options;
using Rh.Inbox.Redis.Provider;
using StackExchange.Redis;
using Xunit;

namespace Rh.Inbox.Tests.Integration.Redis;

/// <summary>
/// Direct provider integration tests for RedisFifoInboxStorageProvider.
/// Only includes FIFO-specific tests that are NOT covered in RedisDefaultProviderDirectTests.
/// Tests create provider instances directly, bypassing DI, and verify behavior
/// by directly querying Redis.
/// </summary>
[Collection("Redis")]
public class RedisFifoProviderDirectTests : IAsyncLifetime
{
    private readonly RedisContainerFixture _container;
    private readonly string _keyPrefix = $"direct-fifo-test-{Guid.NewGuid():N}:";
    private IConnectionMultiplexer _connection = null!;
    private IDatabase _db = null!;
    private RedisInboxProviderOptions _providerOptions = null!;
    private RedisIdentifiers _keys = null!;

    private const string InboxName = "fifo-direct-test";

    public RedisFifoProviderDirectTests(RedisContainerFixture container)
    {
        _container = container;
    }

    public async Task InitializeAsync()
    {
        _connection = await ConnectionMultiplexer.ConnectAsync(_container.ConnectionString);
        _db = _connection.GetDatabase();

        var connectionProvider = Substitute.For<IRedisConnectionProvider>();
        connectionProvider.GetConnectionAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_connection);

        _providerOptions = new RedisInboxProviderOptions
        {
            ConnectionProvider = connectionProvider,
            ConnectionString = _container.ConnectionString,
            KeyPrefix = _keyPrefix,
            MaxMessageLifetime = TimeSpan.FromHours(24)
        };

        _keys = new RedisIdentifiers(_keyPrefix);
        await CleanupRedisAsync();
    }

    public async Task DisposeAsync()
    {
        await CleanupRedisAsync();
        await _connection.DisposeAsync();
    }

    #region ExtendLocksAsync Tests (FIFO-specific: extends both message and group locks)

    [Fact]
    public async Task ExtendLocksAsync_WithGroupId_ExtendsMessageAndGroupLock()
    {
        var provider = CreateProvider();
        var messageId = Guid.NewGuid();
        var groupId = "group-1";
        var processorId = "processor-1";
        var originalTime = DateTime.UtcNow.AddMinutes(-5);
        var newTime = DateTime.UtcNow;

        await InsertTestMessageAsync(messageId, groupId: groupId, capturedAt: originalTime, capturedBy: processorId);
        await InsertGroupLockAsync(groupId, processorId);

        var result = await provider.ExtendLocksAsync(
            processorId,
            [new MessageIdentifier(messageId, groupId)],
            newTime,
            CancellationToken.None);

        result.Should().Be(1);

        var message = await GetMessageAsync(messageId);
        message!.CapturedAt.Should().BeCloseTo(newTime, TimeSpan.FromSeconds(1));

        var groupLockExists = await GetGroupLockAsync(groupId);
        groupLockExists.Should().BeTrue("group lock should still exist after extension");

        var ttl = await GetGroupLockTtl(groupId);
        ttl.Should().BeGreaterThan(TimeSpan.FromMinutes(4), "group lock TTL should be extended");
    }

    [Fact]
    public async Task ExtendLocksAsync_WrongProcessor_DoesNotUpdate()
    {
        var provider = CreateProvider();
        var messageId = Guid.NewGuid();
        var groupId = "group-1";
        var originalTime = DateTime.UtcNow.AddMinutes(-5);

        await InsertTestMessageAsync(messageId, groupId: groupId, capturedAt: originalTime, capturedBy: "other-processor");
        await InsertGroupLockAsync(groupId, "other-processor");

        var result = await provider.ExtendLocksAsync(
            "processor-1",
            [new MessageIdentifier(messageId, groupId)],
            DateTime.UtcNow,
            CancellationToken.None);

        result.Should().Be(0);

        var message = await GetMessageAsync(messageId);
        message!.CapturedAt.Should().BeCloseTo(originalTime, TimeSpan.FromSeconds(1));

        var groupLockExists = await GetGroupLockAsync(groupId);
        groupLockExists.Should().BeTrue("group lock should not be extended by wrong processor");
    }

    [Fact]
    public async Task ExtendLocksAsync_MultipleGroups_ExtendsAll()
    {
        var provider = CreateProvider();
        var processorId = "processor-1";
        var originalTime = DateTime.UtcNow.AddMinutes(-5);
        var newTime = DateTime.UtcNow;

        var messages = new[]
        {
            (Id: Guid.NewGuid(), GroupId: "group-1"),
            (Id: Guid.NewGuid(), GroupId: "group-2"),
            (Id: Guid.NewGuid(), GroupId: "group-1")
        };

        foreach (var m in messages)
        {
            await InsertTestMessageAsync(m.Id, groupId: m.GroupId, capturedAt: originalTime, capturedBy: processorId);
        }
        await InsertGroupLockAsync("group-1", processorId);
        await InsertGroupLockAsync("group-2", processorId);

        var result = await provider.ExtendLocksAsync(
            processorId,
            messages.Select(m => new MessageIdentifier(m.Id, m.GroupId)).ToArray(),
            newTime,
            CancellationToken.None);

        result.Should().Be(3);

        foreach (var m in messages)
        {
            var message = await GetMessageAsync(m.Id);
            message!.CapturedAt.Should().BeCloseTo(newTime, TimeSpan.FromSeconds(1));
        }

        (await GetGroupLockAsync("group-1")).Should().BeTrue();
        (await GetGroupLockAsync("group-2")).Should().BeTrue();

        var ttl1 = await GetGroupLockTtl("group-1");
        var ttl2 = await GetGroupLockTtl("group-2");
        ttl1.Should().BeGreaterThan(TimeSpan.FromMinutes(4));
        ttl2.Should().BeGreaterThan(TimeSpan.FromMinutes(4));
    }

    [Fact]
    public async Task ExtendLocksAsync_EmptyList_ReturnsZero()
    {
        var provider = CreateProvider();
        var messageId = Guid.NewGuid();
        var groupId = "group-1";

        await InsertTestMessageAsync(messageId, groupId: groupId, capturedAt: DateTime.UtcNow, capturedBy: "processor-1");
        await InsertGroupLockAsync(groupId, "processor-1");

        var result = await provider.ExtendLocksAsync(
            "processor-1",
            Array.Empty<IInboxMessageIdentifiers>(),
            DateTime.UtcNow,
            CancellationToken.None);

        result.Should().Be(0);
    }

    #endregion

    #region ReleaseGroupLocksAsync Tests (FIFO-specific)

    [Fact]
    public async Task ReleaseGroupLocksAsync_ReleasesGroupLocks()
    {
        var provider = CreateProvider();

        await InsertGroupLockAsync("group-1", "proc-1");
        await InsertGroupLockAsync("group-2", "proc-1");

        await provider.ReleaseGroupLocksAsync(["group-1", "group-2"], CancellationToken.None);

        (await GetGroupLockAsync("group-1")).Should().BeFalse();
        (await GetGroupLockAsync("group-2")).Should().BeFalse();
    }

    [Fact]
    public async Task ReleaseGroupLocksAsync_MultipleGroups_ReleasesAll()
    {
        var provider = CreateProvider();

        await InsertGroupLockAsync("group-1", "proc-1");
        await InsertGroupLockAsync("group-2", "proc-1");
        await InsertGroupLockAsync("group-3", "proc-1");

        await provider.ReleaseGroupLocksAsync(["group-1", "group-2", "group-3"], CancellationToken.None);

        (await GetGroupLockAsync("group-1")).Should().BeFalse();
        (await GetGroupLockAsync("group-2")).Should().BeFalse();
        (await GetGroupLockAsync("group-3")).Should().BeFalse();
    }

    [Fact]
    public async Task ReleaseGroupLocksAsync_EmptyList_DoesNothing()
    {
        var provider = CreateProvider();

        await InsertGroupLockAsync("group-1", "proc-1");

        await provider.ReleaseGroupLocksAsync(Array.Empty<string>(), CancellationToken.None);

        var groupLockExists = await GetGroupLockAsync("group-1");
        groupLockExists.Should().BeTrue();
    }

    #endregion

    #region ReleaseMessagesAndGroupLocksAsync Tests (FIFO-specific)

    [Fact]
    public async Task ReleaseMessagesAndGroupLocksAsync_ReleasesMessageAndGroupLock()
    {
        var provider = CreateProvider();
        var messageId = Guid.NewGuid();
        var groupId = "group-1";

        await InsertTestMessageAsync(messageId, groupId: groupId, capturedAt: DateTime.UtcNow, capturedBy: "proc-1");
        await InsertGroupLockAsync(groupId, "proc-1");

        await provider.ReleaseMessagesAndGroupLocksAsync(
            [new MessageIdentifier(messageId, groupId)],
            CancellationToken.None);

        var message = await GetMessageAsync(messageId);
        message!.CapturedAt.Should().BeNull();
        message.CapturedBy.Should().BeNull();

        var groupLockExists = await GetGroupLockAsync(groupId);
        groupLockExists.Should().BeFalse();
    }

    [Fact]
    public async Task ReleaseMessagesAndGroupLocksAsync_MultipleGroups_ReleasesAll()
    {
        var provider = CreateProvider();
        var messages = new[]
        {
            (Id: Guid.NewGuid(), GroupId: "group-1"),
            (Id: Guid.NewGuid(), GroupId: "group-2"),
            (Id: Guid.NewGuid(), GroupId: "group-1")
        };

        foreach (var m in messages)
        {
            await InsertTestMessageAsync(m.Id, groupId: m.GroupId, capturedAt: DateTime.UtcNow, capturedBy: "proc-1");
        }
        await InsertGroupLockAsync("group-1", "proc-1");
        await InsertGroupLockAsync("group-2", "proc-1");

        await provider.ReleaseMessagesAndGroupLocksAsync(
            messages.Select(m => new MessageIdentifier(m.Id, m.GroupId)).ToArray(),
            CancellationToken.None);

        foreach (var m in messages)
        {
            var message = await GetMessageAsync(m.Id);
            message!.CapturedAt.Should().BeNull();
            message.CapturedBy.Should().BeNull();
        }

        (await GetGroupLockAsync("group-1")).Should().BeFalse();
        (await GetGroupLockAsync("group-2")).Should().BeFalse();
    }

    [Fact]
    public async Task ReleaseMessagesAndGroupLocksAsync_WithoutGroupIds_OnlyReleasesMessages()
    {
        var provider = CreateProvider();
        var messageIds = new[] { Guid.NewGuid(), Guid.NewGuid() };

        foreach (var id in messageIds)
        {
            await InsertTestMessageAsync(id, groupId: null, capturedAt: DateTime.UtcNow, capturedBy: "proc-1");
        }

        await provider.ReleaseMessagesAndGroupLocksAsync(
            messageIds.Select(id => new MessageIdentifier(id, null)).ToArray(),
            CancellationToken.None);

        foreach (var id in messageIds)
        {
            var message = await GetMessageAsync(id);
            message!.CapturedAt.Should().BeNull();
            message.CapturedBy.Should().BeNull();
        }
    }

    [Fact]
    public async Task ReleaseMessagesAndGroupLocksAsync_EmptyList_DoesNothing()
    {
        var provider = CreateProvider();
        var messageId = Guid.NewGuid();
        var groupId = "group-1";

        await InsertTestMessageAsync(messageId, groupId: groupId, capturedAt: DateTime.UtcNow, capturedBy: "proc-1");
        await InsertGroupLockAsync(groupId, "proc-1");

        await provider.ReleaseMessagesAndGroupLocksAsync(Array.Empty<IInboxMessageIdentifiers>(), CancellationToken.None);

        var message = await GetMessageAsync(messageId);
        message!.CapturedAt.Should().NotBeNull();

        var groupLockExists = await GetGroupLockAsync(groupId);
        groupLockExists.Should().BeTrue();
    }

    #endregion

    #region ReadAndCaptureAsync Tests (FIFO-specific: respects group locking)

    [Fact]
    public async Task ReadAndCaptureAsync_RespectsGroupLocking_OtherWorkersBlocked()
    {
        var provider = CreateProvider();

        // Insert messages for two groups - use very old timestamps to ensure they're picked up
        var lockedGroupId = "locked-group";
        var unlockedGroupId = "unlocked-group";

        var baseTime = DateTime.UtcNow.AddMinutes(-10);
        await InsertTestMessageAsync(Guid.NewGuid(), groupId: lockedGroupId, receivedAt: baseTime);
        await InsertTestMessageAsync(Guid.NewGuid(), groupId: unlockedGroupId, receivedAt: baseTime.AddSeconds(1));

        // Lock one group
        await InsertGroupLockAsync(lockedGroupId, "other-proc");

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        messages.Should().HaveCount(1);
        messages[0].GroupId.Should().Be(unlockedGroupId);
    }

    [Fact]
    public async Task ReadAndCaptureAsync_MultipleMessagesFromSameGroup_CapturedInOneBatch()
    {
        var provider = CreateProvider();
        var groupId = "group-1";

        // Insert multiple messages for same group - use old timestamps to ensure they're picked up
        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();
        var id3 = Guid.NewGuid();

        var baseTime = DateTime.UtcNow.AddMinutes(-10);
        await InsertTestMessageAsync(id1, groupId: groupId, receivedAt: baseTime);
        await InsertTestMessageAsync(id2, groupId: groupId, receivedAt: baseTime.AddSeconds(1));
        await InsertTestMessageAsync(id3, groupId: groupId, receivedAt: baseTime.AddSeconds(2));

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        // Within a single batch, all messages from the group are captured
        messages.Should().HaveCount(3);
        // Messages should be in FIFO order (by received_at timestamp)
        messages[0].Id.Should().Be(id1, "oldest message should be first");
        messages[1].Id.Should().Be(id2);
        messages[2].Id.Should().Be(id3);

        // Group should now be locked
        var groupLockExists = await GetGroupLockAsync(groupId);
        groupLockExists.Should().BeTrue();
    }

    [Fact]
    public async Task ReadAndCaptureAsync_DifferentGroups_CanBeCaptured()
    {
        var provider = CreateProvider();

        // Insert 2 messages per group for 2 groups (to stay within scan limits) - use old timestamps
        var baseTime = DateTime.UtcNow.AddMinutes(-10);
        for (int g = 0; g < 2; g++)
        {
            var groupId = $"group-{g}";
            await InsertTestMessageAsync(Guid.NewGuid(), groupId: groupId, receivedAt: baseTime.AddSeconds(g * 2));
            await InsertTestMessageAsync(Guid.NewGuid(), groupId: groupId, receivedAt: baseTime.AddSeconds(g * 2 + 1));
        }

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        // Within a single batch, all messages from all groups are captured
        messages.Should().HaveCount(4);
        messages.GroupBy(m => m.GroupId).Should().HaveCount(2);

        // All groups should be locked
        for (int g = 0; g < 2; g++)
        {
            var groupLockExists = await GetGroupLockAsync($"group-{g}");
            groupLockExists.Should().BeTrue();
        }
    }

    [Fact]
    public async Task ReadAndCaptureAsync_ExpiredGroupLock_AllowsRecapture()
    {
        var provider = CreateProvider();
        var groupId = "group-1";
        var messageId = Guid.NewGuid();

        await InsertTestMessageAsync(messageId, groupId: groupId);

        // Create expired group lock (TTL = 1 second)
        await InsertGroupLockAsync(groupId, "old-proc", ttl: TimeSpan.FromSeconds(1));

        // Wait for lock to expire
        await Task.Delay(TimeSpan.FromSeconds(1.5));

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        messages.Should().HaveCount(1);
        messages[0].Id.Should().Be(messageId);

        var groupLockExists = await GetGroupLockAsync(groupId);
        groupLockExists.Should().BeTrue("new lock should be created by processor-1");
    }

    [Fact]
    public async Task ReadAndCaptureAsync_MessagesWithoutGroupId_NotSubjectToGroupLocking()
    {
        var provider = CreateProvider();

        // Insert messages without group IDs - use old timestamps
        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();

        var baseTime = DateTime.UtcNow.AddMinutes(-10);
        await InsertTestMessageAsync(id1, groupId: null, receivedAt: baseTime);
        await InsertTestMessageAsync(id2, groupId: null, receivedAt: baseTime.AddSeconds(1));

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        messages.Should().HaveCount(2);
        messages.Should().Contain(m => m.Id == id1);
        messages.Should().Contain(m => m.Id == id2);
    }

    #endregion

    #region Helper Methods

    private IProviderOptionsAccessor CreateMockOptionsAccessor()
    {
        var mock = Substitute.For<IProviderOptionsAccessor>();
        mock.GetForInbox(InboxName).Returns(_providerOptions);
        return mock;
    }

    private IInboxConfiguration CreateConfiguration()
    {
        var options = Substitute.For<IInboxOptions>();
        options.InboxName.Returns(InboxName);
        options.ReadBatchSize.Returns(100);
        options.MaxProcessingTime.Returns(TimeSpan.FromMinutes(5));
        options.EnableDeadLetter.Returns(true);
        options.EnableDeduplication.Returns(false);
        options.DeduplicationInterval.Returns(TimeSpan.Zero);
        options.DeadLetterMaxMessageLifetime.Returns(TimeSpan.FromDays(7));

        var config = Substitute.For<IInboxConfiguration>();
        config.InboxName.Returns(InboxName);
        config.InboxType.Returns(InboxType.Fifo);
        config.Options.Returns(options);
        config.DateTimeProvider.Returns(new DateTimeProvider());

        return config;
    }

    private RedisFifoInboxStorageProvider CreateProvider()
    {
        var config = CreateConfiguration();
        var optionsAccessor = CreateMockOptionsAccessor();
        return new RedisFifoInboxStorageProvider(optionsAccessor, config, NullLogger<RedisFifoInboxStorageProvider>.Instance);
    }

    private async Task InsertTestMessageAsync(
        Guid id,
        string messageType = "TestMessage",
        string? groupId = null,
        DateTime? capturedAt = null,
        string? capturedBy = null,
        int attemptsCount = 0,
        DateTime? receivedAt = null)
    {
        var actualReceivedAt = receivedAt ?? DateTime.UtcNow;
        var score = new DateTimeOffset(actualReceivedAt).ToUnixTimeMilliseconds();

        // Insert into pending sorted set
        await _db.SortedSetAddAsync(_keys.PendingKey, id.ToString(), score);

        // Insert message hash with TTL
        var messageKey = _keys.MessageKey(id);
        var hashEntries = new[]
        {
            new HashEntry("id", id.ToString()),
            new HashEntry("inbox_name", InboxName),
            new HashEntry("message_type", messageType),
            new HashEntry("payload", "{}"),
            new HashEntry("group_id", groupId ?? ""),
            new HashEntry("collapse_key", ""),
            new HashEntry("attempts_count", attemptsCount),
            new HashEntry("received_at", actualReceivedAt.ToString("O")),
            new HashEntry("captured_at", capturedAt?.ToString("O") ?? ""),
            new HashEntry("captured_by", capturedBy ?? "")
        };

        await _db.HashSetAsync(messageKey, hashEntries);
        // Set TTL for message hash
        await _db.KeyExpireAsync(messageKey, TimeSpan.FromHours(24));

        // If captured, add to captured sorted set
        if (capturedAt.HasValue)
        {
            var capturedScore = new DateTimeOffset(capturedAt.Value).ToUnixTimeMilliseconds();
            await _db.SortedSetAddAsync(_keys.CapturedKey, id.ToString(), capturedScore);
        }
    }

    private async Task InsertGroupLockAsync(string groupId, string processorId, TimeSpan? ttl = null)
    {
        var lockKey = _keys.GroupLockKey(groupId);
        var actualTtl = ttl ?? TimeSpan.FromMinutes(5); // Default MaxProcessingTime
        await _db.StringSetAsync(lockKey, processorId, actualTtl);
    }

    private async Task<InboxMessageRecord?> GetMessageAsync(Guid id)
    {
        var messageKey = _keys.MessageKey(id);
        var hash = await _db.HashGetAllAsync(messageKey);

        if (hash.Length == 0)
        {
            return null;
        }

        var hashDict = hash.ToDictionary(h => h.Name.ToString(), h => h.Value.ToString());

        DateTime? capturedAt = null;
        if (hashDict.TryGetValue("captured_at", out var capturedAtStr) && !string.IsNullOrEmpty(capturedAtStr))
        {
            capturedAt = DateTime.Parse(capturedAtStr, null, System.Globalization.DateTimeStyles.RoundtripKind);
        }

        return new InboxMessageRecord(
            Guid.Parse(hashDict["id"]),
            capturedAt,
            hashDict.TryGetValue("captured_by", out var capturedBy) && !string.IsNullOrEmpty(capturedBy) ? capturedBy : null,
            int.Parse(hashDict["attempts_count"]));
    }

    private async Task<bool> GetGroupLockAsync(string groupId)
    {
        var lockKey = _keys.GroupLockKey(groupId);
        return await _db.KeyExistsAsync(lockKey);
    }

    private async Task<TimeSpan> GetGroupLockTtl(string groupId)
    {
        var lockKey = _keys.GroupLockKey(groupId);
        var ttl = await _db.KeyTimeToLiveAsync(lockKey);
        return ttl ?? TimeSpan.Zero;
    }

    private async Task CleanupRedisAsync()
    {
        // Clean up all keys with our prefix
        var server = _connection.GetServer(_connection.GetEndPoints().First());
        var keys = server.Keys(pattern: $"{_keyPrefix}*").ToArray();

        if (keys.Length > 0)
        {
            await _db.KeyDeleteAsync(keys);
        }
    }

    private record InboxMessageRecord(Guid Id, DateTime? CapturedAt, string? CapturedBy, int AttemptsCount);
    private record MessageIdentifier(Guid Id, string? GroupId) : IInboxMessageIdentifiers;

    #endregion
}
