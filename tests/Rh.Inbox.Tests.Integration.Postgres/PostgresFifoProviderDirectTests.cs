using FluentAssertions;
using Npgsql;
using NSubstitute;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Postgres.Options;
using Rh.Inbox.Postgres.Provider;
using Rh.Inbox.Providers;
using Xunit;

namespace Rh.Inbox.Tests.Integration.Postgres;

/// <summary>
/// Direct provider integration tests for PostgresFifoInboxStorageProvider.
/// Only includes FIFO-specific tests that are NOT covered in PostgresDefaultProviderDirectTests.
/// Tests create provider instances directly, bypassing DI, and verify behavior
/// by directly querying the database.
/// </summary>
[Collection("Postgres")]
public class PostgresFifoProviderDirectTests : IAsyncLifetime
{
    private readonly PostgresContainerFixture _container;
    private NpgsqlDataSource _dataSource = null!;
    private PostgresInboxProviderOptions _providerOptions = null!;

    private const string InboxName = "fifo-direct-test";
    private const string TableName = "inbox_messages_fifo_direct";
    private const string DeadLetterTableName = "inbox_dlq_fifo_direct";
    private const string DeduplicationTableName = "inbox_dedup_fifo_direct";
    private const string GroupLocksTableName = "inbox_locks_fifo_direct";

    public PostgresFifoProviderDirectTests(PostgresContainerFixture container)
    {
        _container = container;
    }

    public Task InitializeAsync()
    {
        _dataSource = NpgsqlDataSource.Create(_container.ConnectionString);
        _providerOptions = new PostgresInboxProviderOptions
        {
            DataSource = _dataSource,
            TableName = TableName,
            DeadLetterTableName = DeadLetterTableName,
            DeduplicationTableName = DeduplicationTableName,
            GroupLocksTableName = GroupLocksTableName
        };
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await CleanupTablesAsync();
        await _dataSource.DisposeAsync();
    }

    #region ExtendLocksAsync Tests (FIFO-specific: extends both message and group locks)

    [Fact]
    public async Task ExtendLocksAsync_WithGroupId_ExtendsMessageAndGroupLock()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var messageId = Guid.NewGuid();
        var groupId = "group-1";
        var processorId = "processor-1";
        var originalTime = DateTime.UtcNow.AddMinutes(-5);
        var newTime = DateTime.UtcNow;

        await InsertTestMessageAsync(messageId, groupId: groupId, capturedAt: originalTime, capturedBy: processorId);
        await InsertGroupLockAsync(groupId, originalTime, processorId);

        var result = await provider.ExtendLocksAsync(
            processorId,
            [new MessageIdentifier(messageId, groupId)],
            newTime,
            CancellationToken.None);

        result.Should().Be(1);

        var message = await GetMessageAsync(messageId);
        message!.CapturedAt.Should().BeCloseTo(newTime, TimeSpan.FromSeconds(1));

        var groupLock = await GetGroupLockAsync(groupId);
        groupLock!.LockedAt.Should().BeCloseTo(newTime, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task ExtendLocksAsync_WithoutGroupId_OnlyExtendsMessageLock()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var messageId = Guid.NewGuid();
        var processorId = "processor-1";
        var originalTime = DateTime.UtcNow.AddMinutes(-5);
        var newTime = DateTime.UtcNow;

        await InsertTestMessageAsync(messageId, groupId: null, capturedAt: originalTime, capturedBy: processorId);

        var result = await provider.ExtendLocksAsync(
            processorId,
            [new MessageIdentifier(messageId, null)],
            newTime,
            CancellationToken.None);

        result.Should().Be(1);
        var message = await GetMessageAsync(messageId);
        message!.CapturedAt.Should().BeCloseTo(newTime, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task ExtendLocksAsync_MultipleGroupsAndMessages_ExtendsAll()
    {
        var provider = await CreateAndMigrateProviderAsync();
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
        await InsertGroupLockAsync("group-1", originalTime, processorId);
        await InsertGroupLockAsync("group-2", originalTime, processorId);

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

        var group1Lock = await GetGroupLockAsync("group-1");
        group1Lock!.LockedAt.Should().BeCloseTo(newTime, TimeSpan.FromSeconds(1));

        var group2Lock = await GetGroupLockAsync("group-2");
        group2Lock!.LockedAt.Should().BeCloseTo(newTime, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task ExtendLocksAsync_WrongProcessor_DoesNotUpdate()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var messageId = Guid.NewGuid();
        var groupId = "group-1";
        var originalTime = DateTime.UtcNow.AddMinutes(-5);

        await InsertTestMessageAsync(messageId, groupId: groupId, capturedAt: originalTime, capturedBy: "other-processor");
        await InsertGroupLockAsync(groupId, originalTime, "other-processor");

        var result = await provider.ExtendLocksAsync(
            "processor-1",
            [new MessageIdentifier(messageId, groupId)],
            DateTime.UtcNow,
            CancellationToken.None);

        result.Should().Be(0);

        var message = await GetMessageAsync(messageId);
        message!.CapturedAt.Should().BeCloseTo(originalTime, TimeSpan.FromSeconds(1));
    }

    #endregion

    #region ReleaseGroupLocksAsync Tests (FIFO-specific)

    [Fact]
    public async Task ReleaseGroupLocksAsync_ReleasesGroupLocks()
    {
        var provider = await CreateAndMigrateProviderAsync();

        await InsertGroupLockAsync("group-1", DateTime.UtcNow, "proc-1");
        await InsertGroupLockAsync("group-2", DateTime.UtcNow, "proc-1");

        await provider.ReleaseGroupLocksAsync(["group-1", "group-2"], CancellationToken.None);

        (await GetGroupLockAsync("group-1")).Should().BeNull();
        (await GetGroupLockAsync("group-2")).Should().BeNull();
    }

    [Fact]
    public async Task ReleaseGroupLocksAsync_EmptyList_DoesNothing()
    {
        var provider = await CreateAndMigrateProviderAsync();

        await InsertGroupLockAsync("group-1", DateTime.UtcNow, "proc-1");

        await provider.ReleaseGroupLocksAsync([], CancellationToken.None);

        var groupLock = await GetGroupLockAsync("group-1");
        groupLock.Should().NotBeNull();
    }

    [Fact]
    public async Task ReleaseGroupLocksAsync_PartialRelease_OnlyReleasesSpecified()
    {
        var provider = await CreateAndMigrateProviderAsync();

        await InsertGroupLockAsync("group-1", DateTime.UtcNow, "proc-1");
        await InsertGroupLockAsync("group-2", DateTime.UtcNow, "proc-1");
        await InsertGroupLockAsync("group-3", DateTime.UtcNow, "proc-1");

        await provider.ReleaseGroupLocksAsync(["group-1", "group-3"], CancellationToken.None);

        (await GetGroupLockAsync("group-1")).Should().BeNull();
        (await GetGroupLockAsync("group-2")).Should().NotBeNull();
        (await GetGroupLockAsync("group-3")).Should().BeNull();
    }

    #endregion

    #region ReleaseMessagesAndGroupLocksAsync Tests (FIFO-specific)

    [Fact]
    public async Task ReleaseMessagesAndGroupLocksAsync_ReleasesMessageAndGroupLock()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var messageId = Guid.NewGuid();
        var groupId = "group-1";

        await InsertTestMessageAsync(messageId, groupId: groupId, capturedAt: DateTime.UtcNow, capturedBy: "proc-1");
        await InsertGroupLockAsync(groupId, DateTime.UtcNow, "proc-1");

        await provider.ReleaseMessagesAndGroupLocksAsync(
            [new MessageIdentifier(messageId, groupId)],
            CancellationToken.None);

        var message = await GetMessageAsync(messageId);
        message!.CapturedAt.Should().BeNull();
        message.CapturedBy.Should().BeNull();

        var groupLock = await GetGroupLockAsync(groupId);
        groupLock.Should().BeNull();
    }

    [Fact]
    public async Task ReleaseMessagesAndGroupLocksAsync_MultipleMessagesAndGroups_ReleasesAll()
    {
        var provider = await CreateAndMigrateProviderAsync();
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
        await InsertGroupLockAsync("group-1", DateTime.UtcNow, "proc-1");
        await InsertGroupLockAsync("group-2", DateTime.UtcNow, "proc-1");

        await provider.ReleaseMessagesAndGroupLocksAsync(
            messages.Select(m => new MessageIdentifier(m.Id, m.GroupId)).ToArray(),
            CancellationToken.None);

        foreach (var m in messages)
        {
            var message = await GetMessageAsync(m.Id);
            message!.CapturedAt.Should().BeNull();
            message.CapturedBy.Should().BeNull();
        }

        (await GetGroupLockAsync("group-1")).Should().BeNull();
        (await GetGroupLockAsync("group-2")).Should().BeNull();
    }

    [Fact]
    public async Task ReleaseMessagesAndGroupLocksAsync_WithoutGroupIds_OnlyReleasesMessages()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var messageIds = new[] { Guid.NewGuid(), Guid.NewGuid() };

        foreach (var id in messageIds)
            await InsertTestMessageAsync(id, groupId: null, capturedAt: DateTime.UtcNow, capturedBy: "proc-1");

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
        var provider = await CreateAndMigrateProviderAsync();
        var messageId = Guid.NewGuid();
        var groupId = "group-1";

        await InsertTestMessageAsync(messageId, groupId: groupId, capturedAt: DateTime.UtcNow, capturedBy: "proc-1");
        await InsertGroupLockAsync(groupId, DateTime.UtcNow, "proc-1");

        await provider.ReleaseMessagesAndGroupLocksAsync([], CancellationToken.None);

        var message = await GetMessageAsync(messageId);
        message!.CapturedAt.Should().NotBeNull();

        var groupLock = await GetGroupLockAsync(groupId);
        groupLock.Should().NotBeNull();
    }

    #endregion

    #region ReadAndCaptureAsync Tests (FIFO-specific: respects group locking)

    [Fact]
    public async Task ReadAndCaptureAsync_WithGroupLock_SkipsLockedGroup()
    {
        var provider = await CreateAndMigrateProviderAsync();

        // Insert messages for two groups
        var lockedGroupId = "locked-group";
        var unlockedGroupId = "unlocked-group";

        await InsertTestMessageAsync(Guid.NewGuid(), groupId: lockedGroupId);
        await InsertTestMessageAsync(Guid.NewGuid(), groupId: unlockedGroupId);

        // Lock one group
        await InsertGroupLockAsync(lockedGroupId, DateTime.UtcNow, "other-proc");

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        messages.Should().HaveCount(1);
        messages[0].GroupId.Should().Be(unlockedGroupId);
    }

    [Fact]
    public async Task ReadAndCaptureAsync_CapturesMessagesAndLocksGroup()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var groupId = "group-1";

        // Insert multiple messages for same group
        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();
        var id3 = Guid.NewGuid();

        await InsertTestMessageAsync(id1, groupId: groupId, receivedAt: DateTime.UtcNow.AddSeconds(-3));
        await InsertTestMessageAsync(id2, groupId: groupId, receivedAt: DateTime.UtcNow.AddSeconds(-2));
        await InsertTestMessageAsync(id3, groupId: groupId, receivedAt: DateTime.UtcNow.AddSeconds(-1));

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        // Within a single batch, all messages from the group are captured
        messages.Should().HaveCount(3);
        // Messages should be in FIFO order
        messages[0].Id.Should().Be(id1, "oldest message should be first");
        messages[1].Id.Should().Be(id2);
        messages[2].Id.Should().Be(id3);

        // Group should now be locked
        var groupLock = await GetGroupLockAsync(groupId);
        groupLock.Should().NotBeNull();
        groupLock!.LockedBy.Should().Be("processor-1");
    }

    [Fact]
    public async Task ReadAndCaptureAsync_MultipleGroups_CapturesAndLocksAllGroups()
    {
        var provider = await CreateAndMigrateProviderAsync();

        // Insert 2 messages per group for 3 groups
        for (int g = 0; g < 3; g++)
        {
            var groupId = $"group-{g}";
            await InsertTestMessageAsync(Guid.NewGuid(), groupId: groupId, receivedAt: DateTime.UtcNow.AddSeconds(-2));
            await InsertTestMessageAsync(Guid.NewGuid(), groupId: groupId, receivedAt: DateTime.UtcNow.AddSeconds(-1));
        }

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        // Within a single batch, all messages from all groups are captured
        messages.Should().HaveCount(6);
        messages.GroupBy(m => m.GroupId).Should().HaveCount(3);

        // All groups should be locked
        for (int g = 0; g < 3; g++)
        {
            var groupLock = await GetGroupLockAsync($"group-{g}");
            groupLock.Should().NotBeNull();
        }
    }

    [Fact]
    public async Task ReadAndCaptureAsync_ExpiredGroupLock_RecapturesGroup()
    {
        var provider = await CreateAndMigrateProviderAsync();
        var groupId = "group-1";
        var messageId = Guid.NewGuid();

        await InsertTestMessageAsync(messageId, groupId: groupId);

        // Create expired group lock (10 minutes ago - default MaxProcessingTime is 5 min)
        var expiredTime = DateTime.UtcNow.AddMinutes(-10);
        await InsertGroupLockAsync(groupId, expiredTime, "old-proc");

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        messages.Should().HaveCount(1);
        messages[0].Id.Should().Be(messageId);

        var groupLock = await GetGroupLockAsync(groupId);
        groupLock!.LockedBy.Should().Be("processor-1");
    }

    [Fact]
    public async Task ReadAndCaptureAsync_RespectsBatchSize_AcrossGroups()
    {
        var provider = await CreateAndMigrateProviderAsync(batchSize: 2);

        // Insert messages for 5 groups
        for (int g = 0; g < 5; g++)
        {
            await InsertTestMessageAsync(Guid.NewGuid(), groupId: $"group-{g}");
        }

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        messages.Should().HaveCount(2, "should respect batch size");
    }

    #endregion

    #region Helper Methods

    private IProviderOptionsAccessor CreateMockOptionsAccessor()
    {
        var mock = Substitute.For<IProviderOptionsAccessor>();
        mock.GetForInbox(InboxName).Returns(_providerOptions);
        return mock;
    }

    private IInboxConfiguration CreateConfiguration(int batchSize = 100)
    {
        var options = Substitute.For<IInboxOptions>();
        options.InboxName.Returns(InboxName);
        options.ReadBatchSize.Returns(batchSize);
        options.MaxProcessingTime.Returns(TimeSpan.FromMinutes(5));
        options.EnableDeadLetter.Returns(true);
        options.EnableDeduplication.Returns(false);
        options.DeduplicationInterval.Returns(TimeSpan.Zero);

        var config = Substitute.For<IInboxConfiguration>();
        config.InboxName.Returns(InboxName);
        config.InboxType.Returns(InboxType.Fifo);
        config.Options.Returns(options);
        config.DateTimeProvider.Returns(new DateTimeProvider());

        return config;
    }

    private async Task<PostgresFifoInboxStorageProvider> CreateAndMigrateProviderAsync(int batchSize = 100)
    {
        await CleanupTablesAsync();
        var config = CreateConfiguration(batchSize);
        var optionsAccessor = CreateMockOptionsAccessor();
        var provider = new PostgresFifoInboxStorageProvider(config, optionsAccessor);
        await provider.MigrateAsync(CancellationToken.None);
        return provider;
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
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand($"""
            INSERT INTO "{TableName}"
            (id, inbox_name, message_type, payload, group_id, attempts_count, received_at, captured_at, captured_by)
            VALUES (@id, @inboxName, @messageType, @payload, @groupId, @attemptsCount, @receivedAt, @capturedAt, @capturedBy)
            """, conn);

        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("inboxName", InboxName);
        cmd.Parameters.AddWithValue("messageType", messageType);
        cmd.Parameters.AddWithValue("payload", "{}");
        cmd.Parameters.AddWithValue("groupId", (object?)groupId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("attemptsCount", attemptsCount);
        cmd.Parameters.AddWithValue("receivedAt", receivedAt ?? DateTime.UtcNow);
        cmd.Parameters.AddWithValue("capturedAt", (object?)capturedAt ?? DBNull.Value);
        cmd.Parameters.AddWithValue("capturedBy", (object?)capturedBy ?? DBNull.Value);

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task InsertGroupLockAsync(string groupId, DateTime? lockedAt, string? lockedBy)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand($"""
            INSERT INTO "{GroupLocksTableName}" (inbox_name, group_id, locked_at, locked_by)
            VALUES (@inboxName, @groupId, @lockedAt, @lockedBy)
            """, conn);

        cmd.Parameters.AddWithValue("inboxName", InboxName);
        cmd.Parameters.AddWithValue("groupId", groupId);
        cmd.Parameters.AddWithValue("lockedAt", (object?)lockedAt ?? DBNull.Value);
        cmd.Parameters.AddWithValue("lockedBy", (object?)lockedBy ?? DBNull.Value);

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<InboxMessageRecord?> GetMessageAsync(Guid id)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand($"""
            SELECT id, captured_at, captured_by, attempts_count
            FROM "{TableName}" WHERE id = @id
            """, conn);
        cmd.Parameters.AddWithValue("id", id);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            return new InboxMessageRecord(
                reader.GetGuid(0),
                reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetInt32(3));
        }
        return null;
    }

    private async Task<GroupLockRecord?> GetGroupLockAsync(string groupId)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand($"""
            SELECT group_id, locked_at, locked_by
            FROM "{GroupLocksTableName}"
            WHERE inbox_name = @inboxName AND group_id = @groupId
            """, conn);
        cmd.Parameters.AddWithValue("inboxName", InboxName);
        cmd.Parameters.AddWithValue("groupId", groupId);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            return new GroupLockRecord(
                reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                reader.IsDBNull(2) ? null : reader.GetString(2));
        }
        return null;
    }

    private async Task CleanupTablesAsync()
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand($"""
            DROP TABLE IF EXISTS "{TableName}" CASCADE;
            DROP TABLE IF EXISTS "{DeadLetterTableName}" CASCADE;
            DROP TABLE IF EXISTS "{DeduplicationTableName}" CASCADE;
            DROP TABLE IF EXISTS "{GroupLocksTableName}" CASCADE;
            """, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private record InboxMessageRecord(Guid Id, DateTime? CapturedAt, string? CapturedBy, int AttemptsCount);
    private record GroupLockRecord(string GroupId, DateTime? LockedAt, string? LockedBy);
    private record MessageIdentifier(Guid Id, string? GroupId) : IInboxMessageIdentifiers;

    #endregion
}
