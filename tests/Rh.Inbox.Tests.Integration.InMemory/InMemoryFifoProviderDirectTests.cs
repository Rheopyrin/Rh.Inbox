using System.Reflection;
using FluentAssertions;
using NSubstitute;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.InMemory;
using Rh.Inbox.InMemory.Options;
using Rh.Inbox.Providers;
using Xunit;

namespace Rh.Inbox.Tests.Integration.InMemory;

/// <summary>
/// Direct provider integration tests for InMemoryInboxStorageProvider in FIFO mode.
/// Only includes FIFO-specific tests that are NOT covered by standard tests.
/// Tests create provider instances directly, bypassing DI, and verify behavior
/// by directly calling provider methods.
/// </summary>
public class InMemoryFifoProviderDirectTests
{
    private const string InboxName = "fifo-direct-test";

    #region ExtendLocksAsync Tests (FIFO-specific: extends both message and group locks)

    [Fact]
    public async Task ExtendLocksAsync_WithGroupId_ExtendsMessageAndGroupLock()
    {
        var provider = CreateFifoProviderAsync();
        var messageId = Guid.NewGuid();
        var groupId = "group-1";
        var processorId = "processor-1";
        var originalTime = DateTime.UtcNow.AddMinutes(-5);
        var newTime = DateTime.UtcNow;

        // Insert and capture message
        await InsertAndCaptureMessageAsync(provider, messageId, groupId, originalTime, processorId);

        // Extend locks
        var result = await provider.ExtendLocksAsync(
            processorId,
            [new MessageIdentifier(messageId, groupId)],
            newTime,
            CancellationToken.None);

        result.Should().Be(1);

        // Verify message lock was extended by attempting to capture again (shouldn't be available)
        var messages = await provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        messages.Should().BeEmpty("message lock should be extended, not expired");

        // Verify group lock was extended by inserting another message from same group
        var messageId2 = Guid.NewGuid();
        await InsertMessageAsync(provider, messageId2, groupId);

        var messages2 = await provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        messages2.Should().BeEmpty("group lock should prevent capturing messages from this group");
    }

    [Fact]
    public async Task ExtendLocksAsync_WithoutGroupId_OnlyExtendsMessageLock()
    {
        var provider = CreateFifoProviderAsync();
        var messageId = Guid.NewGuid();
        var processorId = "processor-1";
        var originalTime = DateTime.UtcNow.AddMinutes(-5);
        var newTime = DateTime.UtcNow;

        // Insert and capture message without group
        await InsertAndCaptureMessageAsync(provider, messageId, null, originalTime, processorId);

        var result = await provider.ExtendLocksAsync(
            processorId,
            [new MessageIdentifier(messageId, null)],
            newTime,
            CancellationToken.None);

        result.Should().Be(1);

        // Verify message lock was extended
        var messages = await provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        messages.Should().BeEmpty("message lock should be extended");
    }

    [Fact]
    public async Task ExtendLocksAsync_MultipleGroupsAndMessages_ExtendsAll()
    {
        var provider = CreateFifoProviderAsync();
        var processorId = "processor-1";
        var originalTime = DateTime.UtcNow.AddMinutes(-5);
        var newTime = DateTime.UtcNow;

        var messages = new[]
        {
            (Id: Guid.NewGuid(), GroupId: "group-1"),
            (Id: Guid.NewGuid(), GroupId: "group-2"),
            (Id: Guid.NewGuid(), GroupId: "group-1")
        };

        // Insert all messages first
        foreach (var m in messages)
        {
            await InsertMessageAsync(provider, m.Id, m.GroupId);
        }

        // Capture all at once (this will lock both groups)
        var captured = await provider.ReadAndCaptureAsync(processorId, CancellationToken.None);
        captured.Should().HaveCount(3);

        // Modify captured timestamps using reflection
        foreach (var m in messages)
        {
            ModifyMessageCaptureTime(provider, m.Id, originalTime);
        }
        ModifyGroupLockTime(provider, "group-1", originalTime);
        ModifyGroupLockTime(provider, "group-2", originalTime);

        var result = await provider.ExtendLocksAsync(
            processorId,
            messages.Select(m => new MessageIdentifier(m.Id, m.GroupId)).ToArray(),
            newTime,
            CancellationToken.None);

        result.Should().Be(3);

        // Verify all message locks were extended
        var capturedMessages = await provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        capturedMessages.Should().BeEmpty("all message locks should be extended");

        // Verify group locks were extended by inserting new messages
        await InsertMessageAsync(provider, Guid.NewGuid(), "group-1");
        await InsertMessageAsync(provider, Guid.NewGuid(), "group-2");

        var capturedMessages2 = await provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        capturedMessages2.Should().BeEmpty("both group locks should be extended");
    }

    [Fact]
    public async Task ExtendLocksAsync_WrongProcessor_DoesNotUpdate()
    {
        var provider = CreateFifoProviderAsync();
        var messageId = Guid.NewGuid();
        var groupId = "group-1";
        var originalTime = DateTime.UtcNow.AddMinutes(-5);

        await InsertAndCaptureMessageAsync(provider, messageId, groupId, originalTime, "other-processor");

        var result = await provider.ExtendLocksAsync(
            "processor-1",
            [new MessageIdentifier(messageId, groupId)],
            DateTime.UtcNow,
            CancellationToken.None);

        result.Should().Be(0, "wrong processor should not be able to extend locks");
    }

    [Fact]
    public async Task ExtendLocksAsync_EmptyList_ReturnsZero()
    {
        var provider = CreateFifoProviderAsync();

        var result = await provider.ExtendLocksAsync(
            "processor-1",
            Array.Empty<MessageIdentifier>(),
            DateTime.UtcNow,
            CancellationToken.None);

        result.Should().Be(0);
    }

    #endregion

    #region ReleaseGroupLocksAsync Tests (FIFO-specific)

    [Fact]
    public async Task ReleaseGroupLocksAsync_ReleasesGroupLocks()
    {
        var provider = CreateFifoProviderAsync();
        var groupId1 = "group-1";
        var groupId2 = "group-2";

        // Capture messages to lock groups
        var msg1 = Guid.NewGuid();
        var msg2 = Guid.NewGuid();
        await InsertMessageAsync(provider, msg1, groupId1);
        await InsertMessageAsync(provider, msg2, groupId2);
        await provider.ReadAndCaptureAsync("proc-1", CancellationToken.None);

        // Release group locks
        await provider.ReleaseGroupLocksAsync([groupId1, groupId2], CancellationToken.None);

        // Verify groups are unlocked by inserting and capturing new messages
        var msg3 = Guid.NewGuid();
        var msg4 = Guid.NewGuid();
        await InsertMessageAsync(provider, msg3, groupId1);
        await InsertMessageAsync(provider, msg4, groupId2);

        var messages = await provider.ReadAndCaptureAsync("proc-2", CancellationToken.None);
        messages.Should().HaveCount(2, "both groups should be unlocked");
        messages.Select(m => m.Id).Should().Contain([msg3, msg4]);
    }

    [Fact]
    public async Task ReleaseGroupLocksAsync_EmptyList_DoesNothing()
    {
        var provider = CreateFifoProviderAsync();
        var groupId = "group-1";

        // Lock a group
        var msg1 = Guid.NewGuid();
        await InsertMessageAsync(provider, msg1, groupId);
        await provider.ReadAndCaptureAsync("proc-1", CancellationToken.None);

        // Call with empty list
        await provider.ReleaseGroupLocksAsync(Array.Empty<string>(), CancellationToken.None);

        // Verify group is still locked
        var msg2 = Guid.NewGuid();
        await InsertMessageAsync(provider, msg2, groupId);
        var messages = await provider.ReadAndCaptureAsync("proc-2", CancellationToken.None);
        messages.Should().BeEmpty("group should still be locked");
    }

    [Fact]
    public async Task ReleaseGroupLocksAsync_PartialRelease_OnlyReleasesSpecified()
    {
        var provider = CreateFifoProviderAsync();

        // Lock three groups
        var groups = new[] { "group-1", "group-2", "group-3" };
        foreach (var group in groups)
        {
            await InsertMessageAsync(provider, Guid.NewGuid(), group);
        }
        await provider.ReadAndCaptureAsync("proc-1", CancellationToken.None);

        // Release only group-1 and group-3
        await provider.ReleaseGroupLocksAsync(["group-1", "group-3"], CancellationToken.None);

        // Insert new messages for all groups
        var msg1 = Guid.NewGuid();
        var msg2 = Guid.NewGuid();
        var msg3 = Guid.NewGuid();
        await InsertMessageAsync(provider, msg1, "group-1");
        await InsertMessageAsync(provider, msg2, "group-2");
        await InsertMessageAsync(provider, msg3, "group-3");

        var messages = await provider.ReadAndCaptureAsync("proc-2", CancellationToken.None);
        messages.Should().HaveCount(2, "only group-1 and group-3 should be unlocked");
        messages.Select(m => m.Id).Should().Contain([msg1, msg3]);
        messages.Select(m => m.Id).Should().NotContain(msg2);
    }

    #endregion

    #region ReleaseMessagesAndGroupLocksAsync Tests (FIFO-specific)

    [Fact]
    public async Task ReleaseMessagesAndGroupLocksAsync_ReleasesMessageAndGroupLock()
    {
        var provider = CreateFifoProviderAsync();
        var messageId = Guid.NewGuid();
        var groupId = "group-1";

        await InsertMessageAsync(provider, messageId, groupId);
        var captured = await provider.ReadAndCaptureAsync("proc-1", CancellationToken.None);
        captured.Should().HaveCount(1);

        // Insert another message from the same group (should not be captured due to lock)
        var messageId2 = Guid.NewGuid();
        await InsertMessageAsync(provider, messageId2, groupId);
        var messages = await provider.ReadAndCaptureAsync("proc-2", CancellationToken.None);
        messages.Should().BeEmpty("group should be locked");

        // Release message and group lock
        await provider.ReleaseMessagesAndGroupLocksAsync(
            [new MessageIdentifier(messageId, groupId)],
            CancellationToken.None);

        // Verify message is released and group lock is released (both messages can now be captured)
        var messages2 = await provider.ReadAndCaptureAsync("proc-2", CancellationToken.None);
        messages2.Should().HaveCount(2, "both messages should be captured now");
        messages2.Select(m => m.Id).Should().Contain([messageId, messageId2]);
    }

    [Fact]
    public async Task ReleaseMessagesAndGroupLocksAsync_MultipleMessagesAndGroups_ReleasesAll()
    {
        var provider = CreateFifoProviderAsync();
        var messages = new[]
        {
            (Id: Guid.NewGuid(), GroupId: "group-1"),
            (Id: Guid.NewGuid(), GroupId: "group-2"),
            (Id: Guid.NewGuid(), GroupId: "group-1")
        };

        foreach (var m in messages)
        {
            await InsertMessageAsync(provider, m.Id, m.GroupId);
        }

        var captured = await provider.ReadAndCaptureAsync("proc-1", CancellationToken.None);
        captured.Should().HaveCount(3);

        // Insert new messages for both groups (should not be captured due to locks)
        var newMsg1 = Guid.NewGuid();
        var newMsg2 = Guid.NewGuid();
        await InsertMessageAsync(provider, newMsg1, "group-1");
        await InsertMessageAsync(provider, newMsg2, "group-2");

        var beforeRelease = await provider.ReadAndCaptureAsync("proc-2", CancellationToken.None);
        beforeRelease.Should().BeEmpty("both groups should be locked");

        // Release all messages and group locks
        await provider.ReleaseMessagesAndGroupLocksAsync(
            messages.Select(m => new MessageIdentifier(m.Id, m.GroupId)).ToArray(),
            CancellationToken.None);

        // Verify all messages (both original and new) can now be captured
        var afterRelease = await provider.ReadAndCaptureAsync("proc-2", CancellationToken.None);
        afterRelease.Should().HaveCount(5, "all 5 messages should be capturable now");
        afterRelease.Select(m => m.Id).Should().Contain(messages.Select(m => m.Id).Concat([newMsg1, newMsg2]));
    }

    [Fact]
    public async Task ReleaseMessagesAndGroupLocksAsync_WithoutGroupIds_OnlyReleasesMessages()
    {
        var provider = CreateFifoProviderAsync();
        var messageIds = new[] { Guid.NewGuid(), Guid.NewGuid() };

        foreach (var id in messageIds)
        {
            await InsertMessageAsync(provider, id, groupId: null);
        }

        var captured = await provider.ReadAndCaptureAsync("proc-1", CancellationToken.None);
        captured.Should().HaveCount(2);

        await provider.ReleaseMessagesAndGroupLocksAsync(
            messageIds.Select(id => new MessageIdentifier(id, null)).ToArray(),
            CancellationToken.None);

        // Verify messages are released
        var messages = await provider.ReadAndCaptureAsync("proc-2", CancellationToken.None);
        messages.Should().HaveCount(2);
        messages.Select(m => m.Id).Should().Contain(messageIds);
    }

    [Fact]
    public async Task ReleaseMessagesAndGroupLocksAsync_EmptyList_DoesNothing()
    {
        var provider = CreateFifoProviderAsync();
        var messageId = Guid.NewGuid();
        var groupId = "group-1";

        await InsertMessageAsync(provider, messageId, groupId);
        var captured = await provider.ReadAndCaptureAsync("proc-1", CancellationToken.None);
        captured.Should().HaveCount(1);

        // Call with empty list
        await provider.ReleaseMessagesAndGroupLocksAsync(
            Array.Empty<MessageIdentifier>(),
            CancellationToken.None);

        // Verify message is still captured
        var messages = await provider.ReadAndCaptureAsync("proc-2", CancellationToken.None);
        messages.Should().BeEmpty("message should still be captured");

        // Verify group is still locked
        var messageId2 = Guid.NewGuid();
        await InsertMessageAsync(provider, messageId2, groupId);
        var messages2 = await provider.ReadAndCaptureAsync("proc-2", CancellationToken.None);
        messages2.Should().BeEmpty("group should still be locked");
    }

    #endregion

    #region ReadAndCaptureAsync Tests (FIFO-specific: respects group locking)

    [Fact]
    public async Task ReadAndCaptureAsync_WithGroupLock_SkipsLockedGroup()
    {
        var provider = CreateFifoProviderAsync();

        var lockedGroupId = "locked-group";
        var unlockedGroupId = "unlocked-group";

        // Insert and capture message from first group to lock it
        var lockedMsg = Guid.NewGuid();
        await InsertMessageAsync(provider, lockedMsg, lockedGroupId);
        await provider.ReadAndCaptureAsync("other-proc", CancellationToken.None);

        // Insert message from second group
        var unlockedMsg = Guid.NewGuid();
        await InsertMessageAsync(provider, unlockedMsg, unlockedGroupId);

        // Try to capture as different processor
        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        messages.Should().HaveCount(1);
        messages[0].GroupId.Should().Be(unlockedGroupId);
        messages[0].Id.Should().Be(unlockedMsg);
    }

    [Fact]
    public async Task ReadAndCaptureAsync_MultipleMessagesFromSameGroup_CapturesAllInOneBatch()
    {
        var provider = CreateFifoProviderAsync();
        var groupId = "group-1";

        // Insert multiple messages for same group
        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();
        var id3 = Guid.NewGuid();

        await InsertMessageAsync(provider, id1, groupId, receivedAt: DateTime.UtcNow.AddSeconds(-3));
        await InsertMessageAsync(provider, id2, groupId, receivedAt: DateTime.UtcNow.AddSeconds(-2));
        await InsertMessageAsync(provider, id3, groupId, receivedAt: DateTime.UtcNow.AddSeconds(-1));

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        // All messages from the same group should be captured in one batch
        messages.Should().HaveCount(3);
        messages[0].Id.Should().Be(id1, "oldest message should be first");
        messages[1].Id.Should().Be(id2);
        messages[2].Id.Should().Be(id3);

        messages.Should().AllSatisfy(m =>
        {
            m.GroupId.Should().Be(groupId);
            m.CapturedBy.Should().Be("processor-1");
            m.CapturedAt.Should().NotBeNull();
        });
    }

    [Fact]
    public async Task ReadAndCaptureAsync_DifferentGroups_CanBeCapturedTogether()
    {
        var provider = CreateFifoProviderAsync();

        // Insert messages from 3 different groups
        var groups = new[] { "group-0", "group-1", "group-2" };
        var messageIds = new List<Guid>();

        foreach (var group in groups)
        {
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            messageIds.Add(id1);
            messageIds.Add(id2);
            await InsertMessageAsync(provider, id1, group, receivedAt: DateTime.UtcNow.AddSeconds(-2));
            await InsertMessageAsync(provider, id2, group, receivedAt: DateTime.UtcNow.AddSeconds(-1));
        }

        var messages = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        // All messages from all groups should be captured
        messages.Should().HaveCount(6);
        messages.GroupBy(m => m.GroupId).Should().HaveCount(3);
        messages.Select(m => m.Id).Should().BeEquivalentTo(messageIds);
    }

    [Fact]
    public async Task ReadAndCaptureAsync_ExpiredGroupLock_AllowsRecapture()
    {
        var configuration = CreateConfiguration(maxProcessingTime: TimeSpan.FromSeconds(1));
        var provider = CreateFifoProviderAsync(configuration);
        var groupId = "group-1";
        var messageId = Guid.NewGuid();

        await InsertMessageAsync(provider, messageId, groupId);

        // First capture locks the group
        var captured1 = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        captured1.Should().HaveCount(1);

        // Release message but not group lock (simulating message processing without group release)
        await provider.ReleaseAsync(messageId, CancellationToken.None);

        // Try to capture immediately - should be blocked by group lock
        var captured2 = await provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        captured2.Should().BeEmpty("group lock should still be active");

        // Wait for group lock to expire
        await Task.Delay(TimeSpan.FromSeconds(1.5));

        // Now should be able to capture
        var captured3 = await provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        captured3.Should().HaveCount(1);
        captured3[0].Id.Should().Be(messageId);
        captured3[0].CapturedBy.Should().Be("processor-2");
    }

    [Fact]
    public async Task ReadAndCaptureAsync_MessagesWithoutGroupId_NotSubjectToGroupLocking()
    {
        var provider = CreateFifoProviderAsync();

        // Insert message with group
        var groupedMsg = Guid.NewGuid();
        await InsertMessageAsync(provider, groupedMsg, "group-1");

        // Insert messages without group
        var ungroupedMsg1 = Guid.NewGuid();
        var ungroupedMsg2 = Guid.NewGuid();
        await InsertMessageAsync(provider, ungroupedMsg1, groupId: null);
        await InsertMessageAsync(provider, ungroupedMsg2, groupId: null);

        // Capture by first processor
        var captured1 = await provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        captured1.Should().HaveCount(3);

        // Release only ungrouped messages
        await provider.ReleaseAsync(ungroupedMsg1, CancellationToken.None);
        await provider.ReleaseAsync(ungroupedMsg2, CancellationToken.None);

        // Try to capture by second processor
        var captured2 = await provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);

        // Should capture ungrouped messages but not grouped (group still locked)
        captured2.Should().HaveCount(2);
        captured2.Select(m => m.Id).Should().Contain([ungroupedMsg1, ungroupedMsg2]);
        captured2.Should().NotContain(m => m.Id == groupedMsg);
    }

    #endregion

    #region Helper Methods

    private IProviderOptionsAccessor CreateMockOptionsAccessor()
    {
        var options = new InMemoryInboxProviderOptions
        {
            DeduplicationStore = new InMemoryDeduplicationStore(),
            DeadLetterStore = new InMemoryDeadLetterStore()
        };

        var mock = Substitute.For<IProviderOptionsAccessor>();
        mock.GetForInbox(InboxName).Returns(options);
        return mock;
    }

    private IInboxConfiguration CreateConfiguration(
        int batchSize = 100,
        TimeSpan? maxProcessingTime = null)
    {
        var options = Substitute.For<IInboxOptions>();
        options.InboxName.Returns(InboxName);
        options.ReadBatchSize.Returns(batchSize);
        options.MaxProcessingTime.Returns(maxProcessingTime ?? TimeSpan.FromMinutes(5));
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

    private InMemoryInboxStorageProvider CreateFifoProviderAsync(IInboxConfiguration? configuration = null)
    {
        var config = configuration ?? CreateConfiguration();
        var optionsAccessor = CreateMockOptionsAccessor();
        return new InMemoryInboxStorageProvider(optionsAccessor, config);
    }

    private async Task InsertMessageAsync(
        InMemoryInboxStorageProvider provider,
        Guid id,
        string? groupId,
        DateTime? receivedAt = null)
    {
        var message = new InboxMessage
        {
            Id = id,
            MessageType = "TestMessage",
            Payload = "{}",
            GroupId = groupId,
            ReceivedAt = receivedAt ?? DateTime.UtcNow,
            AttemptsCount = 0,
            CapturedAt = null,
            CapturedBy = null
        };

        await provider.WriteAsync(message, CancellationToken.None);
    }

    private async Task InsertAndCaptureMessageAsync(
        InMemoryInboxStorageProvider provider,
        Guid id,
        string? groupId,
        DateTime capturedAt,
        string capturedBy)
    {
        // First insert the message
        await InsertMessageAsync(provider, id, groupId);

        // Then capture it using ReadAndCaptureAsync to properly lock the group
        var captured = await provider.ReadAndCaptureAsync(capturedBy, CancellationToken.None);

        // Now we need to modify the captured timestamp using reflection to simulate older captures
        // Get the internal _messages field
        var messagesField = typeof(InMemoryInboxStorageProvider)
            .GetField("_messages", BindingFlags.NonPublic | BindingFlags.Instance);

        if (messagesField != null)
        {
            var messages = messagesField.GetValue(provider);
            var tryGetValueMethod = messages!.GetType().GetMethod("TryGetValue");

            var parameters = new object?[] { id, null };
            var found = (bool)tryGetValueMethod!.Invoke(messages, parameters)!;

            if (found && parameters[1] is InboxMessage message)
            {
                message.CapturedAt = capturedAt;
            }
        }

        // Also update the group lock timestamp if there's a group
        if (!string.IsNullOrEmpty(groupId))
        {
            var lockedGroupsField = typeof(InMemoryInboxStorageProvider)
                .GetField("_lockedGroups", BindingFlags.NonPublic | BindingFlags.Instance);

            if (lockedGroupsField != null)
            {
                var lockedGroups = (Dictionary<string, DateTime>)lockedGroupsField.GetValue(provider)!;
                if (lockedGroups.ContainsKey(groupId))
                {
                    lockedGroups[groupId] = capturedAt;
                }
            }
        }
    }

    private void ModifyMessageCaptureTime(InMemoryInboxStorageProvider provider, Guid messageId, DateTime capturedAt)
    {
        var messagesField = typeof(InMemoryInboxStorageProvider)
            .GetField("_messages", BindingFlags.NonPublic | BindingFlags.Instance);

        if (messagesField != null)
        {
            var messages = messagesField.GetValue(provider);
            var tryGetValueMethod = messages!.GetType().GetMethod("TryGetValue");

            var parameters = new object?[] { messageId, null };
            var found = (bool)tryGetValueMethod!.Invoke(messages, parameters)!;

            if (found && parameters[1] is InboxMessage message)
            {
                message.CapturedAt = capturedAt;
            }
        }
    }

    private void ModifyGroupLockTime(InMemoryInboxStorageProvider provider, string groupId, DateTime lockedAt)
    {
        var lockedGroupsField = typeof(InMemoryInboxStorageProvider)
            .GetField("_lockedGroups", BindingFlags.NonPublic | BindingFlags.Instance);

        if (lockedGroupsField != null)
        {
            var lockedGroups = (Dictionary<string, DateTime>)lockedGroupsField.GetValue(provider)!;
            if (lockedGroups.ContainsKey(groupId))
            {
                lockedGroups[groupId] = lockedAt;
            }
        }
    }

    private record MessageIdentifier(Guid Id, string? GroupId) : IInboxMessageIdentifiers;

    #endregion
}
