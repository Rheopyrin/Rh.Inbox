using FluentAssertions;
using NSubstitute;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.InMemory;
using Rh.Inbox.InMemory.Options;
using Xunit;

namespace Rh.Inbox.Tests.Unit.InMemory;

public class InMemoryInboxStorageProviderTests : IDisposable
{
    private readonly InMemoryDeduplicationStore _deduplicationStore;
    private readonly InMemoryDeadLetterStore _deadLetterStore;
    private readonly IInboxConfiguration _configuration;
    private readonly IProviderOptionsAccessor _optionsAccessor;
    private readonly IDateTimeProvider _dateTimeProvider;
    private readonly InMemoryInboxStorageProvider _provider;
    private readonly DateTime _now;

    public InMemoryInboxStorageProviderTests()
    {
        _now = DateTime.UtcNow;
        _deduplicationStore = new InMemoryDeduplicationStore();
        _deadLetterStore = new InMemoryDeadLetterStore();

        _dateTimeProvider = Substitute.For<IDateTimeProvider>();
        _dateTimeProvider.GetUtcNow().Returns(_now);

        var options = Substitute.For<IInboxOptions>();
        options.EnableDeduplication.Returns(true);
        options.DeduplicationInterval.Returns(TimeSpan.FromHours(1));
        options.EnableDeadLetter.Returns(true);
        options.MaxProcessingTime.Returns(TimeSpan.FromMinutes(5));
        options.ReadBatchSize.Returns(100);
        options.DeadLetterMaxMessageLifetime.Returns(TimeSpan.FromDays(7));

        _configuration = Substitute.For<IInboxConfiguration>();
        _configuration.InboxName.Returns("test-inbox");
        _configuration.Options.Returns(options);
        _configuration.DateTimeProvider.Returns(_dateTimeProvider);
        _configuration.InboxType.Returns(InboxType.Default);

        var providerOptions = new InMemoryInboxProviderOptions
        {
            DeduplicationStore = _deduplicationStore,
            DeadLetterStore = _deadLetterStore
        };
        _optionsAccessor = Substitute.For<IProviderOptionsAccessor>();
        _optionsAccessor.GetForInbox("test-inbox").Returns(providerOptions);

        _provider = new InMemoryInboxStorageProvider(_optionsAccessor, _configuration);
    }

    public void Dispose()
    {
        _provider.Dispose();
        GC.SuppressFinalize(this);
    }

    #region Write Tests

    [Fact]
    public async Task WriteAsync_SingleMessage_StoresMessage()
    {
        var message = CreateMessage();

        await _provider.WriteAsync(message, CancellationToken.None);

        var messages = await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        messages.Should().ContainSingle();
        messages[0].Id.Should().Be(message.Id);
    }

    [Fact]
    public async Task WriteAsync_WithDeduplication_SkipsDuplicates()
    {
        var message1 = CreateMessage(deduplicationId: "dup-key");
        var message2 = CreateMessage(deduplicationId: "dup-key");

        await _provider.WriteAsync(message1, CancellationToken.None);
        await _provider.WriteAsync(message2, CancellationToken.None);

        var messages = await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        messages.Should().ContainSingle();
    }

    [Fact]
    public async Task WriteAsync_WithCollapseKey_ReplacesExisting()
    {
        var message1 = CreateMessage(collapseKey: "collapse-1");
        var message2 = CreateMessage(collapseKey: "collapse-1");

        await _provider.WriteAsync(message1, CancellationToken.None);
        await _provider.WriteAsync(message2, CancellationToken.None);

        var messages = await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        messages.Should().ContainSingle();
        messages[0].Id.Should().Be(message2.Id);
    }

    [Fact]
    public async Task WriteBatchAsync_MultipleMessages_StoresAll()
    {
        var messages = new[]
        {
            CreateMessage(),
            CreateMessage(),
            CreateMessage()
        };

        await _provider.WriteBatchAsync(messages, CancellationToken.None);

        var result = await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        result.Should().HaveCount(3);
    }

    [Fact]
    public async Task WriteBatchAsync_WithPreviouslyWrittenDuplicates_FiltersThem()
    {
        // First write a message with deduplication key
        await _provider.WriteAsync(CreateMessage(deduplicationId: "dup-1"), CancellationToken.None);

        // Now write a batch with same deduplication key - should be filtered
        var messages = new[]
        {
            CreateMessage(deduplicationId: "dup-1"),
            CreateMessage(deduplicationId: "dup-2")
        };

        await _provider.WriteBatchAsync(messages, CancellationToken.None);

        var result = await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        result.Should().HaveCount(2); // Original dup-1 + new dup-2
    }

    #endregion

    #region ReadAndCapture Tests

    [Fact]
    public async Task ReadAndCaptureAsync_MarksMessagesAsCaptured()
    {
        var message = CreateMessage();
        await _provider.WriteAsync(message, CancellationToken.None);

        var result = await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        result.Should().ContainSingle();
        result[0].CapturedAt.Should().Be(_now);
        result[0].CapturedBy.Should().Be("processor-1");
    }

    [Fact]
    public async Task ReadAndCaptureAsync_SkipsAlreadyCaptured()
    {
        var message = CreateMessage();
        await _provider.WriteAsync(message, CancellationToken.None);

        // Capture once
        await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        // Try to capture again
        var result = await _provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        result.Should().BeEmpty();
    }

    [Fact]
    public async Task ReadAndCaptureAsync_ReturnsExpiredCapturedMessages()
    {
        var message = CreateMessage();
        await _provider.WriteAsync(message, CancellationToken.None);

        // Capture the message
        await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        // Advance time past MaxProcessingTime
        _dateTimeProvider.GetUtcNow().Returns(_now.AddMinutes(10));

        // Should recapture since lock expired
        var result = await _provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        result.Should().ContainSingle();
    }

    #endregion

    #region Complete Tests

    [Fact]
    public async Task CompleteAsync_RemovesMessage()
    {
        var message = CreateMessage();
        await _provider.WriteAsync(message, CancellationToken.None);
        await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        await _provider.CompleteAsync(message.Id, CancellationToken.None);

        var result = await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        result.Should().BeEmpty();
    }

    [Fact]
    public async Task CompleteBatchAsync_RemovesAllMessages()
    {
        var messages = new[] { CreateMessage(), CreateMessage(), CreateMessage() };
        await _provider.WriteBatchAsync(messages, CancellationToken.None);
        await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        await _provider.CompleteBatchAsync(messages.Select(m => m.Id).ToList(), CancellationToken.None);

        var result = await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        result.Should().BeEmpty();
    }

    [Fact]
    public async Task CompleteBatchAsync_WithEmptyList_DoesNothing()
    {
        var act = async () => await _provider.CompleteBatchAsync([], CancellationToken.None);
        await act.Should().NotThrowAsync();
    }

    #endregion

    #region Fail Tests

    [Fact]
    public async Task FailAsync_IncrementsAttemptsAndReleasesMessage()
    {
        var message = CreateMessage();
        await _provider.WriteAsync(message, CancellationToken.None);
        await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        await _provider.FailAsync(message.Id, CancellationToken.None);

        var result = await _provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        result.Should().ContainSingle();
        result[0].AttemptsCount.Should().Be(1);
        result[0].CapturedAt.Should().NotBeNull();
    }

    [Fact]
    public async Task FailBatchAsync_ProcessesAllMessages()
    {
        var messages = new[] { CreateMessage(), CreateMessage() };
        await _provider.WriteBatchAsync(messages, CancellationToken.None);
        await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        await _provider.FailBatchAsync(messages.Select(m => m.Id).ToList(), CancellationToken.None);

        var result = await _provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        result.Should().HaveCount(2);
        result.All(m => m.AttemptsCount == 1).Should().BeTrue();
    }

    [Fact]
    public async Task FailBatchAsync_WithEmptyList_DoesNothing()
    {
        var act = async () => await _provider.FailBatchAsync([], CancellationToken.None);
        await act.Should().NotThrowAsync();
    }

    #endregion

    #region Release Tests

    [Fact]
    public async Task ReleaseAsync_MakesMessageAvailable()
    {
        var message = CreateMessage();
        await _provider.WriteAsync(message, CancellationToken.None);
        await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        await _provider.ReleaseAsync(message.Id, CancellationToken.None);

        var result = await _provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        result.Should().ContainSingle();
        result[0].AttemptsCount.Should().Be(0);
    }

    [Fact]
    public async Task ReleaseBatchAsync_ReleasesAllMessages()
    {
        var messages = new[] { CreateMessage(), CreateMessage() };
        await _provider.WriteBatchAsync(messages, CancellationToken.None);
        await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        await _provider.ReleaseBatchAsync(messages.Select(m => m.Id).ToList(), CancellationToken.None);

        var result = await _provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        result.Should().HaveCount(2);
    }

    [Fact]
    public async Task ReleaseBatchAsync_WithEmptyList_DoesNothing()
    {
        var act = async () => await _provider.ReleaseBatchAsync([], CancellationToken.None);
        await act.Should().NotThrowAsync();
    }

    #endregion

    #region MoveToDeadLetter Tests

    [Fact]
    public async Task MoveToDeadLetterAsync_TransfersMessageToDeadLetter()
    {
        var message = CreateMessage();
        await _provider.WriteAsync(message, CancellationToken.None);
        await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        await _provider.MoveToDeadLetterAsync(message.Id, "test failure", CancellationToken.None);

        var result = await _provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        result.Should().BeEmpty();

        var deadLetters = await _provider.ReadDeadLettersAsync(10, CancellationToken.None);
        deadLetters.Should().ContainSingle();
        deadLetters[0].FailureReason.Should().Be("test failure");
    }

    [Fact]
    public async Task MoveToDeadLetterBatchAsync_TransfersAllMessages()
    {
        var messages = new[] { CreateMessage(), CreateMessage() };
        await _provider.WriteBatchAsync(messages, CancellationToken.None);
        await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        var deadLetterRequests = messages.Select(m => (m.Id, "failure")).ToList();
        await _provider.MoveToDeadLetterBatchAsync(deadLetterRequests, CancellationToken.None);

        var deadLetters = await _provider.ReadDeadLettersAsync(10, CancellationToken.None);
        deadLetters.Should().HaveCount(2);
    }

    [Fact]
    public async Task MoveToDeadLetterBatchAsync_WithEmptyList_DoesNothing()
    {
        var act = async () => await _provider.MoveToDeadLetterBatchAsync([], CancellationToken.None);
        await act.Should().NotThrowAsync();
    }

    #endregion

    #region ProcessResultsBatch Tests

    [Fact]
    public async Task ProcessResultsBatchAsync_HandlesAllOperations()
    {
        var messages = Enumerable.Range(0, 4).Select(_ => CreateMessage()).ToArray();
        await _provider.WriteBatchAsync(messages, CancellationToken.None);
        await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        await _provider.ProcessResultsBatchAsync(
            toComplete: [messages[0].Id],
            toFail: [messages[1].Id],
            toRelease: [messages[2].Id],
            toDeadLetter: [(messages[3].Id, "dead letter reason")],
            CancellationToken.None);

        var result = await _provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        result.Should().HaveCount(2); // failed + released
    }

    [Fact]
    public async Task ProcessResultsBatchAsync_WithNoWork_DoesNothing()
    {
        var act = async () => await _provider.ProcessResultsBatchAsync(
            [], [], [], [], CancellationToken.None);
        await act.Should().NotThrowAsync();
    }

    #endregion

    #region Health Metrics Tests

    [Fact]
    public async Task GetHealthMetricsAsync_ReturnsPendingCount()
    {
        await _provider.WriteAsync(CreateMessage(), CancellationToken.None);
        await _provider.WriteAsync(CreateMessage(), CancellationToken.None);

        var metrics = await _provider.GetHealthMetricsAsync(CancellationToken.None);

        metrics.PendingCount.Should().Be(2);
        metrics.CapturedCount.Should().Be(0);
    }

    [Fact]
    public async Task GetHealthMetricsAsync_ReturnsCapturedCount()
    {
        await _provider.WriteAsync(CreateMessage(), CancellationToken.None);
        await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        var metrics = await _provider.GetHealthMetricsAsync(CancellationToken.None);

        metrics.PendingCount.Should().Be(0);
        metrics.CapturedCount.Should().Be(1);
    }

    [Fact]
    public async Task GetHealthMetricsAsync_ReturnsDeadLetterCount()
    {
        var message = CreateMessage();
        await _provider.WriteAsync(message, CancellationToken.None);
        await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        await _provider.MoveToDeadLetterAsync(message.Id, "failed", CancellationToken.None);

        var metrics = await _provider.GetHealthMetricsAsync(CancellationToken.None);

        metrics.DeadLetterCount.Should().Be(1);
    }

    [Fact]
    public async Task GetHealthMetricsAsync_ReturnsOldestPendingMessageAt()
    {
        var oldMessage = CreateMessage(receivedAt: _now.AddMinutes(-10));
        await _provider.WriteAsync(oldMessage, CancellationToken.None);

        var newMessage = CreateMessage(receivedAt: _now);
        await _provider.WriteAsync(newMessage, CancellationToken.None);

        var metrics = await _provider.GetHealthMetricsAsync(CancellationToken.None);

        metrics.OldestPendingMessageAt.Should().Be(_now.AddMinutes(-10));
    }

    #endregion

    #region ExtendLocks Tests

    [Fact]
    public async Task ExtendLocksAsync_ExtendsMessageLocks()
    {
        var message = CreateMessage();
        await _provider.WriteAsync(message, CancellationToken.None);
        var captured = await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        var newTime = _now.AddMinutes(3);
        var identifiers = captured.Cast<IInboxMessageIdentifiers>().ToList();
        var extended = await _provider.ExtendLocksAsync("processor-1", identifiers, newTime, CancellationToken.None);

        extended.Should().Be(1);
    }

    [Fact]
    public async Task ExtendLocksAsync_WithEmptyList_ReturnsZero()
    {
        var result = await _provider.ExtendLocksAsync("processor-1", [], _now, CancellationToken.None);
        result.Should().Be(0);
    }

    [Fact]
    public async Task ExtendLocksAsync_WithWrongProcessor_DoesNotExtend()
    {
        var message = CreateMessage();
        await _provider.WriteAsync(message, CancellationToken.None);
        var captured = await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        var identifiers = captured.Cast<IInboxMessageIdentifiers>().ToList();
        var extended = await _provider.ExtendLocksAsync("different-processor", identifiers, _now.AddMinutes(3), CancellationToken.None);

        extended.Should().Be(0);
    }

    [Fact]
    public async Task ExtendLocksAsync_MultipleMessages_ExtendsAll()
    {
        var messages = new[] { CreateMessage(), CreateMessage(), CreateMessage() };
        await _provider.WriteBatchAsync(messages, CancellationToken.None);
        var captured = await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        var newTime = _now.AddMinutes(3);
        var identifiers = captured.Cast<IInboxMessageIdentifiers>().ToList();
        var extended = await _provider.ExtendLocksAsync("processor-1", identifiers, newTime, CancellationToken.None);

        extended.Should().Be(3);
    }

    [Fact]
    public async Task ExtendLocksAsync_WithGroupId_ExtendsGroupLock()
    {
        _configuration.InboxType.Returns(InboxType.Fifo);

        var message = CreateMessage(groupId: "group-1");
        await _provider.WriteAsync(message, CancellationToken.None);
        var captured = await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        var newTime = _now.AddMinutes(3);
        var identifiers = captured.Cast<IInboxMessageIdentifiers>().ToList();
        var extended = await _provider.ExtendLocksAsync("processor-1", identifiers, newTime, CancellationToken.None);

        extended.Should().Be(1);

        // Add new message to the group - should still be locked by processor-1
        await _provider.WriteAsync(CreateMessage(groupId: "group-1"), CancellationToken.None);

        // Advance time past original max processing time but before extended time
        _dateTimeProvider.GetUtcNow().Returns(_now.AddMinutes(6));

        // Group should still be locked (was extended)
        var result = await _provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        result.Should().BeEmpty();
    }

    [Fact]
    public async Task ExtendLocksAsync_WithNonCapturedMessage_DoesNotExtend()
    {
        var message = CreateMessage();
        await _provider.WriteAsync(message, CancellationToken.None);

        // Create identifiers for a message that hasn't been captured
        var identifiers = new List<IInboxMessageIdentifiers> { message };
        var extended = await _provider.ExtendLocksAsync("processor-1", identifiers, _now.AddMinutes(3), CancellationToken.None);

        extended.Should().Be(0);
    }

    [Fact]
    public async Task ExtendLocksAsync_AfterMessageComplete_DoesNotExtend()
    {
        var message = CreateMessage();
        await _provider.WriteAsync(message, CancellationToken.None);
        var captured = await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        // Complete the message
        await _provider.CompleteAsync(message.Id, CancellationToken.None);

        // Try to extend lock for completed message
        var identifiers = captured.Cast<IInboxMessageIdentifiers>().ToList();
        var extended = await _provider.ExtendLocksAsync("processor-1", identifiers, _now.AddMinutes(3), CancellationToken.None);

        extended.Should().Be(0);
    }

    #endregion

    #region FIFO Group Locking Tests

    [Fact]
    public async Task FifoMode_GroupsAreLocked_OtherGroupsCanProcess()
    {
        // Configure for FIFO mode
        _configuration.InboxType.Returns(InboxType.Fifo);

        var group1Message = CreateMessage(groupId: "group-1");
        var group2Message = CreateMessage(groupId: "group-2");
        await _provider.WriteAsync(group1Message, CancellationToken.None);
        await _provider.WriteAsync(group2Message, CancellationToken.None);

        // First processor captures both groups
        var result1 = await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        result1.Should().HaveCount(2);

        // Add more messages to both groups
        await _provider.WriteAsync(CreateMessage(groupId: "group-1"), CancellationToken.None);
        await _provider.WriteAsync(CreateMessage(groupId: "group-2"), CancellationToken.None);

        // Second processor should not be able to capture locked groups
        var result2 = await _provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        result2.Should().BeEmpty();
    }

    [Fact]
    public async Task FifoMode_ReleaseGroupLocksAsync_UnlocksGroups()
    {
        _configuration.InboxType.Returns(InboxType.Fifo);

        var message = CreateMessage(groupId: "group-1");
        await _provider.WriteAsync(message, CancellationToken.None);
        await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        // Release the group lock
        await _provider.ReleaseGroupLocksAsync(["group-1"], CancellationToken.None);

        // Add a new message to the group
        await _provider.WriteAsync(CreateMessage(groupId: "group-1"), CancellationToken.None);

        // Another processor should now be able to capture the new message
        var result = await _provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        result.Should().ContainSingle();
    }

    [Fact]
    public async Task FifoMode_ReleaseGroupLocksAsync_WithEmptyList_DoesNothing()
    {
        _configuration.InboxType.Returns(InboxType.Fifo);
        var act = async () => await _provider.ReleaseGroupLocksAsync([], CancellationToken.None);
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task FifoMode_ReleaseMessagesAndGroupLocksAsync_ReleasesAll()
    {
        _configuration.InboxType.Returns(InboxType.Fifo);

        var message = CreateMessage(groupId: "group-1");
        await _provider.WriteAsync(message, CancellationToken.None);
        var captured = await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        var identifiers = captured.Cast<IInboxMessageIdentifiers>().ToList();
        await _provider.ReleaseMessagesAndGroupLocksAsync(identifiers, CancellationToken.None);

        // The message should be available again
        var result = await _provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        result.Should().ContainSingle();
    }

    [Fact]
    public async Task FifoMode_ReleaseMessagesAndGroupLocksAsync_WithEmptyList_DoesNothing()
    {
        _configuration.InboxType.Returns(InboxType.Fifo);
        var act = async () => await _provider.ReleaseMessagesAndGroupLocksAsync([], CancellationToken.None);
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task FifoMode_ExpiredGroupLocks_AreReleased()
    {
        _configuration.InboxType.Returns(InboxType.Fifo);

        var message = CreateMessage(groupId: "group-1");
        await _provider.WriteAsync(message, CancellationToken.None);
        await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        // Complete the first message so only the new message remains
        await _provider.CompleteAsync(message.Id, CancellationToken.None);

        // Add a new message
        await _provider.WriteAsync(CreateMessage(groupId: "group-1"), CancellationToken.None);

        // Advance time past MaxProcessingTime (to expire the group lock)
        _dateTimeProvider.GetUtcNow().Returns(_now.AddMinutes(10));

        // The new message should now be capturable (group lock expired)
        var result = await _provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        result.Should().ContainSingle();
    }

    [Fact]
    public async Task FifoMode_ReleaseGroupLocksAsync_MultipleGroups_ReleasesAll()
    {
        _configuration.InboxType.Returns(InboxType.Fifo);

        // Create messages for multiple groups
        var msg1 = CreateMessage(groupId: "group-1");
        var msg2 = CreateMessage(groupId: "group-2");
        var msg3 = CreateMessage(groupId: "group-3");
        await _provider.WriteAsync(msg1, CancellationToken.None);
        await _provider.WriteAsync(msg2, CancellationToken.None);
        await _provider.WriteAsync(msg3, CancellationToken.None);

        // Capture all messages
        await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        // Add new messages to all groups
        await _provider.WriteAsync(CreateMessage(groupId: "group-1"), CancellationToken.None);
        await _provider.WriteAsync(CreateMessage(groupId: "group-2"), CancellationToken.None);
        await _provider.WriteAsync(CreateMessage(groupId: "group-3"), CancellationToken.None);

        // Release group locks for groups 1 and 2 only
        await _provider.ReleaseGroupLocksAsync(["group-1", "group-2"], CancellationToken.None);

        // Another processor should capture messages from groups 1 and 2, but not group 3
        var result = await _provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        result.Should().HaveCount(2);
        result.All(m => m.GroupId == "group-1" || m.GroupId == "group-2").Should().BeTrue();
    }

    [Fact]
    public async Task FifoMode_ReleaseMessagesAndGroupLocksAsync_MultipleGroups_ReleasesAll()
    {
        _configuration.InboxType.Returns(InboxType.Fifo);

        var msg1 = CreateMessage(groupId: "group-1");
        var msg2 = CreateMessage(groupId: "group-2");
        await _provider.WriteAsync(msg1, CancellationToken.None);
        await _provider.WriteAsync(msg2, CancellationToken.None);

        var captured = await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        // Release all captured messages and their groups
        var identifiers = captured.Cast<IInboxMessageIdentifiers>().ToList();
        await _provider.ReleaseMessagesAndGroupLocksAsync(identifiers, CancellationToken.None);

        // Both messages should be available again
        var result = await _provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        result.Should().HaveCount(2);
    }

    [Fact]
    public async Task FifoMode_ReleaseMessagesAndGroupLocksAsync_WithNoGroups_ReleasesMessagesOnly()
    {
        // Messages without group_id
        var msg1 = CreateMessage();
        var msg2 = CreateMessage();
        await _provider.WriteAsync(msg1, CancellationToken.None);
        await _provider.WriteAsync(msg2, CancellationToken.None);

        var captured = await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);

        var identifiers = captured.Cast<IInboxMessageIdentifiers>().ToList();
        await _provider.ReleaseMessagesAndGroupLocksAsync(identifiers, CancellationToken.None);

        var result = await _provider.ReadAndCaptureAsync("processor-2", CancellationToken.None);
        result.Should().HaveCount(2);
    }

    [Fact]
    public async Task FifoMode_ReleaseGroupLocks_NonExistentGroup_DoesNotThrow()
    {
        _configuration.InboxType.Returns(InboxType.Fifo);

        var act = async () => await _provider.ReleaseGroupLocksAsync(["non-existent-group"], CancellationToken.None);
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task FifoMode_MultipleMessagesFromSameGroup_CanBeCapturedByOneProcessor()
    {
        _configuration.InboxType.Returns(InboxType.Fifo);

        // Write multiple messages to the same group
        await _provider.WriteAsync(CreateMessage(groupId: "group-1"), CancellationToken.None);
        await _provider.WriteAsync(CreateMessage(groupId: "group-1"), CancellationToken.None);
        await _provider.WriteAsync(CreateMessage(groupId: "group-1"), CancellationToken.None);

        // One processor should capture all messages from the group
        var result = await _provider.ReadAndCaptureAsync("processor-1", CancellationToken.None);
        result.Should().HaveCount(3);
        result.All(m => m.GroupId == "group-1").Should().BeTrue();
    }

    #endregion

    #region Interface Implementation Tests

    [Fact]
    public void Provider_ImplementsIInboxStorageProvider()
    {
        _provider.Should().BeAssignableTo<IInboxStorageProvider>();
    }

    [Fact]
    public void Provider_ImplementsISupportHealthCheck()
    {
        _provider.Should().BeAssignableTo<ISupportHealthCheck>();
    }

    [Fact]
    public void Provider_ImplementsISupportGroupLocksReleaseStorageProvider()
    {
        _provider.Should().BeAssignableTo<ISupportGroupLocksReleaseStorageProvider>();
    }

    [Fact]
    public void Provider_ImplementsIDisposable()
    {
        _provider.Should().BeAssignableTo<IDisposable>();
    }

    #endregion

    #region Dispose Tests

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var provider = new InMemoryInboxStorageProvider(_optionsAccessor, _configuration);
        provider.Dispose();
        var act = () => provider.Dispose();
        act.Should().NotThrow();
    }

    #endregion

    #region Helper Methods

    private InboxMessage CreateMessage(
        string? deduplicationId = null,
        string? collapseKey = null,
        string? groupId = null,
        DateTime? receivedAt = null)
    {
        return new InboxMessage
        {
            Id = Guid.NewGuid(),
            MessageType = "test.message",
            Payload = "test payload",
            DeduplicationId = deduplicationId,
            CollapseKey = collapseKey,
            GroupId = groupId,
            ReceivedAt = receivedAt ?? _now,
            AttemptsCount = 0
        };
    }

    #endregion
}
