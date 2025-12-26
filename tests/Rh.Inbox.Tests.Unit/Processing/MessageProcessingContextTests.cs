using FluentAssertions;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Processing;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Processing;

public class MessageProcessingContextTests
{
    private readonly IInboxStorageProvider _storageProvider;
    private readonly IInboxOptions _options;
    private readonly ILogger _logger;

    public MessageProcessingContextTests()
    {
        _storageProvider = Substitute.For<IInboxStorageProvider>();
        _options = Substitute.For<IInboxOptions>();
        _options.MaxAttempts.Returns(3);
        _logger = Substitute.For<ILogger>();
    }

    private MessageProcessingContext CreateContext(IReadOnlyList<InboxMessage> messages)
    {
        return new MessageProcessingContext(_storageProvider, _options, _logger, messages);
    }

    private static InboxMessage CreateMessage(int attemptsCount = 0)
    {
        return new InboxMessage
        {
            Id = Guid.NewGuid(),
            MessageType = "TestMessage",
            Payload = "{}",
            ReceivedAt = DateTime.UtcNow,
            AttemptsCount = attemptsCount
        };
    }

    #region GetInFlightMessages Tests

    [Fact]
    public void GetInFlightMessages_InitiallyReturnsAllMessages()
    {
        var messages = new[] { CreateMessage(), CreateMessage(), CreateMessage() };
        var context = CreateContext(messages);

        var inFlight = context.GetInFlightMessages();

        inFlight.Should().HaveCount(3);
        inFlight.Should().Contain(messages);
    }

    [Fact]
    public void GetInFlightMessages_EmptyList_ReturnsEmpty()
    {
        var context = CreateContext(Array.Empty<InboxMessage>());

        var inFlight = context.GetInFlightMessages();

        inFlight.Should().BeEmpty();
    }

    #endregion

    #region ProcessResultsBatchAsync Tests

    [Fact]
    public async Task ProcessResultsBatchAsync_RemovesProcessedMessagesFromInFlight()
    {
        var message1 = CreateMessage();
        var message2 = CreateMessage();
        var message3 = CreateMessage();
        var messages = new[] { message1, message2, message3 };
        var context = CreateContext(messages);

        var results = new[]
        {
            new InboxMessageResult(message1.Id, InboxHandleResult.Success),
            new InboxMessageResult(message2.Id, InboxHandleResult.Failed),
            new InboxMessageResult(message3.Id, InboxHandleResult.Retry)
        };

        await context.ProcessResultsBatchAsync(results, CancellationToken.None);

        var inFlight = context.GetInFlightMessages();
        inFlight.Should().BeEmpty();
    }

    [Fact]
    public async Task ProcessResultsBatchAsync_PartialProcess_OnlyRemovesProcessedMessages()
    {
        var message1 = CreateMessage();
        var message2 = CreateMessage();
        var messages = new[] { message1, message2 };
        var context = CreateContext(messages);

        var results = new[] { new InboxMessageResult(message1.Id, InboxHandleResult.Success) };

        await context.ProcessResultsBatchAsync(results, CancellationToken.None);

        var inFlight = context.GetInFlightMessages();
        inFlight.Should().HaveCount(1);
        inFlight.Should().Contain(message2);
    }

    [Fact]
    public async Task ProcessResultsBatchAsync_MaxAttemptsExceeded_MovesToDeadLetter()
    {
        var message = CreateMessage(attemptsCount: 2); // Next attempt exceeds MaxAttempts=3
        var messages = new[] { message };
        var context = CreateContext(messages);

        var results = new[] { new InboxMessageResult(message.Id, InboxHandleResult.Failed) };

        await context.ProcessResultsBatchAsync(results, CancellationToken.None);

        await _storageProvider.Received(1).ProcessResultsBatchAsync(
            Arg.Is<IReadOnlyList<Guid>>(l => l.Count == 0), // toComplete
            Arg.Is<IReadOnlyList<Guid>>(l => l.Count == 0), // toFail
            Arg.Is<IReadOnlyList<Guid>>(l => l.Count == 0), // toRelease
            Arg.Is<IReadOnlyList<(Guid, string)>>(l => l.Count == 1 && l[0].Item1 == message.Id), // toDeadLetter
            Arg.Any<CancellationToken>());
    }

    #endregion

    #region FailMessageAsync Tests

    [Fact]
    public async Task FailMessageAsync_RemovesMessageFromInFlight()
    {
        var message = CreateMessage();
        var context = CreateContext(new[] { message });

        await context.FailMessageAsync(message, CancellationToken.None);

        var inFlight = context.GetInFlightMessages();
        inFlight.Should().BeEmpty();
    }

    [Fact]
    public async Task FailMessageAsync_MaxAttemptsNotExceeded_CallsFailAsync()
    {
        var message = CreateMessage(attemptsCount: 0); // attemptsCount + 1 = 1 < MaxAttempts=3
        var context = CreateContext(new[] { message });

        await context.FailMessageAsync(message, CancellationToken.None);

        await _storageProvider.Received(1).FailAsync(message.Id, Arg.Any<CancellationToken>());
        await _storageProvider.DidNotReceive().MoveToDeadLetterAsync(Arg.Any<Guid>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task FailMessageAsync_MaxAttemptsExceeded_MovesToDeadLetter()
    {
        var message = CreateMessage(attemptsCount: 2); // attemptsCount + 1 = 3 >= MaxAttempts=3
        var context = CreateContext(new[] { message });

        await context.FailMessageAsync(message, CancellationToken.None);

        await _storageProvider.DidNotReceive().FailAsync(Arg.Any<Guid>(), Arg.Any<CancellationToken>());
        await _storageProvider.Received(1).MoveToDeadLetterAsync(
            message.Id,
            Arg.Is<string>(s => s.Contains("Max attempts")),
            Arg.Any<CancellationToken>());
    }

    #endregion

    #region FailMessageBatchAsync Tests

    [Fact]
    public async Task FailMessageBatchAsync_RemovesAllMessagesFromInFlight()
    {
        var messages = new[] { CreateMessage(), CreateMessage(), CreateMessage() };
        var context = CreateContext(messages);

        await context.FailMessageBatchAsync(messages, CancellationToken.None);

        var inFlight = context.GetInFlightMessages();
        inFlight.Should().BeEmpty();
    }

    [Fact]
    public async Task FailMessageBatchAsync_MixedAttempts_SeparatesFailAndDeadLetter()
    {
        var message1 = CreateMessage(attemptsCount: 0); // Will fail
        var message2 = CreateMessage(attemptsCount: 2); // Will go to dead letter
        var messages = new[] { message1, message2 };
        var context = CreateContext(messages);

        await context.FailMessageBatchAsync(messages, CancellationToken.None);

        await _storageProvider.Received(1).FailBatchAsync(
            Arg.Is<IReadOnlyList<Guid>>(l => l.Count == 1 && l.Contains(message1.Id)),
            Arg.Any<CancellationToken>());
        await _storageProvider.Received(1).MoveToDeadLetterBatchAsync(
            Arg.Is<IReadOnlyList<(Guid, string)>>(l => l.Count == 1 && l[0].Item1 == message2.Id),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task FailMessageBatchAsync_EmptyList_DoesNothing()
    {
        var context = CreateContext(Array.Empty<InboxMessage>());

        await context.FailMessageBatchAsync(Array.Empty<InboxMessage>(), CancellationToken.None);

        await _storageProvider.DidNotReceive().FailBatchAsync(Arg.Any<IReadOnlyList<Guid>>(), Arg.Any<CancellationToken>());
        await _storageProvider.DidNotReceive().MoveToDeadLetterBatchAsync(Arg.Any<IReadOnlyList<(Guid, string)>>(), Arg.Any<CancellationToken>());
    }

    #endregion

    #region MoveToDeadLetterAsync Tests

    [Fact]
    public async Task MoveToDeadLetterAsync_RemovesMessageFromInFlight()
    {
        var message = CreateMessage();
        var context = CreateContext(new[] { message });

        await context.MoveToDeadLetterAsync(message, "Test reason", CancellationToken.None);

        var inFlight = context.GetInFlightMessages();
        inFlight.Should().BeEmpty();
    }

    [Fact]
    public async Task MoveToDeadLetterAsync_CallsStorageProvider()
    {
        var message = CreateMessage();
        var context = CreateContext(new[] { message });

        await context.MoveToDeadLetterAsync(message, "Custom reason", CancellationToken.None);

        await _storageProvider.Received(1).MoveToDeadLetterAsync(message.Id, "Custom reason", Arg.Any<CancellationToken>());
    }

    #endregion

    #region MoveToDeadLetterBatchAsync Tests

    [Fact]
    public async Task MoveToDeadLetterBatchAsync_RemovesAllMessagesFromInFlight()
    {
        var message1 = CreateMessage();
        var message2 = CreateMessage();
        var messages = new[] { message1, message2 };
        var context = CreateContext(messages);

        var batch = new[] { (message1, "Reason 1"), (message2, "Reason 2") };
        await context.MoveToDeadLetterBatchAsync(batch, CancellationToken.None);

        var inFlight = context.GetInFlightMessages();
        inFlight.Should().BeEmpty();
    }

    [Fact]
    public async Task MoveToDeadLetterBatchAsync_EmptyList_DoesNothing()
    {
        var context = CreateContext(Array.Empty<InboxMessage>());

        await context.MoveToDeadLetterBatchAsync(Array.Empty<(InboxMessage, string)>(), CancellationToken.None);

        await _storageProvider.DidNotReceive().MoveToDeadLetterBatchAsync(Arg.Any<IReadOnlyList<(Guid, string)>>(), Arg.Any<CancellationToken>());
    }

    #endregion

    #region Clear Tests

    [Fact]
    public void Clear_RemovesAllMessagesFromInFlight()
    {
        var messages = new[] { CreateMessage(), CreateMessage() };
        var context = CreateContext(messages);

        context.Clear();

        var inFlight = context.GetInFlightMessages();
        inFlight.Should().BeEmpty();
    }

    #endregion

    #region Thread Safety Tests

    [Fact]
    public async Task GetInFlightMessages_ConcurrentAccess_ReturnsConsistentResults()
    {
        var messages = Enumerable.Range(0, 100).Select(_ => CreateMessage()).ToArray();
        var context = CreateContext(messages);

        var tasks = new List<Task>();
        var collectedCounts = new System.Collections.Concurrent.ConcurrentBag<int>();

        // Concurrently read and process messages
        for (int i = 0; i < 10; i++)
        {
            var index = i;
            tasks.Add(Task.Run(async () =>
            {
                // Read in-flight count
                var count = context.GetInFlightMessages().Count;
                collectedCounts.Add(count);

                // Process some messages
                if (index < messages.Length)
                {
                    await context.FailMessageAsync(messages[index], CancellationToken.None);
                }
            }));
        }

        await Task.WhenAll(tasks);

        // After processing, should have fewer in-flight messages
        var finalCount = context.GetInFlightMessages().Count;
        finalCount.Should().BeLessThan(100);
    }

    #endregion
}
