using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Configuration;
using Rh.Inbox.Health;
using Rh.Inbox.Inboxes;
using Rh.Inbox.Processing.Strategies.Implementation;
using Rh.Inbox.Tests.Unit.TestHelpers;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Processing.Strategies;

public class ProcessingStrategyEdgeCasesTests
{
    private const string InboxName = "test-inbox";
    private const string MessageTypeName = "TestMessage";
    private const string FifoMessageTypeName = "FifoTestMessage";
    private const string UnknownMessageTypeName = "UnknownMessage";

    #region Test Messages

    public record TestMessage(string Id, string Data);

    public record FifoTestMessage(string GroupId, int Sequence) : IHasGroupId
    {
        public string GetGroupId() => GroupId;
    }

    #endregion

    #region Test Handlers

    private class ThrowingHandler : IInboxHandler<TestMessage>
    {
        private readonly Exception _exception;

        public ThrowingHandler(Exception exception)
        {
            _exception = exception;
        }

        public Task<InboxHandleResult> HandleAsync(InboxMessageEnvelope<TestMessage> message, CancellationToken token)
        {
            throw _exception;
        }
    }

    private class ThrowingFifoHandler : IFifoInboxHandler<FifoTestMessage>
    {
        private readonly Exception _exception;

        public ThrowingFifoHandler(Exception exception)
        {
            _exception = exception;
        }

        public Task<InboxHandleResult> HandleAsync(InboxMessageEnvelope<FifoTestMessage> message, CancellationToken token)
        {
            throw _exception;
        }
    }

    private class ThrowingBatchedHandler : IBatchedInboxHandler<TestMessage>
    {
        private readonly Exception _exception;

        public ThrowingBatchedHandler(Exception exception)
        {
            _exception = exception;
        }

        public Task<IReadOnlyList<InboxMessageResult>> HandleAsync(
            IReadOnlyList<InboxMessageEnvelope<TestMessage>> messages,
            CancellationToken token)
        {
            throw _exception;
        }
    }

    private class ThrowingFifoBatchedHandler : IFifoBatchedInboxHandler<FifoTestMessage>
    {
        private readonly Exception _exception;

        public ThrowingFifoBatchedHandler(Exception exception)
        {
            _exception = exception;
        }

        public Task<IReadOnlyList<InboxMessageResult>> HandleAsync(
            string groupId,
            IReadOnlyList<InboxMessageEnvelope<FifoTestMessage>> messages,
            CancellationToken token)
        {
            throw _exception;
        }
    }

    #endregion

    #region Setup Helpers

    private static InboxBase CreateMockInbox(
        InboxType type,
        string messageTypeName,
        Type? messageClrType,
        IInboxStorageProvider? storageProvider = null,
        IInboxMessagePayloadSerializer? serializer = null)
    {
        var options = TestConfigurationFactory.CreateOptions(inboxName: InboxName, maxAttempts: 3);

        var metadataRegistry = Substitute.For<IInboxMessageMetadataRegistry>();
        metadataRegistry.GetClrType(messageTypeName).Returns(messageClrType);

        var configuration = new InboxConfiguration
        {
            InboxName = InboxName,
            InboxType = type,
            Options = options,
            MetadataRegistry = metadataRegistry,
            StorageProviderFactoryFunc = _ => Substitute.For<IInboxStorageProviderFactory>(),
            SerializerFactoryFunc = _ => Substitute.For<IInboxSerializerFactory>(),
            HealthCheckOptions = new InboxHealthCheckOptions(),
            DateTimeProvider = Substitute.For<IDateTimeProvider>()
        };

        storageProvider ??= Substitute.For<IInboxStorageProvider>();
        serializer ??= CreateSerializer();
        var dateTimeProvider = Substitute.For<IDateTimeProvider>();

        var inbox = Substitute.For<InboxBase>(configuration, storageProvider, serializer, dateTimeProvider);
        inbox.Type.Returns(type);

        return inbox;
    }

    private static IInboxStorageProvider CreateMockStorageProvider(bool supportGroupLocks = false)
    {
        if (supportGroupLocks)
        {
            var provider = Substitute.For<IInboxStorageProvider, ISupportGroupLocksReleaseStorageProvider>();
            return provider;
        }

        return Substitute.For<IInboxStorageProvider>();
    }

    private static IServiceProvider CreateServiceProvider<THandler>(THandler handler)
        where THandler : class
    {
        var services = new ServiceCollection();

        switch (handler)
        {
            case IInboxHandler<TestMessage> h:
                services.AddKeyedSingleton<IInboxHandler<TestMessage>>(InboxName, h);
                break;
            case IBatchedInboxHandler<TestMessage> h:
                services.AddKeyedSingleton<IBatchedInboxHandler<TestMessage>>(InboxName, h);
                break;
            case IFifoInboxHandler<FifoTestMessage> h:
                services.AddKeyedSingleton<IFifoInboxHandler<FifoTestMessage>>(InboxName, h);
                break;
            case IFifoBatchedInboxHandler<FifoTestMessage> h:
                services.AddKeyedSingleton<IFifoBatchedInboxHandler<FifoTestMessage>>(InboxName, h);
                break;
        }

        return services.BuildServiceProvider();
    }

    private static IServiceProvider CreateEmptyServiceProvider()
    {
        return new ServiceCollection().BuildServiceProvider();
    }

    private static IInboxMessagePayloadSerializer CreateSerializer(bool failDeserialization = false)
    {
        var serializer = Substitute.For<IInboxMessagePayloadSerializer>();

        serializer.Deserialize<TestMessage>(Arg.Any<string>())
            .Returns(call =>
            {
                if (failDeserialization) return null;

                var json = call.Arg<string>();
                if (json.Contains("\"Id\""))
                {
                    var id = ExtractJsonValue(json, "Id");
                    var data = ExtractJsonValue(json, "Data");
                    return new TestMessage(id, data);
                }
                return null;
            });

        serializer.Deserialize<FifoTestMessage>(Arg.Any<string>())
            .Returns(call =>
            {
                if (failDeserialization) return null;

                var json = call.Arg<string>();
                if (json.Contains("\"GroupId\""))
                {
                    var groupId = ExtractJsonValue(json, "GroupId");
                    var sequence = int.Parse(ExtractJsonValue(json, "Sequence"));
                    return new FifoTestMessage(groupId, sequence);
                }
                return null;
            });

        return serializer;
    }

    private static string ExtractJsonValue(string json, string key)
    {
        var startIndex = json.IndexOf($"\"{key}\":", StringComparison.Ordinal);
        if (startIndex < 0) return "";

        startIndex = json.IndexOf(':', startIndex) + 1;
        while (startIndex < json.Length && (json[startIndex] == ' ' || json[startIndex] == '"'))
            startIndex++;

        var endIndex = startIndex;
        while (endIndex < json.Length && json[endIndex] != '"' && json[endIndex] != ',' && json[endIndex] != '}')
            endIndex++;

        return json.Substring(startIndex, endIndex - startIndex);
    }

    private static InboxMessage CreateMessage(
        string messageType = MessageTypeName,
        string? groupId = null,
        int attemptsCount = 0)
    {
        return new InboxMessage
        {
            Id = Guid.NewGuid(),
            MessageType = messageType,
            GroupId = groupId,
            Payload = messageType == FifoMessageTypeName
                ? $"{{\"GroupId\":\"{groupId}\",\"Sequence\":1}}"
                : "{\"Id\":\"test\",\"Data\":\"test-data\"}",
            ReceivedAt = DateTime.UtcNow,
            AttemptsCount = attemptsCount
        };
    }

    #endregion

    #region Unknown Message Type Tests

    [Fact]
    public async Task Default_UnknownMessageType_MovesToDeadLetter()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider();
        var inbox = CreateMockInbox(InboxType.Default, UnknownMessageTypeName, null, storageProvider);
        var serviceProvider = CreateEmptyServiceProvider();
        var strategy = new DefaultInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var message = CreateMessage(UnknownMessageTypeName);

        // Act
        await strategy.ProcessAsync("processor-1", [message], CancellationToken.None);

        // Assert
        await storageProvider.Received(1).MoveToDeadLetterAsync(
            message.Id,
            Arg.Is<string>(s => s.Contains("Unknown message type")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Fifo_UnknownMessageType_MovesToDeadLetter()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider(supportGroupLocks: true);
        var inbox = CreateMockInbox(InboxType.Fifo, UnknownMessageTypeName, null, storageProvider);
        var serviceProvider = CreateEmptyServiceProvider();
        var strategy = new FifoInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var message = CreateMessage(UnknownMessageTypeName, "group-1");

        // Act
        await strategy.ProcessAsync("processor-1", [message], CancellationToken.None);

        // Assert
        await storageProvider.Received(1).MoveToDeadLetterAsync(
            message.Id,
            Arg.Is<string>(s => s.Contains("Unknown message type")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Batched_UnknownMessageType_MovesToDeadLetterBatch()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider();
        var inbox = CreateMockInbox(InboxType.Batched, UnknownMessageTypeName, null, storageProvider);
        var serviceProvider = CreateEmptyServiceProvider();
        var strategy = new BatchedInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var messages = new[] { CreateMessage(UnknownMessageTypeName), CreateMessage(UnknownMessageTypeName) };

        // Act
        await strategy.ProcessAsync("processor-1", messages, CancellationToken.None);

        // Assert
        await storageProvider.Received(1).MoveToDeadLetterBatchAsync(
            Arg.Is<IReadOnlyList<(Guid, string)>>(list =>
                list.Count == 2 &&
                list.All(item => item.Item2.Contains("Unknown message type"))),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task FifoBatched_UnknownMessageType_MovesToDeadLetterBatch()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider(supportGroupLocks: true);
        var inbox = CreateMockInbox(InboxType.FifoBatched, UnknownMessageTypeName, null, storageProvider);
        var serviceProvider = CreateEmptyServiceProvider();
        var strategy = new FifoBatchedInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var messages = new[]
        {
            CreateMessage(UnknownMessageTypeName, "group-1"),
            CreateMessage(UnknownMessageTypeName, "group-1")
        };

        // Act
        await strategy.ProcessAsync("processor-1", messages, CancellationToken.None);

        // Assert
        await storageProvider.Received(1).MoveToDeadLetterBatchAsync(
            Arg.Is<IReadOnlyList<(Guid, string)>>(list =>
                list.Count == 2 &&
                list.All(item => item.Item2.Contains("Unknown message type"))),
            Arg.Any<CancellationToken>());
    }

    #endregion

    #region Deserialization Failure Tests

    [Fact]
    public async Task Default_FailedDeserialization_MovesToDeadLetter()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider();
        var serializer = CreateSerializer(failDeserialization: true);
        var inbox = CreateMockInbox(InboxType.Default, MessageTypeName, typeof(TestMessage), storageProvider, serializer);
        var handler = Substitute.For<IInboxHandler<TestMessage>>();
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new DefaultInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var message = CreateMessage();

        // Act
        await strategy.ProcessAsync("processor-1", [message], CancellationToken.None);

        // Assert
        await storageProvider.Received(1).MoveToDeadLetterAsync(
            message.Id,
            Arg.Is<string>(s => s.Contains("Failed to deserialize")),
            Arg.Any<CancellationToken>());
        await handler.DidNotReceive().HandleAsync(Arg.Any<InboxMessageEnvelope<TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Fifo_FailedDeserialization_MovesToDeadLetter()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider(supportGroupLocks: true);
        var serializer = CreateSerializer(failDeserialization: true);
        var inbox = CreateMockInbox(InboxType.Fifo, FifoMessageTypeName, typeof(FifoTestMessage), storageProvider, serializer);
        var handler = Substitute.For<IFifoInboxHandler<FifoTestMessage>>();
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new FifoInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var message = CreateMessage(FifoMessageTypeName, "group-1");

        // Act
        await strategy.ProcessAsync("processor-1", [message], CancellationToken.None);

        // Assert
        await storageProvider.Received(1).MoveToDeadLetterAsync(
            message.Id,
            Arg.Is<string>(s => s.Contains("Failed to deserialize")),
            Arg.Any<CancellationToken>());
        await handler.DidNotReceive().HandleAsync(Arg.Any<InboxMessageEnvelope<FifoTestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Batched_FailedDeserialization_MovesToDeadLetterBatch()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider();
        var serializer = CreateSerializer(failDeserialization: true);
        var inbox = CreateMockInbox(InboxType.Batched, MessageTypeName, typeof(TestMessage), storageProvider, serializer);
        var handler = Substitute.For<IBatchedInboxHandler<TestMessage>>();
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new BatchedInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var messages = new[] { CreateMessage(), CreateMessage() };

        // Act
        await strategy.ProcessAsync("processor-1", messages, CancellationToken.None);

        // Assert
        await storageProvider.Received(1).MoveToDeadLetterBatchAsync(
            Arg.Is<IReadOnlyList<(Guid, string)>>(list =>
                list.Count == 2 &&
                list.All(item => item.Item2.Contains("Failed to deserialize"))),
            Arg.Any<CancellationToken>());
        await handler.DidNotReceive().HandleAsync(Arg.Any<IReadOnlyList<InboxMessageEnvelope<TestMessage>>>(), Arg.Any<CancellationToken>());
    }

    #endregion

    #region No Handler Registered Tests

    [Fact]
    public async Task Default_NoHandlerRegistered_MovesToDeadLetter()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider();
        var inbox = CreateMockInbox(InboxType.Default, MessageTypeName, typeof(TestMessage), storageProvider);
        var serviceProvider = CreateEmptyServiceProvider();
        var strategy = new DefaultInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var message = CreateMessage();

        // Act
        await strategy.ProcessAsync("processor-1", [message], CancellationToken.None);

        // Assert
        await storageProvider.Received(1).MoveToDeadLetterAsync(
            message.Id,
            Arg.Is<string>(s => s.Contains("No handler registered")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Fifo_NoHandlerRegistered_MovesToDeadLetter()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider(supportGroupLocks: true);
        var inbox = CreateMockInbox(InboxType.Fifo, FifoMessageTypeName, typeof(FifoTestMessage), storageProvider);
        var serviceProvider = CreateEmptyServiceProvider();
        var strategy = new FifoInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var message = CreateMessage(FifoMessageTypeName, "group-1");

        // Act
        await strategy.ProcessAsync("processor-1", [message], CancellationToken.None);

        // Assert
        await storageProvider.Received(1).MoveToDeadLetterAsync(
            message.Id,
            Arg.Is<string>(s => s.Contains("No") && s.Contains("handler registered")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Batched_NoHandlerRegistered_MovesToDeadLetterBatch()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider();
        var inbox = CreateMockInbox(InboxType.Batched, MessageTypeName, typeof(TestMessage), storageProvider);
        var serviceProvider = CreateEmptyServiceProvider();
        var strategy = new BatchedInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var messages = new[] { CreateMessage(), CreateMessage() };

        // Act
        await strategy.ProcessAsync("processor-1", messages, CancellationToken.None);

        // Assert
        await storageProvider.Received(1).MoveToDeadLetterBatchAsync(
            Arg.Is<IReadOnlyList<(Guid, string)>>(list =>
                list.Count == 2 &&
                list.All(item => item.Item2.Contains("No handler registered"))),
            Arg.Any<CancellationToken>());
    }

    #endregion

    #region Handler Exception Tests

    [Fact]
    public async Task Default_HandlerThrowsException_FailsMessage()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider();
        var inbox = CreateMockInbox(InboxType.Default, MessageTypeName, typeof(TestMessage), storageProvider);
        var handler = new ThrowingHandler(new InvalidOperationException("Test exception"));
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new DefaultInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var message = CreateMessage();

        // Act
        await strategy.ProcessAsync("processor-1", [message], CancellationToken.None);

        // Assert
        await storageProvider.Received(1).FailAsync(message.Id, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Fifo_HandlerThrowsException_FailsMessage()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider(supportGroupLocks: true);
        var inbox = CreateMockInbox(InboxType.Fifo, FifoMessageTypeName, typeof(FifoTestMessage), storageProvider);
        var handler = new ThrowingFifoHandler(new InvalidOperationException("Test exception"));
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new FifoInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var message = CreateMessage(FifoMessageTypeName, "group-1");

        // Act
        await strategy.ProcessAsync("processor-1", [message], CancellationToken.None);

        // Assert
        await storageProvider.Received(1).FailAsync(message.Id, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Batched_HandlerThrowsException_FailsBatch()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider();
        var inbox = CreateMockInbox(InboxType.Batched, MessageTypeName, typeof(TestMessage), storageProvider);
        var handler = new ThrowingBatchedHandler(new InvalidOperationException("Test exception"));
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new BatchedInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var messages = new[] { CreateMessage(), CreateMessage() };

        // Act
        await strategy.ProcessAsync("processor-1", messages, CancellationToken.None);

        // Assert
        await storageProvider.Received(1).FailBatchAsync(
            Arg.Is<IReadOnlyList<Guid>>(list => list.Count == 2),
            Arg.Any<CancellationToken>());
    }

    #endregion

    #region Max Attempts Exceeded Tests

    [Fact]
    public async Task Default_MaxAttemptsExceeded_MovesToDeadLetter()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider();
        var inbox = CreateMockInbox(InboxType.Default, MessageTypeName, typeof(TestMessage), storageProvider);
        var handler = new ThrowingHandler(new InvalidOperationException("Test exception"));
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new DefaultInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var message = CreateMessage(attemptsCount: 2); // MaxAttempts is 3, so next attempt will exceed

        // Act
        await strategy.ProcessAsync("processor-1", [message], CancellationToken.None);

        // Assert
        await storageProvider.Received(1).MoveToDeadLetterAsync(
            message.Id,
            Arg.Is<string>(s => s.Contains("Max attempts") && s.Contains("exceeded")),
            Arg.Any<CancellationToken>());
        await storageProvider.DidNotReceive().FailAsync(Arg.Any<Guid>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Fifo_MaxAttemptsExceeded_MovesToDeadLetter()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider(supportGroupLocks: true);
        var inbox = CreateMockInbox(InboxType.Fifo, FifoMessageTypeName, typeof(FifoTestMessage), storageProvider);
        var handler = new ThrowingFifoHandler(new InvalidOperationException("Test exception"));
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new FifoInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var message = CreateMessage(FifoMessageTypeName, "group-1", attemptsCount: 2);

        // Act
        await strategy.ProcessAsync("processor-1", [message], CancellationToken.None);

        // Assert
        await storageProvider.Received(1).MoveToDeadLetterAsync(
            message.Id,
            Arg.Is<string>(s => s.Contains("Max attempts") && s.Contains("exceeded")),
            Arg.Any<CancellationToken>());
        await storageProvider.DidNotReceive().FailAsync(Arg.Any<Guid>(), Arg.Any<CancellationToken>());
    }

    #endregion

    #region ProcessResultsAsync Logic Tests

    [Fact]
    public async Task ProcessResults_SuccessResult_CompletesMessage()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider();
        var inbox = CreateMockInbox(InboxType.Default, MessageTypeName, typeof(TestMessage), storageProvider);
        var handler = Substitute.For<IInboxHandler<TestMessage>>();
        handler.HandleAsync(Arg.Any<InboxMessageEnvelope<TestMessage>>(), Arg.Any<CancellationToken>())
            .Returns(InboxHandleResult.Success);
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new DefaultInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var message = CreateMessage();

        // Act
        await strategy.ProcessAsync("processor-1", [message], CancellationToken.None);

        // Assert
        await storageProvider.Received(1).ProcessResultsBatchAsync(
            Arg.Is<IReadOnlyList<Guid>>(list => list.Count == 1 && list.Contains(message.Id)), // toComplete
            Arg.Is<IReadOnlyList<Guid>>(list => list.Count == 0), // toFail
            Arg.Is<IReadOnlyList<Guid>>(list => list.Count == 0), // toRelease
            Arg.Is<IReadOnlyList<(Guid, string)>>(list => list.Count == 0), // toDeadLetter
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessResults_FailedResult_FailsOrMovesToDeadLetter()
    {
        // Arrange - first test failure within attempts
        var storageProvider = CreateMockStorageProvider();
        var inbox = CreateMockInbox(InboxType.Default, MessageTypeName, typeof(TestMessage), storageProvider);
        var handler = Substitute.For<IInboxHandler<TestMessage>>();
        handler.HandleAsync(Arg.Any<InboxMessageEnvelope<TestMessage>>(), Arg.Any<CancellationToken>())
            .Returns(InboxHandleResult.Failed);
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new DefaultInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var message = CreateMessage(attemptsCount: 0);

        // Act
        await strategy.ProcessAsync("processor-1", [message], CancellationToken.None);

        // Assert - should fail (not move to dead letter)
        await storageProvider.Received(1).ProcessResultsBatchAsync(
            Arg.Is<IReadOnlyList<Guid>>(list => list.Count == 0), // toComplete
            Arg.Is<IReadOnlyList<Guid>>(list => list.Count == 1 && list.Contains(message.Id)), // toFail
            Arg.Is<IReadOnlyList<Guid>>(list => list.Count == 0), // toRelease
            Arg.Is<IReadOnlyList<(Guid, string)>>(list => list.Count == 0), // toDeadLetter
            Arg.Any<CancellationToken>());

        // Arrange - now test max attempts exceeded
        storageProvider.ClearReceivedCalls();
        var message2 = CreateMessage(attemptsCount: 2);

        // Act
        await strategy.ProcessAsync("processor-1", [message2], CancellationToken.None);

        // Assert - should move to dead letter
        await storageProvider.Received(1).ProcessResultsBatchAsync(
            Arg.Is<IReadOnlyList<Guid>>(list => list.Count == 0), // toComplete
            Arg.Is<IReadOnlyList<Guid>>(list => list.Count == 0), // toFail
            Arg.Is<IReadOnlyList<Guid>>(list => list.Count == 0), // toRelease
            Arg.Is<IReadOnlyList<(Guid, string)>>(list =>
                list.Count == 1 &&
                list.Any(item => item.Item1 == message2.Id && item.Item2.Contains("Max attempts"))), // toDeadLetter
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessResults_RetryResult_ReleasesMessage()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider();
        var inbox = CreateMockInbox(InboxType.Default, MessageTypeName, typeof(TestMessage), storageProvider);
        var handler = Substitute.For<IInboxHandler<TestMessage>>();
        handler.HandleAsync(Arg.Any<InboxMessageEnvelope<TestMessage>>(), Arg.Any<CancellationToken>())
            .Returns(InboxHandleResult.Retry);
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new DefaultInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var message = CreateMessage();

        // Act
        await strategy.ProcessAsync("processor-1", [message], CancellationToken.None);

        // Assert
        await storageProvider.Received(1).ProcessResultsBatchAsync(
            Arg.Is<IReadOnlyList<Guid>>(list => list.Count == 0), // toComplete
            Arg.Is<IReadOnlyList<Guid>>(list => list.Count == 0), // toFail
            Arg.Is<IReadOnlyList<Guid>>(list => list.Count == 1 && list.Contains(message.Id)), // toRelease
            Arg.Is<IReadOnlyList<(Guid, string)>>(list => list.Count == 0), // toDeadLetter
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessResults_MoveToDeadLetterResult_MovesToDeadLetter()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider();
        var inbox = CreateMockInbox(InboxType.Default, MessageTypeName, typeof(TestMessage), storageProvider);
        var handler = Substitute.For<IInboxHandler<TestMessage>>();
        handler.HandleAsync(Arg.Any<InboxMessageEnvelope<TestMessage>>(), Arg.Any<CancellationToken>())
            .Returns(InboxHandleResult.MoveToDeadLetter);
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new DefaultInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var message = CreateMessage();

        // Act
        await strategy.ProcessAsync("processor-1", [message], CancellationToken.None);

        // Assert
        await storageProvider.Received(1).ProcessResultsBatchAsync(
            Arg.Is<IReadOnlyList<Guid>>(list => list.Count == 0), // toComplete
            Arg.Is<IReadOnlyList<Guid>>(list => list.Count == 0), // toFail
            Arg.Is<IReadOnlyList<Guid>>(list => list.Count == 0), // toRelease
            Arg.Is<IReadOnlyList<(Guid, string)>>(list =>
                list.Count == 1 && list.Any(item => item.Item1 == message.Id)), // toDeadLetter
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessResults_EmptyList_DoesNothing()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider();
        var inbox = CreateMockInbox(InboxType.Default, MessageTypeName, typeof(TestMessage), storageProvider);
        var serviceProvider = CreateEmptyServiceProvider();
        var strategy = new DefaultInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());

        // Act
        await strategy.ProcessAsync("processor-1", Array.Empty<InboxMessage>(), CancellationToken.None);

        // Assert
        await storageProvider.DidNotReceive().ProcessResultsBatchAsync(
            Arg.Any<IReadOnlyList<Guid>>(),
            Arg.Any<IReadOnlyList<Guid>>(),
            Arg.Any<IReadOnlyList<Guid>>(),
            Arg.Any<IReadOnlyList<(Guid, string)>>(),
            Arg.Any<CancellationToken>());
    }

    #endregion

    #region FailMessageBatchAsync Logic Tests

    [Fact]
    public async Task FailMessageBatch_MixedAttempts_CorrectlyRoutes()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider();
        var inbox = CreateMockInbox(InboxType.Batched, MessageTypeName, typeof(TestMessage), storageProvider);
        var handler = new ThrowingBatchedHandler(new InvalidOperationException("Test exception"));
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new BatchedInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());

        var message1 = CreateMessage(attemptsCount: 0); // Will be failed
        var message2 = CreateMessage(attemptsCount: 1); // Will be failed
        var message3 = CreateMessage(attemptsCount: 2); // Will be moved to dead letter (exceeds max)

        // Act
        await strategy.ProcessAsync("processor-1", new[] { message1, message2, message3 }, CancellationToken.None);

        // Assert
        await storageProvider.Received(1).FailBatchAsync(
            Arg.Is<IReadOnlyList<Guid>>(list =>
                list.Count == 2 &&
                list.Contains(message1.Id) &&
                list.Contains(message2.Id)),
            Arg.Any<CancellationToken>());

        await storageProvider.Received(1).MoveToDeadLetterBatchAsync(
            Arg.Is<IReadOnlyList<(Guid, string)>>(list =>
                list.Count == 1 &&
                list.Any(item => item.Item1 == message3.Id && item.Item2.Contains("Max attempts"))),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task FailMessageBatch_EmptyList_DoesNothing()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider();
        var inbox = CreateMockInbox(InboxType.Batched, MessageTypeName, typeof(TestMessage), storageProvider);
        var serviceProvider = CreateEmptyServiceProvider();
        var strategy = new BatchedInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());

        // Act
        await strategy.ProcessAsync("processor-1", Array.Empty<InboxMessage>(), CancellationToken.None);

        // Assert
        await storageProvider.DidNotReceive().FailBatchAsync(
            Arg.Any<IReadOnlyList<Guid>>(),
            Arg.Any<CancellationToken>());
        await storageProvider.DidNotReceive().MoveToDeadLetterBatchAsync(
            Arg.Any<IReadOnlyList<(Guid, string)>>(),
            Arg.Any<CancellationToken>());
    }

    #endregion

    #region FIFO Group Lock Release Tests

    [Fact]
    public async Task Fifo_AfterProcessing_ReleasesGroupLock()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider(supportGroupLocks: true);
        var groupLocksProvider = (ISupportGroupLocksReleaseStorageProvider)storageProvider;
        var inbox = CreateMockInbox(InboxType.Fifo, FifoMessageTypeName, typeof(FifoTestMessage), storageProvider);
        var handler = Substitute.For<IFifoInboxHandler<FifoTestMessage>>();
        handler.HandleAsync(Arg.Any<InboxMessageEnvelope<FifoTestMessage>>(), Arg.Any<CancellationToken>())
            .Returns(InboxHandleResult.Success);
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new FifoInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var message = CreateMessage(FifoMessageTypeName, "group-1");

        // Act
        await strategy.ProcessAsync("processor-1", [message], CancellationToken.None);

        // Assert
        await groupLocksProvider.Received(1).ReleaseGroupLocksAsync(
            Arg.Is<IReadOnlyList<string>>(list => list.Count == 1 && list.Contains("group-1")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Fifo_HandlerException_StillReleasesGroupLock()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider(supportGroupLocks: true);
        var groupLocksProvider = (ISupportGroupLocksReleaseStorageProvider)storageProvider;
        var inbox = CreateMockInbox(InboxType.Fifo, FifoMessageTypeName, typeof(FifoTestMessage), storageProvider);
        var handler = new ThrowingFifoHandler(new InvalidOperationException("Test exception"));
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new FifoInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var message = CreateMessage(FifoMessageTypeName, "group-1");

        // Act
        await strategy.ProcessAsync("processor-1", [message], CancellationToken.None);

        // Assert - should still release the lock despite exception
        await groupLocksProvider.Received(1).ReleaseGroupLocksAsync(
            Arg.Is<IReadOnlyList<string>>(list => list.Count == 1 && list.Contains("group-1")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task FifoBatched_AfterProcessing_ReleasesGroupLock()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider(supportGroupLocks: true);
        var groupLocksProvider = (ISupportGroupLocksReleaseStorageProvider)storageProvider;
        var inbox = CreateMockInbox(InboxType.FifoBatched, FifoMessageTypeName, typeof(FifoTestMessage), storageProvider);
        var handler = Substitute.For<IFifoBatchedInboxHandler<FifoTestMessage>>();
        handler.HandleAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<InboxMessageEnvelope<FifoTestMessage>>>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var messages = call.Arg<IReadOnlyList<InboxMessageEnvelope<FifoTestMessage>>>();
                return Task.FromResult<IReadOnlyList<InboxMessageResult>>(
                    messages.Select(m => new InboxMessageResult(m.Id, InboxHandleResult.Success)).ToList());
            });
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new FifoBatchedInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var messages = new[]
        {
            CreateMessage(FifoMessageTypeName, "group-1"),
            CreateMessage(FifoMessageTypeName, "group-1")
        };

        // Act
        await strategy.ProcessAsync("processor-1", messages, CancellationToken.None);

        // Assert
        await groupLocksProvider.Received(1).ReleaseGroupLocksAsync(
            Arg.Is<IReadOnlyList<string>>(list => list.Count == 1 && list.Contains("group-1")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task FifoBatched_HandlerException_StillReleasesGroupLock()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider(supportGroupLocks: true);
        var groupLocksProvider = (ISupportGroupLocksReleaseStorageProvider)storageProvider;
        var inbox = CreateMockInbox(InboxType.FifoBatched, FifoMessageTypeName, typeof(FifoTestMessage), storageProvider);
        var handler = new ThrowingFifoBatchedHandler(new InvalidOperationException("Test exception"));
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new FifoBatchedInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var messages = new[]
        {
            CreateMessage(FifoMessageTypeName, "group-1"),
            CreateMessage(FifoMessageTypeName, "group-1")
        };

        // Act
        await strategy.ProcessAsync("processor-1", messages, CancellationToken.None);

        // Assert - should still release the lock despite exception
        await groupLocksProvider.Received(1).ReleaseGroupLocksAsync(
            Arg.Is<IReadOnlyList<string>>(list => list.Count == 1 && list.Contains("group-1")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Fifo_EmptyGroupId_DoesNotReleaseGroupLock()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider(supportGroupLocks: true);
        var groupLocksProvider = (ISupportGroupLocksReleaseStorageProvider)storageProvider;
        var inbox = CreateMockInbox(InboxType.Fifo, FifoMessageTypeName, typeof(FifoTestMessage), storageProvider);
        var handler = Substitute.For<IFifoInboxHandler<FifoTestMessage>>();
        handler.HandleAsync(Arg.Any<InboxMessageEnvelope<FifoTestMessage>>(), Arg.Any<CancellationToken>())
            .Returns(InboxHandleResult.Success);
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new FifoInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var message = CreateMessage(FifoMessageTypeName, groupId: ""); // Empty group ID

        // Act
        await strategy.ProcessAsync("processor-1", [message], CancellationToken.None);

        // Assert - should NOT release group lock for empty group ID
        await groupLocksProvider.DidNotReceive().ReleaseGroupLocksAsync(
            Arg.Any<IReadOnlyList<string>>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task FifoBatched_MultipleGroups_ReleasesAllGroupLocks()
    {
        // Arrange
        var storageProvider = CreateMockStorageProvider(supportGroupLocks: true);
        var groupLocksProvider = (ISupportGroupLocksReleaseStorageProvider)storageProvider;
        var inbox = CreateMockInbox(InboxType.FifoBatched, FifoMessageTypeName, typeof(FifoTestMessage), storageProvider);
        var handler = Substitute.For<IFifoBatchedInboxHandler<FifoTestMessage>>();
        handler.HandleAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<InboxMessageEnvelope<FifoTestMessage>>>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var messages = call.Arg<IReadOnlyList<InboxMessageEnvelope<FifoTestMessage>>>();
                return Task.FromResult<IReadOnlyList<InboxMessageResult>>(
                    messages.Select(m => new InboxMessageResult(m.Id, InboxHandleResult.Success)).ToList());
            });
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new FifoBatchedInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var messages = new[]
        {
            CreateMessage(FifoMessageTypeName, "group-1"),
            CreateMessage(FifoMessageTypeName, "group-2"),
            CreateMessage(FifoMessageTypeName, "group-3")
        };

        // Act
        await strategy.ProcessAsync("processor-1", messages, CancellationToken.None);

        // Assert - each group should be released once
        await groupLocksProvider.Received(1).ReleaseGroupLocksAsync(
            Arg.Is<IReadOnlyList<string>>(list => list.Contains("group-1")),
            Arg.Any<CancellationToken>());
        await groupLocksProvider.Received(1).ReleaseGroupLocksAsync(
            Arg.Is<IReadOnlyList<string>>(list => list.Contains("group-2")),
            Arg.Any<CancellationToken>());
        await groupLocksProvider.Received(1).ReleaseGroupLocksAsync(
            Arg.Is<IReadOnlyList<string>>(list => list.Contains("group-3")),
            Arg.Any<CancellationToken>());
    }

    #endregion
}
