using System.Collections.Concurrent;
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

public class ParallelProcessingTests
{
    private const string InboxName = "test-inbox";
    private const string MessageTypeName = "TestMessage";
    private const string FifoMessageTypeName = "FifoTestMessage";

    #region Test Messages

    private record TestMessage(string Id, string Data);

    private record FifoTestMessage(string GroupId, int Sequence) : IHasGroupId
    {
        public string GetGroupId() => GroupId;
    }

    #endregion

    #region Concurrency Tracking Handlers

    private class ConcurrencyTrackingHandler<TMessage> : IInboxHandler<TMessage> where TMessage : class
    {
        private int _currentConcurrency;
        private int _maxConcurrency;
        private readonly TimeSpan _processingDelay;
        private readonly ConcurrentBag<(Guid Id, DateTime StartTime, DateTime EndTime)> _processed = new();

        public ConcurrencyTrackingHandler(TimeSpan processingDelay)
        {
            _processingDelay = processingDelay;
        }

        public int MaxConcurrency => _maxConcurrency;
        public int ProcessedCount => _processed.Count;

        public async Task<InboxHandleResult> HandleAsync(InboxMessageEnvelope<TMessage> message, CancellationToken token)
        {
            var startTime = DateTime.UtcNow;
            var current = Interlocked.Increment(ref _currentConcurrency);

            // Track max concurrency using atomic compare-and-swap
            int oldMax;
            do
            {
                oldMax = _maxConcurrency;
                if (current <= oldMax) break;
            } while (Interlocked.CompareExchange(ref _maxConcurrency, current, oldMax) != oldMax);

            await Task.Delay(_processingDelay, token);

            Interlocked.Decrement(ref _currentConcurrency);
            var endTime = DateTime.UtcNow;

            _processed.Add((message.Id, startTime, endTime));

            return InboxHandleResult.Success;
        }
    }

    private class ConcurrencyTrackingBatchedHandler<TMessage> : IBatchedInboxHandler<TMessage> where TMessage : class
    {
        private int _currentConcurrency;
        private int _maxConcurrency;
        private readonly TimeSpan _processingDelay;
        private readonly ConcurrentBag<(int BatchSize, DateTime StartTime, DateTime EndTime)> _processed = new();

        public ConcurrencyTrackingBatchedHandler(TimeSpan processingDelay)
        {
            _processingDelay = processingDelay;
        }

        public int MaxConcurrency => _maxConcurrency;
        public int BatchCount => _processed.Count;

        public async Task<IReadOnlyList<InboxMessageResult>> HandleAsync(
            IReadOnlyList<InboxMessageEnvelope<TMessage>> messages,
            CancellationToken token)
        {
            var startTime = DateTime.UtcNow;
            var current = Interlocked.Increment(ref _currentConcurrency);

            int oldMax;
            do
            {
                oldMax = _maxConcurrency;
                if (current <= oldMax) break;
            } while (Interlocked.CompareExchange(ref _maxConcurrency, current, oldMax) != oldMax);

            await Task.Delay(_processingDelay, token);

            Interlocked.Decrement(ref _currentConcurrency);
            var endTime = DateTime.UtcNow;

            _processed.Add((messages.Count, startTime, endTime));

            return messages.Select(m => new InboxMessageResult(m.Id, InboxHandleResult.Success)).ToList();
        }
    }

    private class ConcurrencyTrackingFifoHandler<TMessage> : IFifoInboxHandler<TMessage>
        where TMessage : class, IHasGroupId
    {
        private int _currentConcurrency;
        private int _maxConcurrency;
        private readonly TimeSpan _processingDelay;
        private readonly ConcurrentBag<(string GroupId, DateTime StartTime, DateTime EndTime)> _processed = new();

        public ConcurrencyTrackingFifoHandler(TimeSpan processingDelay)
        {
            _processingDelay = processingDelay;
        }

        public int MaxConcurrency => _maxConcurrency;
        public int ProcessedCount => _processed.Count;
        public IReadOnlyCollection<(string GroupId, DateTime StartTime, DateTime EndTime)> Processed => _processed;

        public async Task<InboxHandleResult> HandleAsync(InboxMessageEnvelope<TMessage> message, CancellationToken token)
        {
            var startTime = DateTime.UtcNow;
            var current = Interlocked.Increment(ref _currentConcurrency);

            int oldMax;
            do
            {
                oldMax = _maxConcurrency;
                if (current <= oldMax) break;
            } while (Interlocked.CompareExchange(ref _maxConcurrency, current, oldMax) != oldMax);

            await Task.Delay(_processingDelay, token);

            Interlocked.Decrement(ref _currentConcurrency);
            var endTime = DateTime.UtcNow;

            _processed.Add((message.Payload.GetGroupId(), startTime, endTime));

            return InboxHandleResult.Success;
        }
    }

    private class ConcurrencyTrackingFifoBatchedHandler<TMessage> : IFifoBatchedInboxHandler<TMessage>
        where TMessage : class, IHasGroupId
    {
        private int _currentConcurrency;
        private int _maxConcurrency;
        private readonly TimeSpan _processingDelay;
        private readonly ConcurrentBag<(string GroupId, int BatchSize, DateTime StartTime, DateTime EndTime)> _processed = new();

        public ConcurrencyTrackingFifoBatchedHandler(TimeSpan processingDelay)
        {
            _processingDelay = processingDelay;
        }

        public int MaxConcurrency => _maxConcurrency;
        public int BatchCount => _processed.Count;
        public IReadOnlyCollection<(string GroupId, int BatchSize, DateTime StartTime, DateTime EndTime)> Processed => _processed;

        public async Task<IReadOnlyList<InboxMessageResult>> HandleAsync(
            string groupId,
            IReadOnlyList<InboxMessageEnvelope<TMessage>> messages,
            CancellationToken token)
        {
            var startTime = DateTime.UtcNow;
            var current = Interlocked.Increment(ref _currentConcurrency);

            int oldMax;
            do
            {
                oldMax = _maxConcurrency;
                if (current <= oldMax) break;
            } while (Interlocked.CompareExchange(ref _maxConcurrency, current, oldMax) != oldMax);

            await Task.Delay(_processingDelay, token);

            Interlocked.Decrement(ref _currentConcurrency);
            var endTime = DateTime.UtcNow;

            _processed.Add((groupId, messages.Count, startTime, endTime));

            return messages.Select(m => new InboxMessageResult(m.Id, InboxHandleResult.Success)).ToList();
        }
    }

    #endregion

    #region Test Setup Helpers

    private static InboxBase CreateMockInbox(InboxType type, int maxProcessingThreads, string messageTypeName, Type messageClrType)
    {
        var options = TestConfigurationFactory.CreateOptions(
            inboxName: InboxName,
            maxProcessingThreads: maxProcessingThreads);

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

        var storageProvider = Substitute.For<IInboxStorageProvider>();
        var serializer = CreateSerializer();
        var dateTimeProvider = Substitute.For<IDateTimeProvider>();

        var inbox = Substitute.For<InboxBase>(configuration, storageProvider, serializer, dateTimeProvider);
        inbox.Type.Returns(type);

        return inbox;
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

    private static IInboxMessagePayloadSerializer CreateSerializer()
    {
        var serializer = Substitute.For<IInboxMessagePayloadSerializer>();

        serializer.Deserialize<TestMessage>(Arg.Any<string>())
            .Returns(call =>
            {
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

    private static List<InboxMessage> CreateTestMessages(int count)
    {
        return Enumerable.Range(0, count)
            .Select(i => new InboxMessage
            {
                Id = Guid.NewGuid(),
                MessageType = MessageTypeName,
                Payload = $"{{\"Id\":\"{i}\",\"Data\":\"test-{i}\"}}",
                ReceivedAt = DateTime.UtcNow
            })
            .ToList();
    }

    private static List<InboxMessage> CreateFifoTestMessages(int groupCount, int messagesPerGroup)
    {
        var messages = new List<InboxMessage>();
        for (var g = 0; g < groupCount; g++)
        {
            for (var m = 0; m < messagesPerGroup; m++)
            {
                messages.Add(new InboxMessage
                {
                    Id = Guid.NewGuid(),
                    GroupId = $"group-{g}",
                    MessageType = FifoMessageTypeName,
                    Payload = $"{{\"GroupId\":\"group-{g}\",\"Sequence\":{m}}}",
                    ReceivedAt = DateTime.UtcNow.AddMilliseconds(g * messagesPerGroup + m)
                });
            }
        }
        return messages;
    }

    #endregion

    #region DefaultInboxProcessingStrategy Tests

    [Fact]
    public async Task Default_Sequential_ProcessesMessagesOneAtATime()
    {
        // Arrange
        var handler = new ConcurrencyTrackingHandler<TestMessage>(TimeSpan.FromMilliseconds(50));
        var inbox = CreateMockInbox(InboxType.Default, 1, MessageTypeName, typeof(TestMessage));
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new DefaultInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var messages = CreateTestMessages(5);

        // Act
        await strategy.ProcessAsync(string.Empty, messages, CancellationToken.None);

        // Assert
        handler.MaxConcurrency.Should().Be(1, "sequential processing should have max concurrency of 1");
        handler.ProcessedCount.Should().Be(5);
    }

    [Fact]
    public async Task Default_Parallel_ProcessesMessagesConcurrently()
    {
        // Arrange
        var handler = new ConcurrencyTrackingHandler<TestMessage>(TimeSpan.FromMilliseconds(100));
        var inbox = CreateMockInbox(InboxType.Default, 4, MessageTypeName, typeof(TestMessage));
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new DefaultInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var messages = CreateTestMessages(8);

        // Act
        await strategy.ProcessAsync(string.Empty, messages, CancellationToken.None);

        // Assert
        handler.MaxConcurrency.Should().BeGreaterThan(1, "parallel processing should have concurrency > 1");
        handler.MaxConcurrency.Should().BeLessOrEqualTo(4, "concurrency should not exceed MaxProcessingThreads");
        handler.ProcessedCount.Should().Be(8);
    }

    [Fact]
    public async Task Default_Parallel_RespectsMaxProcessingThreads()
    {
        // Arrange
        const int maxProcessingThreads = 2;
        var handler = new ConcurrencyTrackingHandler<TestMessage>(TimeSpan.FromMilliseconds(100));
        var inbox = CreateMockInbox(InboxType.Default, maxProcessingThreads, MessageTypeName, typeof(TestMessage));
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new DefaultInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var messages = CreateTestMessages(10);

        // Act
        await strategy.ProcessAsync(string.Empty, messages, CancellationToken.None);

        // Assert
        handler.MaxConcurrency.Should().BeLessOrEqualTo(maxProcessingThreads);
        handler.ProcessedCount.Should().Be(10);
    }

    #endregion

    #region BatchedInboxProcessingStrategy Tests

    [Fact]
    public async Task Batched_Sequential_ProcessesTypeGroupsOneAtATime()
    {
        // Arrange
        var handler = new ConcurrencyTrackingBatchedHandler<TestMessage>(TimeSpan.FromMilliseconds(50));
        var inbox = CreateMockInbox(InboxType.Batched, 1, MessageTypeName, typeof(TestMessage));
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new BatchedInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var messages = CreateTestMessages(5);

        // Act
        await strategy.ProcessAsync(string.Empty, messages, CancellationToken.None);

        // Assert
        handler.MaxConcurrency.Should().Be(1);
        handler.BatchCount.Should().Be(1, "all messages of same type should be in one batch");
    }

    [Fact]
    public async Task Batched_Parallel_ProcessesMultipleTypeGroupsConcurrently()
    {
        // Arrange
        var handler = new ConcurrencyTrackingBatchedHandler<TestMessage>(TimeSpan.FromMilliseconds(100));
        var inbox = CreateMockInbox(InboxType.Batched, 4, MessageTypeName, typeof(TestMessage));

        // Register second message type
        var config = inbox.GetConfiguration();
        config.MetadataRegistry.GetClrType("TestMessage2").Returns(typeof(TestMessage));

        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new BatchedInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());

        // Create messages of different types
        var messages = new List<InboxMessage>();
        for (var i = 0; i < 4; i++)
        {
            messages.Add(new InboxMessage
            {
                Id = Guid.NewGuid(),
                MessageType = i % 2 == 0 ? MessageTypeName : "TestMessage2",
                Payload = $"{{\"Id\":\"{i}\",\"Data\":\"test-{i}\"}}",
                ReceivedAt = DateTime.UtcNow
            });
        }

        // Act
        await strategy.ProcessAsync(string.Empty, messages, CancellationToken.None);

        // Assert
        handler.MaxConcurrency.Should().BeGreaterThanOrEqualTo(1);
        handler.BatchCount.Should().Be(2, "should have 2 batches for 2 message types");
    }

    #endregion

    #region FifoInboxProcessingStrategy Tests

    [Fact]
    public async Task Fifo_Sequential_ProcessesAllGroupsSequentially()
    {
        // Arrange
        var handler = new ConcurrencyTrackingFifoHandler<FifoTestMessage>(TimeSpan.FromMilliseconds(30));
        var inbox = CreateMockInbox(InboxType.Fifo, 1, FifoMessageTypeName, typeof(FifoTestMessage));
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new FifoInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var messages = CreateFifoTestMessages(groupCount: 3, messagesPerGroup: 2);

        // Act
        await strategy.ProcessAsync(string.Empty, messages, CancellationToken.None);

        // Assert
        handler.MaxConcurrency.Should().Be(1, "sequential processing should have max concurrency of 1");
        handler.ProcessedCount.Should().Be(6);
    }

    [Fact]
    public async Task Fifo_Parallel_ProcessesDifferentGroupsConcurrently()
    {
        // Arrange
        var handler = new ConcurrencyTrackingFifoHandler<FifoTestMessage>(TimeSpan.FromMilliseconds(100));
        var inbox = CreateMockInbox(InboxType.Fifo, 4, FifoMessageTypeName, typeof(FifoTestMessage));
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new FifoInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var messages = CreateFifoTestMessages(groupCount: 4, messagesPerGroup: 2);

        // Act
        await strategy.ProcessAsync(string.Empty, messages, CancellationToken.None);

        // Assert
        handler.MaxConcurrency.Should().BeGreaterThan(1, "different groups should process in parallel");
        handler.ProcessedCount.Should().Be(8);
    }

    [Fact]
    public async Task Fifo_Parallel_MaintainsOrderWithinGroup()
    {
        // Arrange
        var handler = new ConcurrencyTrackingFifoHandler<FifoTestMessage>(TimeSpan.FromMilliseconds(50));
        var inbox = CreateMockInbox(InboxType.Fifo, 4, FifoMessageTypeName, typeof(FifoTestMessage));
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new FifoInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var messages = CreateFifoTestMessages(groupCount: 2, messagesPerGroup: 5);

        // Act
        await strategy.ProcessAsync(string.Empty, messages, CancellationToken.None);

        // Assert
        handler.ProcessedCount.Should().Be(10);

        // Verify that within each group, messages were processed in order (by time)
        var processedByGroup = handler.Processed
            .GroupBy(p => p.GroupId)
            .ToList();

        foreach (var group in processedByGroup)
        {
            var orderedByTime = group.OrderBy(p => p.StartTime).ToList();
            for (var i = 1; i < orderedByTime.Count; i++)
            {
                // Each message should start after previous one ended (within same group)
                orderedByTime[i].StartTime.Should().BeOnOrAfter(orderedByTime[i - 1].EndTime,
                    $"messages within group {group.Key} should be processed sequentially");
            }
        }
    }

    #endregion

    #region FifoBatchedInboxProcessingStrategy Tests

    [Fact]
    public async Task FifoBatched_Sequential_ProcessesAllGroupsSequentially()
    {
        // Arrange
        var handler = new ConcurrencyTrackingFifoBatchedHandler<FifoTestMessage>(TimeSpan.FromMilliseconds(30));
        var inbox = CreateMockInbox(InboxType.FifoBatched, 1, FifoMessageTypeName, typeof(FifoTestMessage));
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new FifoBatchedInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var messages = CreateFifoTestMessages(groupCount: 3, messagesPerGroup: 2);

        // Act
        await strategy.ProcessAsync(string.Empty, messages, CancellationToken.None);

        // Assert
        handler.MaxConcurrency.Should().Be(1, "sequential processing should have max concurrency of 1");
    }

    [Fact]
    public async Task FifoBatched_Parallel_ProcessesDifferentGroupsConcurrently()
    {
        // Arrange
        var handler = new ConcurrencyTrackingFifoBatchedHandler<FifoTestMessage>(TimeSpan.FromMilliseconds(100));
        var inbox = CreateMockInbox(InboxType.FifoBatched, 4, FifoMessageTypeName, typeof(FifoTestMessage));
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new FifoBatchedInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());
        var messages = CreateFifoTestMessages(groupCount: 4, messagesPerGroup: 3);

        // Act
        await strategy.ProcessAsync(string.Empty, messages, CancellationToken.None);

        // Assert
        handler.MaxConcurrency.Should().BeGreaterThan(1, "different groups should process in parallel");
    }

    [Fact]
    public async Task FifoBatched_Parallel_BatchesConsecutiveSameTypeMessages()
    {
        // Arrange
        var handler = new ConcurrencyTrackingFifoBatchedHandler<FifoTestMessage>(TimeSpan.FromMilliseconds(50));
        var inbox = CreateMockInbox(InboxType.FifoBatched, 2, FifoMessageTypeName, typeof(FifoTestMessage));
        var serviceProvider = CreateServiceProvider(handler);
        var strategy = new FifoBatchedInboxProcessingStrategy(inbox, serviceProvider, Substitute.For<ILogger>());

        // All messages in same group, same type = one batch
        var messages = CreateFifoTestMessages(groupCount: 1, messagesPerGroup: 5);

        // Act
        await strategy.ProcessAsync(string.Empty, messages, CancellationToken.None);

        // Assert
        handler.BatchCount.Should().Be(1, "consecutive same-type messages should be batched together");
        handler.Processed.First().BatchSize.Should().Be(5);
    }

    #endregion
}