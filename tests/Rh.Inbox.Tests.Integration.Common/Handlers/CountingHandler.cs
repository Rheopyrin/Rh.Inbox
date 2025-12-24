using System.Collections.Concurrent;
using Rh.Inbox.Abstractions.Handlers;

namespace Rh.Inbox.Tests.Integration.Common.Handlers;

public class CountingHandler<TMessage> : IInboxHandler<TMessage> where TMessage : class
{
    private readonly ConcurrentBag<ProcessedMessage<TMessage>> _processed = new();
    private readonly string _processorId;

    public CountingHandler(string processorId = "default")
    {
        _processorId = processorId;
    }

    public IReadOnlyCollection<ProcessedMessage<TMessage>> Processed => _processed;
    public int ProcessedCount => _processed.Count;

    public Task<InboxHandleResult> HandleAsync(InboxMessageEnvelope<TMessage> message, CancellationToken token)
    {
        _processed.Add(new ProcessedMessage<TMessage>(
            message.Id,
            message.Payload,
            DateTime.UtcNow,
            _processorId));

        return Task.FromResult(InboxHandleResult.Success);
    }

    public void Clear() => _processed.Clear();
}

public class BatchedCountingHandler<TMessage> : IBatchedInboxHandler<TMessage> where TMessage : class
{
    private readonly ConcurrentBag<ProcessedMessage<TMessage>> _processed = new();
    private readonly string _processorId;

    public BatchedCountingHandler(string processorId = "default")
    {
        _processorId = processorId;
    }

    public IReadOnlyCollection<ProcessedMessage<TMessage>> Processed => _processed;
    public int ProcessedCount => _processed.Count;

    public Task<IReadOnlyList<InboxMessageResult>> HandleAsync(
        IReadOnlyList<InboxMessageEnvelope<TMessage>> messages,
        CancellationToken token)
    {
        var results = new List<InboxMessageResult>();
        var now = DateTime.UtcNow;

        foreach (var message in messages)
        {
            _processed.Add(new ProcessedMessage<TMessage>(
                message.Id,
                message.Payload,
                now,
                _processorId));

            results.Add(new InboxMessageResult(message.Id, InboxHandleResult.Success));
        }

        return Task.FromResult<IReadOnlyList<InboxMessageResult>>(results);
    }

    public void Clear() => _processed.Clear();
}

public record ProcessedMessage<TMessage>(
    Guid MessageId,
    TMessage Message,
    DateTime ProcessedAt,
    string ProcessorId);
