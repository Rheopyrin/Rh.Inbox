using System.Collections.Concurrent;
using Rh.Inbox.Abstractions.Handlers;

namespace Rh.Inbox.Tests.Integration.Common.Handlers;

public class FailingHandler<TMessage> : IInboxHandler<TMessage> where TMessage : class
{
    private readonly double _failureRate;
    private readonly ConcurrentBag<Guid> _processed = new();
    private readonly ConcurrentBag<Guid> _failed = new();

    public FailingHandler(double failureRate = 0.5)
    {
        _failureRate = failureRate;
    }

    public IReadOnlyCollection<Guid> Processed => _processed;
    public IReadOnlyCollection<Guid> Failed => _failed;
    public int ProcessedCount => _processed.Count;
    public int FailedCount => _failed.Count;

    public Task<InboxHandleResult> HandleAsync(InboxMessageEnvelope<TMessage> message, CancellationToken token)
    {
        if (Random.Shared.NextDouble() < _failureRate)
        {
            _failed.Add(message.Id);
            return Task.FromResult(InboxHandleResult.Failed);
        }

        _processed.Add(message.Id);
        return Task.FromResult(InboxHandleResult.Success);
    }
}

public class BatchedFailingHandler<TMessage> : IBatchedInboxHandler<TMessage> where TMessage : class
{
    private readonly double _failureRate;
    private readonly ConcurrentBag<Guid> _processed = new();
    private readonly ConcurrentBag<Guid> _failed = new();

    public BatchedFailingHandler(double failureRate = 0.5)
    {
        _failureRate = failureRate;
    }

    public IReadOnlyCollection<Guid> Processed => _processed;
    public IReadOnlyCollection<Guid> Failed => _failed;
    public int ProcessedCount => _processed.Count;
    public int FailedCount => _failed.Count;

    public Task<IReadOnlyList<InboxMessageResult>> HandleAsync(
        IReadOnlyList<InboxMessageEnvelope<TMessage>> messages,
        CancellationToken token)
    {
        var results = new List<InboxMessageResult>();

        foreach (var message in messages)
        {
            if (Random.Shared.NextDouble() < _failureRate)
            {
                _failed.Add(message.Id);
                results.Add(new InboxMessageResult(
                    message.Id,
                    InboxHandleResult.Failed,
                    "Simulated failure"));
            }
            else
            {
                _processed.Add(message.Id);
                results.Add(new InboxMessageResult(message.Id, InboxHandleResult.Success));
            }
        }

        return Task.FromResult<IReadOnlyList<InboxMessageResult>>(results);
    }
}
