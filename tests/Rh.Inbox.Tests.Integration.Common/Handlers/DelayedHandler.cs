using System.Collections.Concurrent;
using Rh.Inbox.Abstractions.Handlers;

namespace Rh.Inbox.Tests.Integration.Common.Handlers;

public class DelayedHandler<TMessage> : IInboxHandler<TMessage> where TMessage : class
{
    private readonly TimeSpan _delay;
    private readonly ConcurrentBag<Guid> _processed = new();

    public DelayedHandler(TimeSpan delay)
    {
        _delay = delay;
    }

    public IReadOnlyCollection<Guid> Processed => _processed;
    public int ProcessedCount => _processed.Count;

    public async Task<InboxHandleResult> HandleAsync(InboxMessageEnvelope<TMessage> message, CancellationToken token)
    {
        await Task.Delay(_delay, token);
        _processed.Add(message.Id);
        return InboxHandleResult.Success;
    }
}

public class BatchedDelayedHandler<TMessage> : IBatchedInboxHandler<TMessage> where TMessage : class
{
    private readonly TimeSpan _delayPerMessage;
    private readonly ConcurrentBag<Guid> _processed = new();

    public BatchedDelayedHandler(TimeSpan delayPerMessage)
    {
        _delayPerMessage = delayPerMessage;
    }

    public IReadOnlyCollection<Guid> Processed => _processed;
    public int ProcessedCount => _processed.Count;

    public async Task<IReadOnlyList<InboxMessageResult>> HandleAsync(
        IReadOnlyList<InboxMessageEnvelope<TMessage>> messages,
        CancellationToken token)
    {
        var results = new List<InboxMessageResult>();

        foreach (var message in messages)
        {
            await Task.Delay(_delayPerMessage, token);
            _processed.Add(message.Id);
            results.Add(new InboxMessageResult(message.Id, InboxHandleResult.Success));
        }

        return results;
    }
}
