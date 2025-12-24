using System.Collections.Concurrent;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;

namespace Rh.Inbox.Tests.Integration.Common.Handlers;

/// <summary>
/// FIFO handler that introduces a delay to test sequential processing behavior.
/// </summary>
public class DelayedFifoHandler<TMessage> : IFifoInboxHandler<TMessage>
    where TMessage : class, IHasGroupId
{
    private readonly ConcurrentBag<FifoProcessedMessage<TMessage>> _processed = new();
    private readonly TimeSpan _delay;
    private readonly string _processorId;
    private int _sequence;

    public DelayedFifoHandler(TimeSpan delay, string processorId = "delayed")
    {
        _delay = delay;
        _processorId = processorId;
    }

    public IReadOnlyCollection<FifoProcessedMessage<TMessage>> Processed => _processed;
    public int ProcessedCount => _processed.Count;

    public async Task<InboxHandleResult> HandleAsync(InboxMessageEnvelope<TMessage> message, CancellationToken token)
    {
        await Task.Delay(_delay, token);

        var seq = Interlocked.Increment(ref _sequence);
        _processed.Add(new FifoProcessedMessage<TMessage>(
            message.Payload,
            DateTime.UtcNow,
            _processorId,
            seq));

        return InboxHandleResult.Success;
    }

    public void Clear()
    {
        _processed.Clear();
        _sequence = 0;
    }
}
