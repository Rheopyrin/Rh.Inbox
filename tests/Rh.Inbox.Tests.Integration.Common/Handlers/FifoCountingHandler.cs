using System.Collections.Concurrent;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;

namespace Rh.Inbox.Tests.Integration.Common.Handlers;

public class FifoCountingHandler<TMessage> : IFifoInboxHandler<TMessage>
    where TMessage : class, IHasGroupId
{
    private readonly ConcurrentBag<FifoProcessedMessage<TMessage>> _processed = new();
    private readonly string _processorId;
    private int _sequence;

    public FifoCountingHandler(string processorId = "default")
    {
        _processorId = processorId;
    }

    public IReadOnlyCollection<FifoProcessedMessage<TMessage>> Processed => _processed;
    public int ProcessedCount => _processed.Count;

    public Task<InboxHandleResult> HandleAsync(InboxMessageEnvelope<TMessage> message, CancellationToken token)
    {
        var seq = Interlocked.Increment(ref _sequence);
        _processed.Add(new FifoProcessedMessage<TMessage>(
            message.Payload,
            DateTime.UtcNow,
            _processorId,
            seq));

        return Task.FromResult(InboxHandleResult.Success);
    }

    public void Clear()
    {
        _processed.Clear();
        _sequence = 0;
    }

    public bool ValidateOrdering()
    {
        var byGroup = _processed
            .GroupBy(p => p.Message.GetGroupId())
            .ToList();

        foreach (var group in byGroup)
        {
            var orderedBySeq = group.OrderBy(p => p.GlobalSequence).ToList();
            for (int i = 1; i < orderedBySeq.Count; i++)
            {
                var prev = orderedBySeq[i - 1];
                var curr = orderedBySeq[i];

                // Within same group, processing time should be increasing
                if (curr.ProcessedAt < prev.ProcessedAt)
                {
                    return false;
                }
            }
        }

        return true;
    }
}

public record FifoProcessedMessage<TMessage>(
    TMessage Message,
    DateTime ProcessedAt,
    string ProcessorId,
    int GlobalSequence);
