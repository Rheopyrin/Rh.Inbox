using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;

namespace Rh.Inbox.Tests.Integration.Common.Handlers;

/// <summary>
/// Handler that tracks all processed messages with timestamps.
/// </summary>
public class TrackingFifoHandler<TMessage> : IFifoInboxHandler<TMessage>
    where TMessage : class, IHasGroupId
{
    private readonly List<TrackingProcessedItem<TMessage>> _processed = new();

    public IReadOnlyList<TrackingProcessedItem<TMessage>> Processed
    {
        get
        {
            lock (_processed)
                return _processed.ToList();
        }
    }

    public int ProcessedCount
    {
        get
        {
            lock (_processed)
                return _processed.Count;
        }
    }

    public Task<InboxHandleResult> HandleAsync(InboxMessageEnvelope<TMessage> message, CancellationToken token)
    {
        lock (_processed)
            _processed.Add(new TrackingProcessedItem<TMessage>(message.Payload, DateTime.UtcNow));

        return Task.FromResult(InboxHandleResult.Success);
    }

    public void Clear()
    {
        lock (_processed)
            _processed.Clear();
    }
}

public record TrackingProcessedItem<TMessage>(TMessage Message, DateTime ProcessedAt);
