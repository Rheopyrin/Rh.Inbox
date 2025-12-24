using System.Collections.Concurrent;
using Rh.Inbox.Abstractions.Handlers;

namespace Rh.Inbox.Tests.Integration.Common.Handlers;

public class MultiProcessorTrackingHandler<TMessage> : IInboxHandler<TMessage>
    where TMessage : class
{
    private readonly ConcurrentDictionary<Guid, string> _globalProcessed;
    private int _processedCount;

    public string ProcessorId { get; }
    public int ProcessedCount => _processedCount;

    public MultiProcessorTrackingHandler(string processorId, ConcurrentDictionary<Guid, string> globalProcessed)
    {
        ProcessorId = processorId;
        _globalProcessed = globalProcessed;
    }

    public Task<InboxHandleResult> HandleAsync(InboxMessageEnvelope<TMessage> message, CancellationToken token)
    {
        if (_globalProcessed.TryAdd(message.Id, ProcessorId))
        {
            Interlocked.Increment(ref _processedCount);
        }

        return Task.FromResult(InboxHandleResult.Success);
    }
}
