using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Tests.Integration.Common.TestMessages;

namespace Rh.Inbox.Tests.Integration.Common.Handlers;

public class LargePayloadTrackingHandler : IInboxHandler<LargePayloadMessage>
{
    private int _processedCount;
    private bool _payloadSizesValid = true;

    public int ProcessedCount => _processedCount;
    public bool PayloadSizesValid => _payloadSizesValid;

    public Task<InboxHandleResult> HandleAsync(InboxMessageEnvelope<LargePayloadMessage> message, CancellationToken token)
    {
        if (message.Payload.Payload == null || message.Payload.Payload.Length == 0)
        {
            _payloadSizesValid = false;
        }

        Interlocked.Increment(ref _processedCount);
        return Task.FromResult(InboxHandleResult.Success);
    }
}
