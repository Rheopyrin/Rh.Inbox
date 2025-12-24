using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;

namespace Rh.Inbox.Tests.Integration.Common.Handlers;

/// <summary>
/// Handler that fails the first N messages, then succeeds.
/// </summary>
public class FailFirstThenSucceedFifoHandler<TMessage> : IFifoInboxHandler<TMessage>
    where TMessage : class, IHasGroupId
{
    private readonly int _failCount;
    private int _currentFailures;
    private int _successCount;

    public FailFirstThenSucceedFifoHandler(int failCount)
    {
        _failCount = failCount;
    }

    public int FailCount => _currentFailures;
    public int SuccessCount => _successCount;

    public Task<InboxHandleResult> HandleAsync(InboxMessageEnvelope<TMessage> message, CancellationToken token)
    {
        if (_currentFailures < _failCount)
        {
            Interlocked.Increment(ref _currentFailures);
            return Task.FromResult(InboxHandleResult.Failed);
        }

        Interlocked.Increment(ref _successCount);
        return Task.FromResult(InboxHandleResult.Success);
    }

    public void Reset()
    {
        _currentFailures = 0;
        _successCount = 0;
    }
}
