using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Tests.Integration.Common.TestMessages;

namespace Rh.Inbox.Tests.Integration.Common.Handlers;

/// <summary>
/// Handler that blocks until explicitly released. Used to test locking behavior.
/// </summary>
public class BlockingFifoHandler<TMessage> : IFifoInboxHandler<TMessage>
    where TMessage : class, IHasGroupId
{
    private readonly SemaphoreSlim _gate = new(0);
    private readonly List<int> _capturedSequence = new();
    private int _currentlyProcessing;
    private int _processedCount;

    public IReadOnlyList<int> CapturedSequence
    {
        get
        {
            lock (_capturedSequence)
                return _capturedSequence.ToList();
        }
    }

    public int CurrentlyProcessing => _currentlyProcessing;
    public int ProcessedCount => _processedCount;

    public async Task<InboxHandleResult> HandleAsync(InboxMessageEnvelope<TMessage> message, CancellationToken token)
    {
        var sequence = (message.Payload as FifoMessage)?.Sequence ?? -1;

        lock (_capturedSequence)
            _capturedSequence.Add(sequence);

        Interlocked.Increment(ref _currentlyProcessing);

        try
        {
            // Wait until released
            await _gate.WaitAsync(token);
            Interlocked.Increment(ref _processedCount);
            return InboxHandleResult.Success;
        }
        finally
        {
            Interlocked.Decrement(ref _currentlyProcessing);
        }
    }

    public void ReleaseOne() => _gate.Release(1);
    public void ReleaseAll() => _gate.Release(100);

    public void Reset()
    {
        lock (_capturedSequence)
            _capturedSequence.Clear();
        _processedCount = 0;
        _currentlyProcessing = 0;
    }
}
