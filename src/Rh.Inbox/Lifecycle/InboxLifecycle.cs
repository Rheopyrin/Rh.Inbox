using Rh.Inbox.Abstractions.Lifecycle;

namespace Rh.Inbox.Lifecycle;

internal sealed class InboxLifecycle : IInboxLifecycle, IDisposable
{
    private readonly CancellationTokenSource _stoppingCts = new();
    private int _isRunning;

    public CancellationToken StoppingToken => _stoppingCts.Token;

    public bool IsRunning => Interlocked.CompareExchange(ref _isRunning, 0, 0) == 1;

    public void Start() => Interlocked.Exchange(ref _isRunning, 1);

    public void Stop()
    {
        if (Interlocked.Exchange(ref _isRunning, 0) == 1)
        {
            _stoppingCts.Cancel();
        }
    }

    public void Dispose() => _stoppingCts.Dispose();
}
