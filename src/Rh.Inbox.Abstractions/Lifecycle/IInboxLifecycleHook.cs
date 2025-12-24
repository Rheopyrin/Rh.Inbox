namespace Rh.Inbox.Abstractions.Lifecycle;

/// <summary>
/// Hook interface for inbox lifecycle events.
/// Implement this interface to perform custom actions when inboxes start or stop.
/// </summary>
public interface IInboxLifecycleHook
{
    /// <summary>
    /// Called when the inbox manager starts.
    /// </summary>
    /// <param name="token">Cancellation token for the startup operation.</param>
    Task OnStart(CancellationToken token = default);

    /// <summary>
    /// Called when the inbox manager stops.
    /// </summary>
    /// <param name="token">Cancellation token for the shutdown operation.</param>
    Task OnStop(CancellationToken token = default);
}