namespace Rh.Inbox.Abstractions.Lifecycle;

/// <summary>
/// Provides global lifecycle management for all inboxes, including cancellation signaling.
/// Registered as a singleton - shared by all inboxes.
/// </summary>
public interface IInboxLifecycle
{
    /// <summary>
    /// Gets the cancellation token that is triggered when inboxes are stopping.
    /// </summary>
    CancellationToken StoppingToken { get; }

    /// <summary>
    /// Gets whether the inboxes are currently running.
    /// </summary>
    bool IsRunning { get; }

    /// <summary>
    /// Marks the lifecycle as running.
    /// </summary>
    void Start();

    /// <summary>
    /// Marks the lifecycle as stopped and triggers the stopping token.
    /// </summary>
    void Stop();
}