namespace Rh.Inbox.Postgres.Services;

/// <summary>
/// Internal interface for cleanup tasks that can run in two modes:
/// - Execute once: runs cleanup until no items remain
/// - Continuous: runs cleanup loop with restart capability
/// </summary>
internal interface ICleanupTask
{
    /// <summary>
    /// Gets the unique name of this cleanup task (combination of task type and inbox name).
    /// </summary>
    string TaskName { get; }

    /// <summary>
    /// Gets the name of the inbox this cleanup task is associated with.
    /// </summary>
    string InboxName { get; }

    /// <summary>
    /// Executes the cleanup task once, looping until all expired items are removed.
    /// </summary>
    /// <param name="token">Cancellation token.</param>
    Task ExecuteOnceAsync(CancellationToken token);

    /// <summary>
    /// Starts the cleanup task in continuous mode with automatic restart on failure.
    /// </summary>
    /// <param name="stoppingToken">Token that signals when to stop the task.</param>
    Task StartAsync(CancellationToken stoppingToken);

    /// <summary>
    /// Stops the cleanup task gracefully.
    /// </summary>
    /// <param name="token">Cancellation token for the shutdown operation.</param>
    Task StopAsync(CancellationToken token);
}
