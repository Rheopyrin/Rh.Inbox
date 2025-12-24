namespace Rh.Inbox.Postgres.Services;

/// <summary>
/// Manages PostgreSQL inbox cleanup tasks with support for two run modes:
/// - Execute mode: Runs cleanup tasks once in parallel (loops until no items remain)
/// - Start/Stop mode: Runs continuous cleanup loops (for hosted scenarios)
/// </summary>
public interface IPostgresCleanupTasksManager
{
    /// <summary>
    /// Executes all cleanup tasks once in parallel, looping until all expired items are removed.
    /// </summary>
    /// <param name="token">Cancellation token.</param>
    Task ExecuteAsync(CancellationToken token = default);

    /// <summary>
    /// Executes cleanup tasks for a specific inbox once, looping until all expired items are removed.
    /// </summary>
    /// <param name="inboxName">The name of the inbox to run cleanup for.</param>
    /// <param name="token">Cancellation token.</param>
    Task ExecuteAsync(string inboxName, CancellationToken token = default);

    /// <summary>
    /// Executes cleanup tasks for specified inboxes once in parallel, looping until all expired items are removed.
    /// </summary>
    /// <param name="inboxNames">The names of the inboxes to run cleanup for.</param>
    /// <param name="token">Cancellation token.</param>
    Task ExecuteAsync(IEnumerable<string> inboxNames, CancellationToken token = default);

    /// <summary>
    /// Starts all cleanup tasks in continuous mode with automatic restart on failure.
    /// </summary>
    /// <param name="token">Token that signals when to stop the tasks.</param>
    Task StartAsync(CancellationToken token = default);

    /// <summary>
    /// Starts cleanup tasks for a specific inbox in continuous mode.
    /// </summary>
    /// <param name="inboxName">The name of the inbox to start cleanup for.</param>
    /// <param name="token">Token that signals when to stop the task.</param>
    Task StartAsync(string inboxName, CancellationToken token = default);

    /// <summary>
    /// Starts cleanup tasks for specified inboxes in continuous mode.
    /// </summary>
    /// <param name="inboxNames">The names of the inboxes to start cleanup for.</param>
    /// <param name="token">Token that signals when to stop the tasks.</param>
    Task StartAsync(IEnumerable<string> inboxNames, CancellationToken token = default);

    /// <summary>
    /// Stops all running cleanup tasks gracefully.
    /// </summary>
    /// <param name="token">Cancellation token for the shutdown operation.</param>
    Task StopAsync(CancellationToken token = default);
}
