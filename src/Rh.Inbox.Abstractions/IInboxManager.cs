namespace Rh.Inbox.Abstractions;

/// <summary>
/// Service for retrieving inbox instances.
/// Use this to get references to specific inboxes for inspection or advanced operations.
/// </summary>
public interface IInboxManager
{
    /// <summary>
    /// Gets the default inbox (first registered inbox).
    /// </summary>
    /// <returns>The default inbox instance.</returns>
    /// <exception>InboxNotFoundException - Thrown when no inbox is registered.</exception>
    IInbox GetInbox();

    /// <summary>
    /// Gets an inbox by its name.
    /// </summary>
    /// <param name="name">The unique name of the inbox.</param>
    /// <returns>The inbox instance.</returns>
    /// <exception>InboxNotFoundException - Thrown when no inbox with the specified name exists.</exception>
    IInbox GetInbox(string name);

    /// <summary>
    /// Gets whether the inbox manager is currently running.
    /// </summary>
    bool IsRunning { get; }

    /// <summary>
    /// Starts all registered inboxes and begins message processing.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for the startup operation.</param>
    Task StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops all registered inboxes and message processing.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for the shutdown operation.</param>
    Task StopAsync(CancellationToken cancellationToken = default);
}