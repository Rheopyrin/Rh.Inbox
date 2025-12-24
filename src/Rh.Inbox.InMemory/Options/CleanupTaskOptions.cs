namespace Rh.Inbox.InMemory.Options;

/// <summary>
/// Configuration options for cleanup tasks.
/// </summary>
public sealed class CleanupTaskOptions
{
    /// <summary>
    /// Gets or sets the interval between cleanup cycles.
    /// Default is 5 minutes.
    /// </summary>
    public TimeSpan Interval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Gets or sets the delay before restarting the cleanup loop after a failure.
    /// Default is 30 seconds.
    /// </summary>
    public TimeSpan RestartDelay { get; set; } = TimeSpan.FromSeconds(30);
}
