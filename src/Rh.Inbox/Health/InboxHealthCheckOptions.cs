using Rh.Inbox.Abstractions.Configuration;

namespace Rh.Inbox.Health;

/// <summary>
/// Configuration options for inbox health checks.
/// </summary>
public sealed class InboxHealthCheckOptions : IInboxHealthCheckOptions
{
    /// <summary>
    /// Gets or sets whether the health check is enabled.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Gets or sets the tags for the health check.
    /// </summary>
    public string[] Tags { get; set; } = ["inbox"];

    /// <summary>
    /// Gets or sets the queue depth threshold for warning status.
    /// </summary>
    public int QueueDepthWarningThreshold { get; set; } = 1000;

    /// <summary>
    /// Gets or sets the queue depth threshold for critical/unhealthy status.
    /// </summary>
    public int QueueDepthCriticalThreshold { get; set; } = 10000;

    /// <summary>
    /// Gets or sets the processing lag threshold for warning status.
    /// </summary>
    public TimeSpan LagWarningThreshold { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Gets or sets the processing lag threshold for critical/unhealthy status.
    /// </summary>
    public TimeSpan LagCriticalThreshold { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>
    /// Gets or sets the dead letter count threshold for warning status.
    /// </summary>
    public int DeadLetterWarningThreshold { get; set; } = 100;

    /// <summary>
    /// Gets or sets the dead letter count threshold for critical/unhealthy status.
    /// </summary>
    public int DeadLetterCriticalThreshold { get; set; } = 1000;
}