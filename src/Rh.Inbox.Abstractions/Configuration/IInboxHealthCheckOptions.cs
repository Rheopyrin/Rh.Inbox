namespace Rh.Inbox.Abstractions.Configuration;

/// <summary>
/// Configuration options for inbox health checks.
/// </summary>
public interface IInboxHealthCheckOptions
{
    /// <summary>
    /// Gets or sets whether the health check is enabled.
    /// </summary>
    bool Enabled { get; set; }

    /// <summary>
    /// Gets or sets the tags for the health check.
    /// </summary>
    string[] Tags { get; set; }

    /// <summary>
    /// Gets or sets the queue depth threshold for warning status.
    /// </summary>
    int QueueDepthWarningThreshold { get; set; }

    /// <summary>
    /// Gets or sets the queue depth threshold for critical/unhealthy status.
    /// </summary>
    int QueueDepthCriticalThreshold { get; set; }

    /// <summary>
    /// Gets or sets the processing lag threshold for warning status.
    /// </summary>
    TimeSpan LagWarningThreshold { get; set; }

    /// <summary>
    /// Gets or sets the processing lag threshold for critical/unhealthy status.
    /// </summary>
    TimeSpan LagCriticalThreshold { get; set; }

    /// <summary>
    /// Gets or sets the dead letter count threshold for warning status.
    /// </summary>
    int DeadLetterWarningThreshold { get; set; }

    /// <summary>
    /// Gets or sets the dead letter count threshold for critical/unhealthy status.
    /// </summary>
    int DeadLetterCriticalThreshold { get; set; }
}