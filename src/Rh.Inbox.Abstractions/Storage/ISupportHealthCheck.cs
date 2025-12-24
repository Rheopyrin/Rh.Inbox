using Rh.Inbox.Abstractions.Health;

namespace Rh.Inbox.Abstractions.Storage;

/// <summary>
/// Interface for storage providers that support health check metrics.
/// Implement this interface to expose inbox health information for monitoring.
/// </summary>
public interface ISupportHealthCheck
{
    /// <summary>
    /// Gets health metrics for the inbox.
    /// </summary>
    /// <param name="token">Cancellation token.</param>
    /// <returns>Health metrics including message counts and oldest pending message age.</returns>
    Task<InboxHealthMetrics> GetHealthMetricsAsync(CancellationToken token = default);
}
