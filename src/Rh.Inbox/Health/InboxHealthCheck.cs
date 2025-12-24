using Microsoft.Extensions.Diagnostics.HealthChecks;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Management;

namespace Rh.Inbox.Health;

internal sealed class InboxHealthCheck : IHealthCheck
{
    private readonly InboxManager _inboxManager;
    private readonly string _inboxName;
    private readonly IInboxHealthCheckOptions _options;
    private readonly IDateTimeProvider _dateTimeProvider;

    public InboxHealthCheck(
        InboxManager inboxManager,
        string inboxName,
        IInboxHealthCheckOptions options,
        IDateTimeProvider dateTimeProvider)
    {
        _inboxManager = inboxManager;
        _inboxName = inboxName;
        _options = options;
        _dateTimeProvider = dateTimeProvider;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (!_inboxManager.IsRunning)
        {
            return HealthCheckResult.Degraded($"Inbox '{_inboxName}' is not started yet");
        }

        var inbox = _inboxManager.GetInboxInternal(_inboxName);
        var storageProvider = inbox.GetStorageProvider();

        if (storageProvider is not ISupportHealthCheck healthCheckProvider)
        {
            return HealthCheckResult.Healthy("Storage provider does not support health checks");
        }

        var metrics = await healthCheckProvider.GetHealthMetricsAsync(cancellationToken);
        var data = new Dictionary<string, object>
        {
            ["inbox"] = _inboxName,
            ["pendingCount"] = metrics.PendingCount,
            ["capturedCount"] = metrics.CapturedCount,
            ["deadLetterCount"] = metrics.DeadLetterCount
        };

        var queueDepth = metrics.PendingCount + metrics.CapturedCount;
        TimeSpan? lag = null;

        if (metrics.OldestPendingMessageAt.HasValue)
        {
            lag = _dateTimeProvider.GetUtcNow() - metrics.OldestPendingMessageAt.Value;
            data["lagSeconds"] = lag.Value.TotalSeconds;
            data["oldestPendingAt"] = metrics.OldestPendingMessageAt.Value;
        }

        // Check critical thresholds first
        if (queueDepth >= _options.QueueDepthCriticalThreshold)
        {
            return HealthCheckResult.Unhealthy(
                $"Queue depth ({queueDepth}) exceeds critical threshold ({_options.QueueDepthCriticalThreshold})",
                data: data);
        }

        if (lag.HasValue && lag.Value >= _options.LagCriticalThreshold)
        {
            return HealthCheckResult.Unhealthy(
                $"Processing lag ({lag.Value.TotalMinutes:F1} minutes) exceeds critical threshold ({_options.LagCriticalThreshold.TotalMinutes} minutes)",
                data: data);
        }

        if (metrics.DeadLetterCount >= _options.DeadLetterCriticalThreshold)
        {
            return HealthCheckResult.Unhealthy(
                $"Dead letter count ({metrics.DeadLetterCount}) exceeds critical threshold ({_options.DeadLetterCriticalThreshold})",
                data: data);
        }

        // Check warning thresholds
        if (queueDepth >= _options.QueueDepthWarningThreshold)
        {
            return HealthCheckResult.Degraded(
                $"Queue depth ({queueDepth}) exceeds warning threshold ({_options.QueueDepthWarningThreshold})",
                data: data);
        }

        if (lag.HasValue && lag.Value >= _options.LagWarningThreshold)
        {
            return HealthCheckResult.Degraded(
                $"Processing lag ({lag.Value.TotalMinutes:F1} minutes) exceeds warning threshold ({_options.LagWarningThreshold.TotalMinutes} minutes)",
                data: data);
        }

        if (metrics.DeadLetterCount >= _options.DeadLetterWarningThreshold)
        {
            return HealthCheckResult.Degraded(
                $"Dead letter count ({metrics.DeadLetterCount}) exceeds warning threshold ({_options.DeadLetterWarningThreshold})",
                data: data);
        }

        return HealthCheckResult.Healthy($"Inbox '{_inboxName}' is healthy", data: data);
    }
}