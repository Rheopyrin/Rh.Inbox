using Microsoft.Extensions.Logging;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Lifecycle;
using Rh.Inbox.InMemory.Options;

namespace Rh.Inbox.InMemory;

/// <summary>
/// Service that periodically cleans up expired deduplication records from in-memory storage.
/// Registered as IInboxLifecycleHook and started/stopped by the inbox lifecycle.
/// </summary>
internal sealed class InMemoryDeduplicationCleanupService : IInboxLifecycleHook
{
    private readonly IInboxLifecycle _lifecycle;
    private readonly ILogger<InMemoryDeduplicationCleanupService> _logger;
    private readonly InMemoryInboxProviderOptions _providerOptions;
    private readonly CleanupTaskOptions _cleanupOptions;
    private readonly IInboxConfiguration _configuration;

    private Task? _executeTask;

    public InMemoryDeduplicationCleanupService(
        IInboxConfiguration configuration,
        CleanupTaskOptions cleanupOptions,
        IProviderOptionsAccessor optionsAccessor,
        IInboxLifecycle lifecycle,
        ILogger<InMemoryDeduplicationCleanupService> logger)
    {
        _providerOptions = optionsAccessor.GetForInbox(configuration.InboxName);
        _cleanupOptions = cleanupOptions;
        _configuration = configuration;
        _lifecycle = lifecycle;
        _logger = logger;
    }

    /// <summary>
    /// Starts the cleanup service.
    /// The task runs until the cancellation token is triggered.
    /// </summary>
    public Task OnStart(CancellationToken stoppingToken)
    {
        _executeTask = ExecuteAsync();
        return Task.CompletedTask;
    }

    public async Task OnStop(CancellationToken stoppingToken)
    {
        if (_executeTask != null)
        {
            try
            {
                await _executeTask.WaitAsync(stoppingToken);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    private async Task ExecuteAsync()
    {
        _logger.LogInformation(
            "In-memory deduplication cleanup started for inbox {InboxName}",
            _configuration.InboxName);

        try
        {
            await RunCleanupLoopWithRestartAsync(_lifecycle.StoppingToken);
        }
        catch (OperationCanceledException) when (_lifecycle.StoppingToken.IsCancellationRequested)
        {
            // Expected during shutdown
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Cleanup task failed for inbox {InboxName}", _configuration.InboxName);
        }

        _logger.LogInformation(
            "In-memory deduplication cleanup stopped for inbox {InboxName}",
            _configuration.InboxName);
    }

    private async Task RunCleanupLoopWithRestartAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunCleanupLoopAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Cleanup loop for inbox {InboxName} failed. Restarting in {RestartDelay}",
                    _configuration.InboxName,
                    _cleanupOptions.RestartDelay);

                try
                {
                    await Task.Delay(_cleanupOptions.RestartDelay, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }
    }

    private async Task RunCleanupLoopAsync(CancellationToken stoppingToken)
    {
        _logger.LogDebug(
            "Starting cleanup loop for inbox {InboxName}. DeduplicationInterval: {Interval}, Cleanup interval: {CleanupInterval}",
            _configuration.InboxName,
            _configuration.Options.DeduplicationInterval,
            _cleanupOptions.Interval);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(_cleanupOptions.Interval, stoppingToken);
            CleanupExpiredRecords();
        }
    }

    private void CleanupExpiredRecords()
    {
        var expirationTime = _configuration.DateTimeProvider.GetUtcNow() - _configuration.Options.DeduplicationInterval;
        var deleted = _providerOptions.DeduplicationStore.CleanupExpired(expirationTime);

        if (deleted > 0)
        {
            _logger.LogDebug(
                "Deleted {Count} expired in-memory deduplication records from inbox {InboxName}",
                deleted,
                _configuration.InboxName);
        }
    }
}