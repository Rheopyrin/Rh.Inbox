using Microsoft.Extensions.Logging;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Lifecycle;
using Rh.Inbox.InMemory.Options;

namespace Rh.Inbox.InMemory;

/// <summary>
/// Service that periodically cleans up expired dead letter messages from in-memory storage.
/// Registered as IInboxLifecycleHook and started/stopped by the inbox lifecycle.
/// </summary>
internal sealed class InMemoryDeadLetterCleanupService : IInboxLifecycleHook
{
    private readonly IInboxLifecycle _lifecycle;
    private readonly ILogger<InMemoryDeadLetterCleanupService> _logger;
    private readonly InMemoryInboxProviderOptions _providerOptions;
    private readonly CleanupTaskOptions _cleanupOptions;
    private readonly IInboxConfiguration _configuration;

    private Task? _executeTask;

    public InMemoryDeadLetterCleanupService(
        IInboxConfiguration configuration,
        CleanupTaskOptions cleanupOptions,
        IProviderOptionsAccessor optionsAccessor,
        IInboxLifecycle lifecycle,
        ILogger<InMemoryDeadLetterCleanupService> logger)
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
            "In-memory dead letter cleanup started for inbox {InboxName}",
            _configuration.InboxName);

        try
        {
            await RunCleanupLoopWithRestartAsync(_lifecycle.StoppingToken);
        }
        catch (OperationCanceledException) when (_lifecycle.StoppingToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Dead letter cleanup task failed for inbox {InboxName}", _configuration.InboxName);
        }

        _logger.LogInformation(
            "In-memory dead letter cleanup stopped for inbox {InboxName}",
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
                    "Dead letter cleanup loop for inbox {InboxName} failed. Restarting in {RestartDelay}",
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
            "Starting dead letter cleanup loop for inbox {InboxName}. DeadLetterMaxMessageLifetime: {Lifetime}, Cleanup interval: {CleanupInterval}",
            _configuration.InboxName,
            _configuration.Options.DeadLetterMaxMessageLifetime,
            _cleanupOptions.Interval);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(_cleanupOptions.Interval, stoppingToken);
            await CleanupExpiredRecordsAsync(stoppingToken);
        }
    }

    private async Task CleanupExpiredRecordsAsync(CancellationToken token)
    {
        var expirationTime = _configuration.DateTimeProvider.GetUtcNow() - _configuration.Options.DeadLetterMaxMessageLifetime;
        var deleted = await _providerOptions.DeadLetterStore.CleanupExpiredAsync(expirationTime, token);

        if (deleted > 0)
        {
            _logger.LogDebug(
                "Deleted {Count} expired in-memory dead letter messages from inbox {InboxName}",
                deleted,
                _configuration.InboxName);
        }
    }
}