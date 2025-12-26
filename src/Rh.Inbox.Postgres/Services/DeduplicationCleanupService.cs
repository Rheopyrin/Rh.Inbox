using Microsoft.Extensions.Logging;
using Npgsql;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Postgres.Options;
using Rh.Inbox.Postgres.Scripts;

namespace Rh.Inbox.Postgres.Services;

internal sealed class DeduplicationCleanupService : ICleanupTask
{
    private readonly PostgresInboxProviderOptions _providerOptions;
    private readonly CleanupTaskOptions _cleanupOptions;
    private readonly IInboxConfiguration _configuration;
    private readonly ILogger<DeduplicationCleanupService> _logger;

    private Task? _executeTask;
    private CancellationTokenSource? _stoppingTokenSource;

    public DeduplicationCleanupService(
        IInboxConfiguration configuration,
        CleanupTaskOptions cleanupOptions,
        IProviderOptionsAccessor optionsAccessor,
        ILogger<DeduplicationCleanupService> logger)
    {
        _providerOptions = optionsAccessor.GetForInbox(configuration.InboxName);
        _cleanupOptions = cleanupOptions;
        _configuration = configuration;
        _logger = logger;
    }

    public string TaskName => $"{nameof(DeduplicationCleanupService)}:{_configuration.InboxName}";

    public string InboxName => _configuration.InboxName;

    public async Task ExecuteOnceAsync(CancellationToken token)
    {
        _logger.LogDebug(
            "Executing deduplication cleanup once for table {TableName}",
            _providerOptions.DeduplicationTableName);

        var cleanupSql = PostgresSqlScriptsBase.BuildDeduplicationCleanup(_providerOptions.DeduplicationTableName);
        await CleanupAllExpiredRecordsAsync(cleanupSql, token);
    }

    public Task StartAsync(CancellationToken stoppingToken)
    {
        _stoppingTokenSource = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        _executeTask = ExecuteContinuousAsync(_stoppingTokenSource.Token);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken token)
    {
        if (_stoppingTokenSource != null)
        {
            await _stoppingTokenSource.CancelAsync();
        }

        if (_executeTask != null)
        {
            try
            {
                await _executeTask.WaitAsync(token);
            }
            catch (OperationCanceledException)
            {
            }
        }

        _stoppingTokenSource?.Dispose();
        _stoppingTokenSource = null;
    }

    private async Task ExecuteContinuousAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Deduplication cleanup started for table {TableName}",
            _providerOptions.DeduplicationTableName);

        try
        {
            await RunCleanupLoopWithRestartAsync(stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Cleanup task failed for {TableName}", _providerOptions.DeduplicationTableName);
        }

        _logger.LogInformation(
            "Deduplication cleanup stopped for table {TableName}",
            _providerOptions.DeduplicationTableName);
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
                    "Cleanup loop for table {TableName} failed. Restarting in {RestartDelay}",
                    _providerOptions.DeduplicationTableName,
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
        var cleanupSql = PostgresSqlScriptsBase.BuildDeduplicationCleanup(_providerOptions.DeduplicationTableName);

        _logger.LogDebug(
            "Starting cleanup loop for table {TableName}. DeduplicationInterval: {Interval}, Cleanup interval: {CleanupInterval}",
            _providerOptions.DeduplicationTableName,
            _configuration.Options.DeduplicationInterval,
            _cleanupOptions.Interval);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(_cleanupOptions.Interval, stoppingToken);
            await CleanupAllExpiredRecordsAsync(cleanupSql, stoppingToken);
        }
    }

    private async Task CleanupAllExpiredRecordsAsync(string cleanupSql, CancellationToken token)
    {
        var expirationTime = _configuration.DateTimeProvider.GetUtcNow() - _configuration.Options.DeduplicationInterval;
        var batchSize = _cleanupOptions.BatchSize;
        var totalDeleted = 0;

        await using var connection = await _providerOptions.DataSource.OpenConnectionAsync(token);

        while (!token.IsCancellationRequested)
        {
            await using var cmd = new NpgsqlCommand(cleanupSql, connection);
            cmd.Parameters.AddWithValue("expirationTime", expirationTime);
            cmd.Parameters.AddWithValue("batchSize", batchSize);

            var batchDeleted = await cmd.ExecuteNonQueryAsync(token);

            if (batchDeleted == 0)
            {
                break;
            }

            totalDeleted += batchDeleted;

            if (batchDeleted < batchSize)
            {
                break;
            }
        }

        if (totalDeleted > 0)
        {
            _logger.LogDebug(
                "Deleted {Count} expired deduplication records from {TableName}",
                totalDeleted,
                _providerOptions.DeduplicationTableName);
        }
    }
}