using Microsoft.Extensions.Logging;
using Npgsql;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Postgres.Options;
using Rh.Inbox.Postgres.Scripts;

namespace Rh.Inbox.Postgres.Services;

internal sealed class DeadLetterCleanupService : ICleanupTask
{
    private readonly PostgresInboxProviderOptions _providerOptions;
    private readonly CleanupTaskOptions _cleanupOptions;
    private readonly IInboxConfiguration _configuration;
    private readonly ILogger<DeadLetterCleanupService> _logger;

    private Task? _executeTask;
    private CancellationTokenSource? _stoppingTokenSource;

    public DeadLetterCleanupService(
        IInboxConfiguration configuration,
        CleanupTaskOptions cleanupOptions,
        IProviderOptionsAccessor optionsAccessor,
        ILogger<DeadLetterCleanupService> logger)
    {
        _providerOptions = optionsAccessor.GetForInbox(configuration.InboxName);
        _cleanupOptions = cleanupOptions;
        _configuration = configuration;
        _logger = logger;
    }

    public string TaskName => $"{nameof(DeadLetterCleanupService)}:{_configuration.InboxName}";

    public string InboxName => _configuration.InboxName;

    public async Task ExecuteOnceAsync(CancellationToken token)
    {
        _logger.LogDebug(
            "Executing dead letter cleanup once for table {TableName}",
            _providerOptions.DeadLetterTableName);

        var cleanupSql = PostgresSqlScriptsBase.BuildDeadLetterCleanup(_providerOptions.DeadLetterTableName);
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
                // Expected when token is cancelled before task completes
            }
        }

        _stoppingTokenSource?.Dispose();
        _stoppingTokenSource = null;
    }

    private async Task ExecuteContinuousAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Dead letter cleanup started for table {TableName}",
            _providerOptions.DeadLetterTableName);

        try
        {
            await RunCleanupLoopWithRestartAsync(stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Expected during shutdown
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Cleanup task failed for {TableName}", _providerOptions.DeadLetterTableName);
        }

        _logger.LogInformation(
            "Dead letter cleanup stopped for table {TableName}",
            _providerOptions.DeadLetterTableName);
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
                    _providerOptions.DeadLetterTableName,
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
        var cleanupSql = PostgresSqlScriptsBase.BuildDeadLetterCleanup(_providerOptions.DeadLetterTableName);

        _logger.LogDebug(
            "Starting cleanup loop for table {TableName}. DeadLetterMaxMessageLifetime: {Lifetime}, Cleanup interval: {CleanupInterval}",
            _providerOptions.DeadLetterTableName,
            _configuration.Options.DeadLetterMaxMessageLifetime,
            _cleanupOptions.Interval);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(_cleanupOptions.Interval, stoppingToken);
            await CleanupAllExpiredRecordsAsync(cleanupSql, stoppingToken);
        }
    }

    private async Task CleanupAllExpiredRecordsAsync(string cleanupSql, CancellationToken token)
    {
        var expirationTime = _configuration.DateTimeProvider.GetUtcNow() - _configuration.Options.DeadLetterMaxMessageLifetime;
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
                break;

            totalDeleted += batchDeleted;

            if (batchDeleted < batchSize)
                break;
        }

        if (totalDeleted > 0)
        {
            _logger.LogDebug(
                "Deleted {Count} expired dead letter messages from {TableName}",
                totalDeleted,
                _providerOptions.DeadLetterTableName);
        }
    }
}