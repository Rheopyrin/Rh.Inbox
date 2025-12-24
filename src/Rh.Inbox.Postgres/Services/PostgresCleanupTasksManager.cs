using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Rh.Inbox.Abstractions.Lifecycle;

namespace Rh.Inbox.Postgres.Services;

internal sealed class PostgresCleanupTasksManager : IPostgresCleanupTasksManager, IInboxLifecycleHook
{
    private static readonly TimeSpan StopTimeout = TimeSpan.FromSeconds(10);

    private readonly IEnumerable<ICleanupTask> _cleanupTasks;
    private readonly ILogger<PostgresCleanupTasksManager> _logger;

    private readonly ConcurrentDictionary<string, ICleanupTask> _runningTasks = new();

    public PostgresCleanupTasksManager(
        IEnumerable<ICleanupTask> cleanupTasks,
        ILogger<PostgresCleanupTasksManager> logger)
    {
        _cleanupTasks = cleanupTasks;
        _logger = logger;
    }

    #region Execute Once Mode

    public Task ExecuteAsync(CancellationToken token)
    {
        return ExecuteInternalAsync(_cleanupTasks, token);
    }

    public Task ExecuteAsync(string inboxName, CancellationToken token)
    {
        var tasks = _cleanupTasks.Where(t => t.InboxName == inboxName);
        return ExecuteInternalAsync(tasks, token);
    }

    public Task ExecuteAsync(IEnumerable<string> inboxNames, CancellationToken token)
    {
        var names = inboxNames.ToHashSet(StringComparer.Ordinal);
        var tasks = _cleanupTasks.Where(t => names.Contains(t.InboxName));
        return ExecuteInternalAsync(tasks, token);
    }

    private async Task ExecuteInternalAsync(IEnumerable<ICleanupTask> tasks, CancellationToken token)
    {
        var taskList = tasks.ToList();

        if (taskList.Count == 0)
        {
            _logger.LogDebug("No cleanup tasks to execute");
            return;
        }

        _logger.LogInformation("Executing {Count} cleanup task(s)", taskList.Count);

        try
        {
            await Task.WhenAll(taskList.Select(t => ExecuteTaskAsync(t, token)));
            _logger.LogInformation("All cleanup tasks completed successfully");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "One or more cleanup tasks failed");
            throw;
        }
    }

    private async Task ExecuteTaskAsync(ICleanupTask task, CancellationToken token)
    {
        try
        {
            _logger.LogDebug("Executing cleanup task for inbox {InboxName}", task.InboxName);
            await task.ExecuteOnceAsync(token);
            _logger.LogDebug("Cleanup task for inbox {InboxName} completed", task.InboxName);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Cleanup task for inbox {InboxName} failed", task.InboxName);
            throw;
        }
    }

    #endregion

    #region Continuous Mode

    public Task StartAsync(CancellationToken token)
    {
        return StartInternalAsync(_cleanupTasks, token);
    }

    public Task StartAsync(string inboxName, CancellationToken token)
    {
        var tasks = _cleanupTasks.Where(t => t.InboxName == inboxName);
        return StartInternalAsync(tasks, token);
    }

    public Task StartAsync(IEnumerable<string> inboxNames, CancellationToken token)
    {
        var names = inboxNames.ToHashSet(StringComparer.Ordinal);
        var tasks = _cleanupTasks.Where(t => names.Contains(t.InboxName));
        return StartInternalAsync(tasks, token);
    }

    private Task StartInternalAsync(IEnumerable<ICleanupTask> tasks, CancellationToken stoppingToken)
    {
        var taskList = tasks.ToList();

        if (taskList.Count == 0)
        {
            _logger.LogDebug("No cleanup tasks to start");
            return Task.CompletedTask;
        }

        var startedCount = 0;
        foreach (var task in taskList)
        {
            if (_runningTasks.TryAdd(task.TaskName, task))
            {
                _ = task.StartAsync(stoppingToken);
                startedCount++;
            }
        }

        if (startedCount > 0)
        {
            _logger.LogInformation("Started {Count} cleanup task(s) in continuous mode", startedCount);
        }

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken token)
    {
        var tasksToStop = _runningTasks.Values.ToList();
        _runningTasks.Clear();

        if (tasksToStop.Count == 0)
        {
            _logger.LogDebug("No cleanup tasks to stop");
            return;
        }

        _logger.LogInformation("Stopping {Count} cleanup task(s)", tasksToStop.Count);

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeoutCts.CancelAfter(StopTimeout);

        try
        {
            await Task.WhenAll(tasksToStop.Select(t => t.StopAsync(timeoutCts.Token)));
            _logger.LogInformation("All cleanup tasks stopped successfully");
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
        {
            _logger.LogWarning("Cleanup tasks did not stop within the timeout period");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error stopping cleanup tasks");
        }
    }

    #endregion

    #region IInboxLifecycleHook

    Task IInboxLifecycleHook.OnStart(CancellationToken token)
    {
        return StartAsync(token);
    }

    Task IInboxLifecycleHook.OnStop(CancellationToken token)
    {
        return StopAsync(token);
    }

    #endregion
}
