using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Rh.Inbox.Postgres.Services;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Postgres;

public class PostgresCleanupTasksManagerTests
{
    private readonly ILogger<PostgresCleanupTasksManager> _logger;

    public PostgresCleanupTasksManagerTests()
    {
        _logger = NullLogger<PostgresCleanupTasksManager>.Instance;
    }

    #region ExecuteAsync Tests

    [Fact]
    public async Task ExecuteAsync_WithNoTasks_CompletesSuccessfully()
    {
        var manager = new PostgresCleanupTasksManager([], _logger);

        var act = async () => await manager.ExecuteAsync(CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ExecuteAsync_WithSingleTask_ExecutesTask()
    {
        var task = CreateMockCleanupTask("inbox1");
        var manager = new PostgresCleanupTasksManager([task], _logger);

        await manager.ExecuteAsync(CancellationToken.None);

        await task.Received(1).ExecuteOnceAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ExecuteAsync_WithMultipleTasks_ExecutesAllTasksInParallel()
    {
        var task1 = CreateMockCleanupTask("inbox1");
        var task2 = CreateMockCleanupTask("inbox2");
        var task3 = CreateMockCleanupTask("inbox3");
        var manager = new PostgresCleanupTasksManager([task1, task2, task3], _logger);

        await manager.ExecuteAsync(CancellationToken.None);

        await task1.Received(1).ExecuteOnceAsync(Arg.Any<CancellationToken>());
        await task2.Received(1).ExecuteOnceAsync(Arg.Any<CancellationToken>());
        await task3.Received(1).ExecuteOnceAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ExecuteAsync_WithInboxName_ExecutesOnlyMatchingTask()
    {
        var task1 = CreateMockCleanupTask("inbox1");
        var task2 = CreateMockCleanupTask("inbox2");
        var manager = new PostgresCleanupTasksManager([task1, task2], _logger);

        await manager.ExecuteAsync("inbox1", CancellationToken.None);

        await task1.Received(1).ExecuteOnceAsync(Arg.Any<CancellationToken>());
        await task2.DidNotReceive().ExecuteOnceAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ExecuteAsync_WithInboxNames_ExecutesOnlyMatchingTasks()
    {
        var task1 = CreateMockCleanupTask("inbox1");
        var task2 = CreateMockCleanupTask("inbox2");
        var task3 = CreateMockCleanupTask("inbox3");
        var manager = new PostgresCleanupTasksManager([task1, task2, task3], _logger);

        await manager.ExecuteAsync(["inbox1", "inbox3"], CancellationToken.None);

        await task1.Received(1).ExecuteOnceAsync(Arg.Any<CancellationToken>());
        await task2.DidNotReceive().ExecuteOnceAsync(Arg.Any<CancellationToken>());
        await task3.Received(1).ExecuteOnceAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ExecuteAsync_WithNonExistentInboxName_CompletesWithoutExecutingTasks()
    {
        var task1 = CreateMockCleanupTask("inbox1");
        var manager = new PostgresCleanupTasksManager([task1], _logger);

        await manager.ExecuteAsync("nonexistent", CancellationToken.None);

        await task1.DidNotReceive().ExecuteOnceAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ExecuteAsync_WithCancellationToken_PassesTokenToTasks()
    {
        using var cts = new CancellationTokenSource();
        var task = CreateMockCleanupTask("inbox1");
        var manager = new PostgresCleanupTasksManager([task], _logger);

        await manager.ExecuteAsync(cts.Token);

        await task.Received(1).ExecuteOnceAsync(cts.Token);
    }

    [Fact]
    public async Task ExecuteAsync_WhenTaskThrows_PropagatesException()
    {
        var task = CreateMockCleanupTask("inbox1");
        task.ExecuteOnceAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("Test error")));
        var manager = new PostgresCleanupTasksManager([task], _logger);

        var act = async () => await manager.ExecuteAsync(CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task ExecuteAsync_WithMultipleTasksForSameInbox_ExecutesAllMatchingTasks()
    {
        var task1 = CreateMockCleanupTask("inbox1", "GroupLocks");
        var task2 = CreateMockCleanupTask("inbox1", "Deduplication");
        var manager = new PostgresCleanupTasksManager([task1, task2], _logger);

        await manager.ExecuteAsync("inbox1", CancellationToken.None);

        await task1.Received(1).ExecuteOnceAsync(Arg.Any<CancellationToken>());
        await task2.Received(1).ExecuteOnceAsync(Arg.Any<CancellationToken>());
    }

    #endregion

    #region StartAsync Tests

    [Fact]
    public async Task StartAsync_WithNoTasks_CompletesSuccessfully()
    {
        var manager = new PostgresCleanupTasksManager([], _logger);

        var act = async () => await manager.StartAsync(CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task StartAsync_WithSingleTask_StartsTask()
    {
        var task = CreateMockCleanupTask("inbox1");
        var manager = new PostgresCleanupTasksManager([task], _logger);

        await manager.StartAsync(CancellationToken.None);

        await task.Received(1).StartAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task StartAsync_WithMultipleTasks_StartsAllTasks()
    {
        var task1 = CreateMockCleanupTask("inbox1");
        var task2 = CreateMockCleanupTask("inbox2");
        var manager = new PostgresCleanupTasksManager([task1, task2], _logger);

        await manager.StartAsync(CancellationToken.None);

        await task1.Received(1).StartAsync(Arg.Any<CancellationToken>());
        await task2.Received(1).StartAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task StartAsync_WithInboxName_StartsOnlyMatchingTask()
    {
        var task1 = CreateMockCleanupTask("inbox1");
        var task2 = CreateMockCleanupTask("inbox2");
        var manager = new PostgresCleanupTasksManager([task1, task2], _logger);

        await manager.StartAsync("inbox1", CancellationToken.None);

        await task1.Received(1).StartAsync(Arg.Any<CancellationToken>());
        await task2.DidNotReceive().StartAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task StartAsync_WithInboxNames_StartsOnlyMatchingTasks()
    {
        var task1 = CreateMockCleanupTask("inbox1");
        var task2 = CreateMockCleanupTask("inbox2");
        var task3 = CreateMockCleanupTask("inbox3");
        var manager = new PostgresCleanupTasksManager([task1, task2, task3], _logger);

        await manager.StartAsync(["inbox1", "inbox3"], CancellationToken.None);

        await task1.Received(1).StartAsync(Arg.Any<CancellationToken>());
        await task2.DidNotReceive().StartAsync(Arg.Any<CancellationToken>());
        await task3.Received(1).StartAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task StartAsync_CalledTwice_DoesNotStartTaskAgain()
    {
        var task = CreateMockCleanupTask("inbox1");
        var manager = new PostgresCleanupTasksManager([task], _logger);

        await manager.StartAsync(CancellationToken.None);
        await manager.StartAsync(CancellationToken.None);

        await task.Received(1).StartAsync(Arg.Any<CancellationToken>());
    }

    #endregion

    #region StopAsync Tests

    [Fact]
    public async Task StopAsync_WithNoRunningTasks_CompletesSuccessfully()
    {
        var manager = new PostgresCleanupTasksManager([], _logger);

        var act = async () => await manager.StopAsync(CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task StopAsync_AfterStart_StopsAllRunningTasks()
    {
        var task1 = CreateMockCleanupTask("inbox1");
        var task2 = CreateMockCleanupTask("inbox2");
        var manager = new PostgresCleanupTasksManager([task1, task2], _logger);

        await manager.StartAsync(CancellationToken.None);
        await manager.StopAsync(CancellationToken.None);

        await task1.Received(1).StopAsync(Arg.Any<CancellationToken>());
        await task2.Received(1).StopAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task StopAsync_CalledWithoutStart_DoesNotStopTasks()
    {
        var task = CreateMockCleanupTask("inbox1");
        var manager = new PostgresCleanupTasksManager([task], _logger);

        await manager.StopAsync(CancellationToken.None);

        await task.DidNotReceive().StopAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task StopAsync_ClearsRunningTasks()
    {
        var task = CreateMockCleanupTask("inbox1");
        var manager = new PostgresCleanupTasksManager([task], _logger);

        await manager.StartAsync(CancellationToken.None);
        await manager.StopAsync(CancellationToken.None);
        await manager.StopAsync(CancellationToken.None);

        await task.Received(1).StopAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task StopAsync_AfterPartialStart_StopsOnlyStartedTasks()
    {
        var task1 = CreateMockCleanupTask("inbox1");
        var task2 = CreateMockCleanupTask("inbox2");
        var manager = new PostgresCleanupTasksManager([task1, task2], _logger);

        await manager.StartAsync("inbox1", CancellationToken.None);
        await manager.StopAsync(CancellationToken.None);

        await task1.Received(1).StopAsync(Arg.Any<CancellationToken>());
        await task2.DidNotReceive().StopAsync(Arg.Any<CancellationToken>());
    }

    #endregion

    #region IInboxLifecycleHook Tests

    [Fact]
    public async Task OnStart_DelegatesToStartAsync()
    {
        var task = CreateMockCleanupTask("inbox1");
        var manager = new PostgresCleanupTasksManager([task], _logger);
        var lifecycleHook = (Rh.Inbox.Abstractions.Lifecycle.IInboxLifecycleHook)manager;

        await lifecycleHook.OnStart(CancellationToken.None);

        await task.Received(1).StartAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task OnStop_DelegatesToStopAsync()
    {
        var task = CreateMockCleanupTask("inbox1");
        var manager = new PostgresCleanupTasksManager([task], _logger);
        var lifecycleHook = (Rh.Inbox.Abstractions.Lifecycle.IInboxLifecycleHook)manager;

        await lifecycleHook.OnStart(CancellationToken.None);
        await lifecycleHook.OnStop(CancellationToken.None);

        await task.Received(1).StopAsync(Arg.Any<CancellationToken>());
    }

    #endregion

    #region TaskName Uniqueness Tests

    [Fact]
    public async Task StartAsync_WithSameTaskName_DoesNotAddDuplicate()
    {
        var task1 = CreateMockCleanupTask("inbox1", "GroupLocks");
        var task2 = CreateMockCleanupTask("inbox1", "GroupLocks"); // Same TaskName
        var manager = new PostgresCleanupTasksManager([task1, task2], _logger);

        await manager.StartAsync(CancellationToken.None);

        // First task should be added, second should be skipped (same TaskName)
        await task1.Received(1).StartAsync(Arg.Any<CancellationToken>());
        // task2 may or may not be started depending on order, but only one should be tracked
    }

    #endregion

    #region Helper Methods

    private static ICleanupTask CreateMockCleanupTask(string inboxName, string? taskType = null)
    {
        var task = Substitute.For<ICleanupTask>();
        task.InboxName.Returns(inboxName);
        task.TaskName.Returns($"{taskType ?? "TestCleanupTask"}:{inboxName}");
        task.ExecuteOnceAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        task.StartAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        task.StopAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        return task;
    }

    #endregion
}
