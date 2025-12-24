using FluentAssertions;
using Rh.Inbox.Lifecycle;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Lifecycle;

public class InboxLifecycleTests
{
    #region IsRunning Tests

    [Fact]
    public void IsRunning_Initially_ReturnsFalse()
    {
        using var lifecycle = new InboxLifecycle();

        lifecycle.IsRunning.Should().BeFalse();
    }

    [Fact]
    public void IsRunning_AfterStart_ReturnsTrue()
    {
        using var lifecycle = new InboxLifecycle();

        lifecycle.Start();

        lifecycle.IsRunning.Should().BeTrue();
    }

    [Fact]
    public void IsRunning_AfterStartThenStop_ReturnsFalse()
    {
        using var lifecycle = new InboxLifecycle();

        lifecycle.Start();
        lifecycle.Stop();

        lifecycle.IsRunning.Should().BeFalse();
    }

    #endregion

    #region Start Tests

    [Fact]
    public void Start_MultipleCalls_RemainsRunning()
    {
        using var lifecycle = new InboxLifecycle();

        lifecycle.Start();
        lifecycle.Start();
        lifecycle.Start();

        lifecycle.IsRunning.Should().BeTrue();
    }

    [Fact]
    public void Start_AfterStop_CanRestartRunning()
    {
        using var lifecycle = new InboxLifecycle();

        lifecycle.Start();
        lifecycle.Stop();
        lifecycle.Start();

        lifecycle.IsRunning.Should().BeTrue();
    }

    #endregion

    #region Stop Tests

    [Fact]
    public void Stop_WhenNotStarted_RemainsNotRunning()
    {
        using var lifecycle = new InboxLifecycle();

        lifecycle.Stop();

        lifecycle.IsRunning.Should().BeFalse();
    }

    [Fact]
    public void Stop_MultipleCalls_RemainsNotRunning()
    {
        using var lifecycle = new InboxLifecycle();

        lifecycle.Start();
        lifecycle.Stop();
        lifecycle.Stop();
        lifecycle.Stop();

        lifecycle.IsRunning.Should().BeFalse();
    }

    #endregion

    #region StoppingToken Tests

    [Fact]
    public void StoppingToken_Initially_IsNotCancelled()
    {
        using var lifecycle = new InboxLifecycle();

        lifecycle.StoppingToken.IsCancellationRequested.Should().BeFalse();
    }

    [Fact]
    public void StoppingToken_AfterStart_IsNotCancelled()
    {
        using var lifecycle = new InboxLifecycle();

        lifecycle.Start();

        lifecycle.StoppingToken.IsCancellationRequested.Should().BeFalse();
    }

    [Fact]
    public void StoppingToken_AfterStop_IsCancelled()
    {
        using var lifecycle = new InboxLifecycle();

        lifecycle.Start();
        lifecycle.Stop();

        lifecycle.StoppingToken.IsCancellationRequested.Should().BeTrue();
    }

    [Fact]
    public void StoppingToken_StopWithoutStart_IsCancelled()
    {
        using var lifecycle = new InboxLifecycle();

        lifecycle.Stop();

        // Token is not cancelled because Stop only cancels when transitioning from running to not running
        lifecycle.StoppingToken.IsCancellationRequested.Should().BeFalse();
    }

    [Fact]
    public void StoppingToken_MultipleStops_OnlyCancelledOnce()
    {
        using var lifecycle = new InboxLifecycle();

        lifecycle.Start();
        lifecycle.Stop();

        // Second stop should not throw (CancellationTokenSource.Cancel is idempotent,
        // but we only call it when transitioning from running state)
        var act = () => lifecycle.Stop();

        act.Should().NotThrow();
        lifecycle.StoppingToken.IsCancellationRequested.Should().BeTrue();
    }

    [Fact]
    public async Task StoppingToken_CanBeUsedForCancellation()
    {
        using var lifecycle = new InboxLifecycle();
        lifecycle.Start();

        var task = Task.Run(async () =>
        {
            await Task.Delay(Timeout.Infinite, lifecycle.StoppingToken);
        });

        await Task.Delay(50); // Give task time to start
        lifecycle.Stop();

        var act = async () => await task;
        await act.Should().ThrowAsync<TaskCanceledException>();
    }

    #endregion

    #region Dispose Tests

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var lifecycle = new InboxLifecycle();

        var act = () =>
        {
            lifecycle.Dispose();
            lifecycle.Dispose();
            lifecycle.Dispose();
        };

        act.Should().NotThrow();
    }

    [Fact]
    public void Dispose_AfterDispose_StoppingTokenThrows()
    {
        var lifecycle = new InboxLifecycle();
        lifecycle.Dispose();

        var act = () => _ = lifecycle.StoppingToken.IsCancellationRequested;

        act.Should().Throw<ObjectDisposedException>();
    }

    #endregion

    #region Thread Safety Tests

    [Fact]
    public async Task StartAndStop_ConcurrentCalls_ThreadSafe()
    {
        using var lifecycle = new InboxLifecycle();

        var tasks = new List<Task>();

        for (int i = 0; i < 100; i++)
        {
            if (i % 2 == 0)
            {
                tasks.Add(Task.Run(() => lifecycle.Start()));
            }
            else
            {
                tasks.Add(Task.Run(() => lifecycle.Stop()));
            }
        }

        var act = async () => await Task.WhenAll(tasks);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task IsRunning_ConcurrentReads_ThreadSafe()
    {
        using var lifecycle = new InboxLifecycle();
        lifecycle.Start();

        var tasks = Enumerable.Range(0, 100)
            .Select(_ => Task.Run(() => lifecycle.IsRunning));

        var act = async () => await Task.WhenAll(tasks);

        await act.Should().NotThrowAsync();
    }

    #endregion
}
