using System.Diagnostics;

namespace Rh.Inbox.Tests.Integration.Common;

public static class TestWaitHelper
{
    public static async Task<TimeSpan> WaitForCountAsync(
        Func<int> getCount,
        int expectedCount,
        TimeSpan? timeout = null,
        int pollIntervalMs = 50)
    {
        timeout ??= TestConstants.DefaultProcessingTimeout;
        var sw = Stopwatch.StartNew();

        while (getCount() < expectedCount && sw.Elapsed < timeout)
        {
            await Task.Delay(pollIntervalMs);
        }

        sw.Stop();
        return sw.Elapsed;
    }

    public static async Task<TimeSpan> WaitForAllAsync(
        IReadOnlyList<Func<int>> getCounts,
        int expectedCountEach,
        TimeSpan? timeout = null,
        int pollIntervalMs = 50)
    {
        timeout ??= TestConstants.LongProcessingTimeout;
        var sw = Stopwatch.StartNew();

        while (!getCounts.All(getCount => getCount() >= expectedCountEach) && sw.Elapsed < timeout)
        {
            await Task.Delay(pollIntervalMs);
        }

        sw.Stop();
        return sw.Elapsed;
    }

    public static async Task<TimeSpan> WaitForConditionAsync(
        Func<bool> condition,
        TimeSpan? timeout = null,
        int pollIntervalMs = 50)
    {
        timeout ??= TestConstants.DefaultProcessingTimeout;
        var sw = Stopwatch.StartNew();

        while (!condition() && sw.Elapsed < timeout)
        {
            await Task.Delay(pollIntervalMs);
        }

        sw.Stop();
        return sw.Elapsed;
    }
}
