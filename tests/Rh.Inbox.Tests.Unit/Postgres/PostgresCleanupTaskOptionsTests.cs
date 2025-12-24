using FluentAssertions;
using Rh.Inbox.Postgres.Options;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Postgres;

public class PostgresCleanupTaskOptionsTests
{
    #region Default Values Tests

    [Fact]
    public void BatchSize_DefaultsTo1000()
    {
        var options = new CleanupTaskOptions();

        options.BatchSize.Should().Be(1000);
    }

    [Fact]
    public void Interval_DefaultsTo5Minutes()
    {
        var options = new CleanupTaskOptions();

        options.Interval.Should().Be(TimeSpan.FromMinutes(5));
    }

    [Fact]
    public void RestartDelay_DefaultsTo30Seconds()
    {
        var options = new CleanupTaskOptions();

        options.RestartDelay.Should().Be(TimeSpan.FromSeconds(30));
    }

    #endregion

    #region Property Setting Tests

    [Fact]
    public void BatchSize_CanBeSet()
    {
        var options = new CleanupTaskOptions { BatchSize = 500 };

        options.BatchSize.Should().Be(500);
    }

    [Fact]
    public void BatchSize_CanBeSetToLargeValue()
    {
        var options = new CleanupTaskOptions { BatchSize = 10000 };

        options.BatchSize.Should().Be(10000);
    }

    [Fact]
    public void Interval_CanBeSet()
    {
        var options = new CleanupTaskOptions { Interval = TimeSpan.FromMinutes(10) };

        options.Interval.Should().Be(TimeSpan.FromMinutes(10));
    }

    [Fact]
    public void Interval_CanBeSetToSmallValue()
    {
        var options = new CleanupTaskOptions { Interval = TimeSpan.FromSeconds(30) };

        options.Interval.Should().Be(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void Interval_CanBeSetToLargeValue()
    {
        var options = new CleanupTaskOptions { Interval = TimeSpan.FromHours(1) };

        options.Interval.Should().Be(TimeSpan.FromHours(1));
    }

    [Fact]
    public void RestartDelay_CanBeSet()
    {
        var options = new CleanupTaskOptions { RestartDelay = TimeSpan.FromMinutes(1) };

        options.RestartDelay.Should().Be(TimeSpan.FromMinutes(1));
    }

    [Fact]
    public void RestartDelay_CanBeSetToZero()
    {
        var options = new CleanupTaskOptions { RestartDelay = TimeSpan.Zero };

        options.RestartDelay.Should().Be(TimeSpan.Zero);
    }

    #endregion

    #region Multiple Properties Tests

    [Fact]
    public void AllProperties_CanBeSetTogether()
    {
        var options = new CleanupTaskOptions
        {
            BatchSize = 2000,
            Interval = TimeSpan.FromMinutes(15),
            RestartDelay = TimeSpan.FromSeconds(45)
        };

        options.BatchSize.Should().Be(2000);
        options.Interval.Should().Be(TimeSpan.FromMinutes(15));
        options.RestartDelay.Should().Be(TimeSpan.FromSeconds(45));
    }

    [Fact]
    public void NewInstance_HasIndependentValues()
    {
        var options1 = new CleanupTaskOptions { BatchSize = 100 };
        var options2 = new CleanupTaskOptions { BatchSize = 200 };

        options1.BatchSize.Should().Be(100);
        options2.BatchSize.Should().Be(200);
    }

    #endregion
}
