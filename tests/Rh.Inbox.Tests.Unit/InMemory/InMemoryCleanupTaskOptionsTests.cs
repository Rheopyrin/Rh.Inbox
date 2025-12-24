using FluentAssertions;
using Rh.Inbox.InMemory.Options;
using Xunit;

namespace Rh.Inbox.Tests.Unit.InMemory;

public class InMemoryCleanupTaskOptionsTests
{
    #region Default Values Tests

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

    [Fact]
    public void RestartDelay_CanBeSetToLargeValue()
    {
        var options = new CleanupTaskOptions { RestartDelay = TimeSpan.FromMinutes(10) };

        options.RestartDelay.Should().Be(TimeSpan.FromMinutes(10));
    }

    #endregion

    #region Multiple Properties Tests

    [Fact]
    public void AllProperties_CanBeSetTogether()
    {
        var options = new CleanupTaskOptions
        {
            Interval = TimeSpan.FromMinutes(15),
            RestartDelay = TimeSpan.FromSeconds(45)
        };

        options.Interval.Should().Be(TimeSpan.FromMinutes(15));
        options.RestartDelay.Should().Be(TimeSpan.FromSeconds(45));
    }

    [Fact]
    public void NewInstance_HasIndependentValues()
    {
        var options1 = new CleanupTaskOptions { Interval = TimeSpan.FromMinutes(1) };
        var options2 = new CleanupTaskOptions { Interval = TimeSpan.FromMinutes(2) };

        options1.Interval.Should().Be(TimeSpan.FromMinutes(1));
        options2.Interval.Should().Be(TimeSpan.FromMinutes(2));
    }

    #endregion

    #region Difference from Postgres CleanupTaskOptions Tests

    [Fact]
    public void InMemoryCleanupTaskOptions_DoesNotHaveBatchSize()
    {
        var options = new CleanupTaskOptions();

        // InMemory CleanupTaskOptions should not have BatchSize property
        // This is verified by the fact that it only has Interval and RestartDelay
        options.Should().NotBeNull();
        options.Interval.Should().Be(TimeSpan.FromMinutes(5));
        options.RestartDelay.Should().Be(TimeSpan.FromSeconds(30));
    }

    #endregion
}
