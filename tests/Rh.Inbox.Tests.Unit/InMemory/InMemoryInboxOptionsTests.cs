using FluentAssertions;
using Rh.Inbox.InMemory;
using Rh.Inbox.InMemory.Options;
using Xunit;

namespace Rh.Inbox.Tests.Unit.InMemory;

public class InMemoryInboxOptionsTests
{
    #region Cleanup Options Initialization Tests

    [Fact]
    public void DeadLetterCleanup_IsInitializedByDefault()
    {
        var options = new InMemoryInboxOptions();

        options.DeadLetterCleanup.Should().NotBeNull();
    }

    [Fact]
    public void DeduplicationCleanup_IsInitializedByDefault()
    {
        var options = new InMemoryInboxOptions();

        options.DeduplicationCleanup.Should().NotBeNull();
    }

    [Fact]
    public void DeadLetterCleanup_HasDefaultValues()
    {
        var options = new InMemoryInboxOptions();

        options.DeadLetterCleanup.Interval.Should().Be(TimeSpan.FromMinutes(5));
        options.DeadLetterCleanup.RestartDelay.Should().Be(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void DeduplicationCleanup_HasDefaultValues()
    {
        var options = new InMemoryInboxOptions();

        options.DeduplicationCleanup.Interval.Should().Be(TimeSpan.FromMinutes(5));
        options.DeduplicationCleanup.RestartDelay.Should().Be(TimeSpan.FromSeconds(30));
    }

    #endregion

    #region Cleanup Options Configuration Tests

    [Fact]
    public void DeadLetterCleanup_CanBeConfigured()
    {
        var options = new InMemoryInboxOptions();
        options.DeadLetterCleanup.Interval = TimeSpan.FromMinutes(10);
        options.DeadLetterCleanup.RestartDelay = TimeSpan.FromMinutes(1);

        options.DeadLetterCleanup.Interval.Should().Be(TimeSpan.FromMinutes(10));
        options.DeadLetterCleanup.RestartDelay.Should().Be(TimeSpan.FromMinutes(1));
    }

    [Fact]
    public void DeduplicationCleanup_CanBeConfigured()
    {
        var options = new InMemoryInboxOptions();
        options.DeduplicationCleanup.Interval = TimeSpan.FromMinutes(15);
        options.DeduplicationCleanup.RestartDelay = TimeSpan.FromSeconds(45);

        options.DeduplicationCleanup.Interval.Should().Be(TimeSpan.FromMinutes(15));
        options.DeduplicationCleanup.RestartDelay.Should().Be(TimeSpan.FromSeconds(45));
    }

    [Fact]
    public void CleanupOptions_AreIndependent()
    {
        var options = new InMemoryInboxOptions();
        options.DeadLetterCleanup.Interval = TimeSpan.FromMinutes(1);
        options.DeduplicationCleanup.Interval = TimeSpan.FromMinutes(2);

        options.DeadLetterCleanup.Interval.Should().Be(TimeSpan.FromMinutes(1));
        options.DeduplicationCleanup.Interval.Should().Be(TimeSpan.FromMinutes(2));
    }

    [Fact]
    public void CleanupOptions_CanBeReplacedWithNewInstance()
    {
        var options = new InMemoryInboxOptions();
        options.DeadLetterCleanup = new CleanupTaskOptions
        {
            Interval = TimeSpan.FromHours(1),
            RestartDelay = TimeSpan.FromMinutes(5)
        };

        options.DeadLetterCleanup.Interval.Should().Be(TimeSpan.FromHours(1));
        options.DeadLetterCleanup.RestartDelay.Should().Be(TimeSpan.FromMinutes(5));
    }

    #endregion

    #region Multiple Instances Tests

    [Fact]
    public void NewInstances_HaveIndependentOptions()
    {
        var options1 = new InMemoryInboxOptions();
        var options2 = new InMemoryInboxOptions();

        options1.DeadLetterCleanup.Interval = TimeSpan.FromMinutes(1);
        options2.DeadLetterCleanup.Interval = TimeSpan.FromMinutes(2);

        options1.DeadLetterCleanup.Interval.Should().Be(TimeSpan.FromMinutes(1));
        options2.DeadLetterCleanup.Interval.Should().Be(TimeSpan.FromMinutes(2));
    }

    #endregion
}
