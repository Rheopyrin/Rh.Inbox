using FluentAssertions;
using Rh.Inbox.Configuration;
using Rh.Inbox.Tests.Unit.TestHelpers;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Configuration;

public class InboxOptionsTests
{
    #region Default Values Tests

    [Fact]
    public void DefaultInboxName_IsDefault()
    {
        InboxOptions.DefaultInboxName.Should().Be("default");
    }

    [Fact]
    public void DefaultValues_AreCorrect()
    {
        var options = TestConfigurationFactory.CreateOptions();

        options.InboxName.Should().Be("test-inbox");
        options.ReadBatchSize.Should().Be(100);
        options.WriteBatchSize.Should().Be(100);
        options.MaxProcessingTime.Should().Be(TimeSpan.FromMinutes(5));
        options.PollingInterval.Should().Be(TimeSpan.FromSeconds(5));
        options.ReadDelay.Should().Be(TimeSpan.Zero);
        options.ShutdownTimeout.Should().Be(TimeSpan.FromSeconds(30));
        options.MaxAttempts.Should().Be(3);
        options.EnableDeadLetter.Should().BeTrue();
        options.MaxProcessingThreads.Should().Be(1);
        options.MaxWriteThreads.Should().Be(1);
    }

    #endregion

    #region Property Assignment Tests

    [Fact]
    public void InboxName_CanBeSet()
    {
        var options = TestConfigurationFactory.CreateOptions(inboxName: "custom-inbox");

        options.InboxName.Should().Be("custom-inbox");
    }

    [Fact]
    public void ReadBatchSize_CanBeSet()
    {
        var options = TestConfigurationFactory.CreateOptions(readBatchSize: 50);

        options.ReadBatchSize.Should().Be(50);
    }

    [Fact]
    public void WriteBatchSize_CanBeSet()
    {
        var options = TestConfigurationFactory.CreateOptions(writeBatchSize: 200);

        options.WriteBatchSize.Should().Be(200);
    }

    [Fact]
    public void MaxProcessingTime_CanBeSet()
    {
        var options = TestConfigurationFactory.CreateOptions(maxProcessingTime: TimeSpan.FromMinutes(10));

        options.MaxProcessingTime.Should().Be(TimeSpan.FromMinutes(10));
    }

    [Fact]
    public void PollingInterval_CanBeSet()
    {
        var options = TestConfigurationFactory.CreateOptions(pollingInterval: TimeSpan.FromSeconds(10));

        options.PollingInterval.Should().Be(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void ReadDelay_CanBeSet()
    {
        var options = TestConfigurationFactory.CreateOptions(readDelay: TimeSpan.FromMilliseconds(100));

        options.ReadDelay.Should().Be(TimeSpan.FromMilliseconds(100));
    }

    [Fact]
    public void ShutdownTimeout_CanBeSet()
    {
        var options = TestConfigurationFactory.CreateOptions(shutdownTimeout: TimeSpan.FromMinutes(1));

        options.ShutdownTimeout.Should().Be(TimeSpan.FromMinutes(1));
    }

    [Fact]
    public void MaxAttempts_CanBeSet()
    {
        var options = TestConfigurationFactory.CreateOptions(maxAttempts: 5);

        options.MaxAttempts.Should().Be(5);
    }

    [Fact]
    public void EnableDeadLetter_CanBeSet()
    {
        var options = TestConfigurationFactory.CreateOptions(enableDeadLetter: false);

        options.EnableDeadLetter.Should().BeFalse();
    }

    [Fact]
    public void MaxProcessingThreads_CanBeSet()
    {
        var options = TestConfigurationFactory.CreateOptions(maxProcessingThreads: 4);

        options.MaxProcessingThreads.Should().Be(4);
    }

    [Fact]
    public void MaxWriteThreads_CanBeSet()
    {
        var options = TestConfigurationFactory.CreateOptions(maxWriteThreads: 4);

        options.MaxWriteThreads.Should().Be(4);
    }

    [Fact]
    public void EnableLockExtension_DefaultIsFalse()
    {
        var options = TestConfigurationFactory.CreateOptions();

        options.EnableLockExtension.Should().BeFalse();
    }

    [Fact]
    public void EnableLockExtension_CanBeSet()
    {
        var options = TestConfigurationFactory.CreateOptions(enableLockExtension: true);

        options.EnableLockExtension.Should().BeTrue();
    }

    [Fact]
    public void LockExtensionThreshold_DefaultIs50Percent()
    {
        var options = TestConfigurationFactory.CreateOptions();

        options.LockExtensionThreshold.Should().Be(0.5);
    }

    [Fact]
    public void LockExtensionThreshold_CanBeSet()
    {
        var options = TestConfigurationFactory.CreateOptions(lockExtensionThreshold: 0.7);

        options.LockExtensionThreshold.Should().Be(0.7);
    }

    [Fact]
    public void DeadLetterMaxMessageLifetime_DefaultIsZero()
    {
        var options = TestConfigurationFactory.CreateOptions();

        options.DeadLetterMaxMessageLifetime.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void DeadLetterMaxMessageLifetime_CanBeSet()
    {
        var options = TestConfigurationFactory.CreateOptions(deadLetterMaxMessageLifetime: TimeSpan.FromDays(7));

        options.DeadLetterMaxMessageLifetime.Should().Be(TimeSpan.FromDays(7));
    }

    [Fact]
    public void DeduplicationInterval_DefaultIsZero()
    {
        var options = TestConfigurationFactory.CreateOptions();

        options.DeduplicationInterval.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void DeduplicationInterval_CanBeSet()
    {
        var options = TestConfigurationFactory.CreateOptions(deduplicationInterval: TimeSpan.FromHours(1));

        options.DeduplicationInterval.Should().Be(TimeSpan.FromHours(1));
    }

    [Fact]
    public void EnableDeduplication_DefaultIsFalse()
    {
        var options = TestConfigurationFactory.CreateOptions();

        options.EnableDeduplication.Should().BeFalse();
    }

    [Fact]
    public void EnableDeduplication_CanBeSet()
    {
        var options = TestConfigurationFactory.CreateOptions(enableDeduplication: true);

        options.EnableDeduplication.Should().BeTrue();
    }

    #endregion

    #region Multiple Properties Tests

    [Fact]
    public void AllProperties_CanBeSetTogether()
    {
        var options = TestConfigurationFactory.CreateOptions(
            inboxName: "my-inbox",
            readBatchSize: 25,
            writeBatchSize: 50,
            maxProcessingTime: TimeSpan.FromMinutes(2),
            pollingInterval: TimeSpan.FromSeconds(1),
            readDelay: TimeSpan.FromMilliseconds(50),
            shutdownTimeout: TimeSpan.FromSeconds(15),
            maxAttempts: 10,
            enableDeadLetter: false,
            maxProcessingThreads: 8,
            maxWriteThreads: 4);

        options.InboxName.Should().Be("my-inbox");
        options.ReadBatchSize.Should().Be(25);
        options.WriteBatchSize.Should().Be(50);
        options.MaxProcessingTime.Should().Be(TimeSpan.FromMinutes(2));
        options.PollingInterval.Should().Be(TimeSpan.FromSeconds(1));
        options.ReadDelay.Should().Be(TimeSpan.FromMilliseconds(50));
        options.ShutdownTimeout.Should().Be(TimeSpan.FromSeconds(15));
        options.MaxAttempts.Should().Be(10);
        options.EnableDeadLetter.Should().BeFalse();
        options.MaxProcessingThreads.Should().Be(8);
        options.MaxWriteThreads.Should().Be(4);
    }

    #endregion
}