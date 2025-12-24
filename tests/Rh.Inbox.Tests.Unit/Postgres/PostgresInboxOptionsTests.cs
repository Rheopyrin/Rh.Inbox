using FluentAssertions;
using Rh.Inbox.Postgres.Options;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Postgres;

public class PostgresInboxOptionsTests
{
    #region Default Table Prefix Constants Tests

    [Fact]
    public void DefaultTablePrefix_IsCorrect()
    {
        PostgresInboxOptions.DefaultTablePrefix.Should().Be("inbox_messages");
    }

    [Fact]
    public void DefaultDeadLetterTablePrefix_IsCorrect()
    {
        PostgresInboxOptions.DefaultDeadLetterTablePrefix.Should().Be("inbox_dead_letters");
    }

    [Fact]
    public void DefaultDeduplicationTablePrefix_IsCorrect()
    {
        PostgresInboxOptions.DefaultDeduplicationTablePrefix.Should().Be("inbox_dedup");
    }

    [Fact]
    public void DefaultGroupLocksTablePrefix_IsCorrect()
    {
        PostgresInboxOptions.DefaultGroupLocksTablePrefix.Should().Be("inbox_group_locks");
    }

    #endregion

    #region AutostartCleanupTasks Tests

    [Fact]
    public void AutostartCleanupTasks_DefaultsToTrue()
    {
        var options = new PostgresInboxOptions { ConnectionString = "Host=localhost" };

        options.AutostartCleanupTasks.Should().BeTrue();
    }

    [Fact]
    public void AutostartCleanupTasks_CanBeSetToFalse()
    {
        var options = new PostgresInboxOptions
        {
            ConnectionString = "Host=localhost",
            AutostartCleanupTasks = false
        };

        options.AutostartCleanupTasks.Should().BeFalse();
    }

    #endregion

    #region Cleanup Options Initialization Tests

    [Fact]
    public void DeadLetterCleanup_IsInitializedByDefault()
    {
        var options = new PostgresInboxOptions { ConnectionString = "Host=localhost" };

        options.DeadLetterCleanup.Should().NotBeNull();
    }

    [Fact]
    public void DeduplicationCleanup_IsInitializedByDefault()
    {
        var options = new PostgresInboxOptions { ConnectionString = "Host=localhost" };

        options.DeduplicationCleanup.Should().NotBeNull();
    }

    [Fact]
    public void GroupLocksCleanup_IsInitializedByDefault()
    {
        var options = new PostgresInboxOptions { ConnectionString = "Host=localhost" };

        options.GroupLocksCleanup.Should().NotBeNull();
    }

    [Fact]
    public void DeadLetterCleanup_HasDefaultValues()
    {
        var options = new PostgresInboxOptions { ConnectionString = "Host=localhost" };

        options.DeadLetterCleanup.BatchSize.Should().Be(1000);
        options.DeadLetterCleanup.Interval.Should().Be(TimeSpan.FromMinutes(5));
        options.DeadLetterCleanup.RestartDelay.Should().Be(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void DeduplicationCleanup_HasDefaultValues()
    {
        var options = new PostgresInboxOptions { ConnectionString = "Host=localhost" };

        options.DeduplicationCleanup.BatchSize.Should().Be(1000);
        options.DeduplicationCleanup.Interval.Should().Be(TimeSpan.FromMinutes(5));
        options.DeduplicationCleanup.RestartDelay.Should().Be(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void GroupLocksCleanup_HasDefaultValues()
    {
        var options = new PostgresInboxOptions { ConnectionString = "Host=localhost" };

        options.GroupLocksCleanup.BatchSize.Should().Be(1000);
        options.GroupLocksCleanup.Interval.Should().Be(TimeSpan.FromMinutes(5));
        options.GroupLocksCleanup.RestartDelay.Should().Be(TimeSpan.FromSeconds(30));
    }

    #endregion

    #region Cleanup Options Configuration Tests

    [Fact]
    public void DeadLetterCleanup_CanBeConfigured()
    {
        var options = new PostgresInboxOptions { ConnectionString = "Host=localhost" };
        options.DeadLetterCleanup.BatchSize = 500;
        options.DeadLetterCleanup.Interval = TimeSpan.FromMinutes(10);

        options.DeadLetterCleanup.BatchSize.Should().Be(500);
        options.DeadLetterCleanup.Interval.Should().Be(TimeSpan.FromMinutes(10));
    }

    [Fact]
    public void DeduplicationCleanup_CanBeConfigured()
    {
        var options = new PostgresInboxOptions { ConnectionString = "Host=localhost" };
        options.DeduplicationCleanup.BatchSize = 2000;
        options.DeduplicationCleanup.Interval = TimeSpan.FromMinutes(15);

        options.DeduplicationCleanup.BatchSize.Should().Be(2000);
        options.DeduplicationCleanup.Interval.Should().Be(TimeSpan.FromMinutes(15));
    }

    [Fact]
    public void GroupLocksCleanup_CanBeConfigured()
    {
        var options = new PostgresInboxOptions { ConnectionString = "Host=localhost" };
        options.GroupLocksCleanup.BatchSize = 100;
        options.GroupLocksCleanup.Interval = TimeSpan.FromMinutes(1);

        options.GroupLocksCleanup.BatchSize.Should().Be(100);
        options.GroupLocksCleanup.Interval.Should().Be(TimeSpan.FromMinutes(1));
    }

    [Fact]
    public void CleanupOptions_AreIndependent()
    {
        var options = new PostgresInboxOptions { ConnectionString = "Host=localhost" };
        options.DeadLetterCleanup.BatchSize = 100;
        options.DeduplicationCleanup.BatchSize = 200;
        options.GroupLocksCleanup.BatchSize = 300;

        options.DeadLetterCleanup.BatchSize.Should().Be(100);
        options.DeduplicationCleanup.BatchSize.Should().Be(200);
        options.GroupLocksCleanup.BatchSize.Should().Be(300);
    }

    [Fact]
    public void CleanupOptions_CanBeReplacedWithNewInstance()
    {
        var options = new PostgresInboxOptions { ConnectionString = "Host=localhost" };
        options.DeadLetterCleanup = new CleanupTaskOptions
        {
            BatchSize = 5000,
            Interval = TimeSpan.FromHours(1),
            RestartDelay = TimeSpan.FromMinutes(5)
        };

        options.DeadLetterCleanup.BatchSize.Should().Be(5000);
        options.DeadLetterCleanup.Interval.Should().Be(TimeSpan.FromHours(1));
        options.DeadLetterCleanup.RestartDelay.Should().Be(TimeSpan.FromMinutes(5));
    }

    #endregion

    #region Table Name Tests

    [Fact]
    public void TableName_DefaultsToNull()
    {
        var options = new PostgresInboxOptions { ConnectionString = "Host=localhost" };

        options.TableName.Should().BeNull();
    }

    [Fact]
    public void TableName_CanBeSet()
    {
        var options = new PostgresInboxOptions
        {
            ConnectionString = "Host=localhost",
            TableName = "custom_messages"
        };

        options.TableName.Should().Be("custom_messages");
    }

    [Fact]
    public void DeadLetterTableName_DefaultsToNull()
    {
        var options = new PostgresInboxOptions { ConnectionString = "Host=localhost" };

        options.DeadLetterTableName.Should().BeNull();
    }

    [Fact]
    public void DeadLetterTableName_CanBeSet()
    {
        var options = new PostgresInboxOptions
        {
            ConnectionString = "Host=localhost",
            DeadLetterTableName = "custom_dead_letters"
        };

        options.DeadLetterTableName.Should().Be("custom_dead_letters");
    }

    [Fact]
    public void DeduplicationTableName_DefaultsToNull()
    {
        var options = new PostgresInboxOptions { ConnectionString = "Host=localhost" };

        options.DeduplicationTableName.Should().BeNull();
    }

    [Fact]
    public void DeduplicationTableName_CanBeSet()
    {
        var options = new PostgresInboxOptions
        {
            ConnectionString = "Host=localhost",
            DeduplicationTableName = "custom_dedup"
        };

        options.DeduplicationTableName.Should().Be("custom_dedup");
    }

    #endregion

    #region ConnectionString Tests

    [Fact]
    public void ConnectionString_IsRequired()
    {
        var options = new PostgresInboxOptions { ConnectionString = "Host=localhost;Database=test" };

        options.ConnectionString.Should().Be("Host=localhost;Database=test");
    }

    [Fact]
    public void ConnectionString_CanContainFullConnectionDetails()
    {
        var connectionString = "Host=localhost;Port=5432;Database=mydb;Username=user;Password=pass";
        var options = new PostgresInboxOptions { ConnectionString = connectionString };

        options.ConnectionString.Should().Be(connectionString);
    }

    #endregion
}
