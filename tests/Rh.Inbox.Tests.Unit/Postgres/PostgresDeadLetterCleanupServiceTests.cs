using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Postgres.Options;
using Rh.Inbox.Postgres.Services;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Postgres;

public class PostgresDeadLetterCleanupServiceTests
{
    private readonly IInboxConfiguration _configuration;
    private readonly IProviderOptionsAccessor _optionsAccessor;
    private readonly PostgresInboxProviderOptions _postgresOptions;
    private readonly CleanupTaskOptions _cleanupOptions;

    public PostgresDeadLetterCleanupServiceTests()
    {
        _configuration = CreateMockConfiguration("test-inbox");
        _postgresOptions = new PostgresInboxProviderOptions
        {
            DataSource = null!, // Not needed for these tests
            TableName = "inbox_messages",
            DeadLetterTableName = "inbox_dead_letters",
            DeduplicationTableName = "inbox_deduplication",
            GroupLocksTableName = "inbox_group_locks"
        };
        _cleanupOptions = new CleanupTaskOptions();
        _optionsAccessor = Substitute.For<IProviderOptionsAccessor>();
        _optionsAccessor.GetForInbox("test-inbox").Returns(_postgresOptions);
    }

    #region TaskName Tests

    [Fact]
    public void TaskName_ReturnsCorrectFormat()
    {
        var service = CreateService();

        service.TaskName.Should().Be("DeadLetterCleanupService:test-inbox");
    }

    [Fact]
    public void TaskName_IncludesInboxName()
    {
        var config = CreateMockConfiguration("my-custom-inbox");
        _optionsAccessor.GetForInbox("my-custom-inbox").Returns(_postgresOptions);
        var service = new DeadLetterCleanupService(
            config,
            _cleanupOptions,
            _optionsAccessor,
            NullLogger<DeadLetterCleanupService>.Instance);

        service.TaskName.Should().Be("DeadLetterCleanupService:my-custom-inbox");
    }

    #endregion

    #region InboxName Tests

    [Fact]
    public void InboxName_ReturnsConfiguredInboxName()
    {
        var service = CreateService();

        service.InboxName.Should().Be("test-inbox");
    }

    #endregion

    #region StartAsync Tests

    [Fact]
    public async Task StartAsync_CompletesImmediately()
    {
        var service = CreateService();

        var task = service.StartAsync(CancellationToken.None);

        task.IsCompleted.Should().BeTrue();
        await task; // Should not throw
    }

    [Fact]
    public async Task StartAsync_WithCancellation_RespectsToken()
    {
        var service = CreateService();
        using var cts = new CancellationTokenSource();

        var task = service.StartAsync(cts.Token);

        await task;
        task.IsCompleted.Should().BeTrue();
    }

    #endregion

    #region StopAsync Tests

    [Fact]
    public async Task StopAsync_WithoutStart_CompletesSuccessfully()
    {
        var service = CreateService();

        var act = async () => await service.StopAsync(CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task StopAsync_AfterStart_CompletesSuccessfully()
    {
        var service = CreateService();
        await service.StartAsync(CancellationToken.None);

        var act = async () => await service.StopAsync(CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    #endregion

    #region ICleanupTask Interface Tests

    [Fact]
    public void Service_ImplementsICleanupTask()
    {
        var service = CreateService();

        service.Should().BeAssignableTo<ICleanupTask>();
    }

    #endregion

    #region Helper Methods

    private DeadLetterCleanupService CreateService()
    {
        return new DeadLetterCleanupService(
            _configuration,
            _cleanupOptions,
            _optionsAccessor,
            NullLogger<DeadLetterCleanupService>.Instance);
    }

    private static IInboxConfiguration CreateMockConfiguration(string inboxName)
    {
        var options = Substitute.For<IInboxOptions>();
        options.DeadLetterMaxMessageLifetime.Returns(TimeSpan.FromDays(7));

        var dateTimeProvider = Substitute.For<IDateTimeProvider>();
        dateTimeProvider.GetUtcNow().Returns(DateTime.UtcNow);

        var configuration = Substitute.For<IInboxConfiguration>();
        configuration.InboxName.Returns(inboxName);
        configuration.Options.Returns(options);
        configuration.DateTimeProvider.Returns(dateTimeProvider);

        return configuration;
    }

    #endregion
}
