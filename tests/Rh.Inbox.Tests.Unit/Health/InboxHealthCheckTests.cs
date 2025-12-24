using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Health;
using Rh.Inbox.Abstractions.Lifecycle;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Configuration.Registry;
using Rh.Inbox.Health;
using Rh.Inbox.Inboxes;
using Rh.Inbox.Inboxes.Factory;
using Rh.Inbox.Management;
using Rh.Inbox.Processing.Strategies.Factory;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Health;

public class InboxHealthCheckTests
{
    private readonly IDateTimeProvider _dateTimeProvider;
    private readonly InboxHealthCheckOptions _options;
    private readonly string _inboxName = "test-inbox";

    public InboxHealthCheckTests()
    {
        _dateTimeProvider = Substitute.For<IDateTimeProvider>();
        _dateTimeProvider.GetUtcNow().Returns(DateTime.UtcNow);

        _options = new InboxHealthCheckOptions();
    }

    #region InboxHealthCheckOptions Tests

    [Fact]
    public void Options_DefaultValues_AreCorrect()
    {
        var options = new InboxHealthCheckOptions();

        options.Enabled.Should().BeFalse();
        options.Tags.Should().Contain("inbox");
        options.QueueDepthWarningThreshold.Should().Be(1000);
        options.QueueDepthCriticalThreshold.Should().Be(10000);
        options.LagWarningThreshold.Should().Be(TimeSpan.FromMinutes(5));
        options.LagCriticalThreshold.Should().Be(TimeSpan.FromMinutes(30));
        options.DeadLetterWarningThreshold.Should().Be(100);
        options.DeadLetterCriticalThreshold.Should().Be(1000);
    }

    [Fact]
    public void Options_CanBeModified()
    {
        var options = new InboxHealthCheckOptions
        {
            Enabled = true,
            Tags = ["custom", "tag"],
            QueueDepthWarningThreshold = 500,
            QueueDepthCriticalThreshold = 5000,
            LagWarningThreshold = TimeSpan.FromMinutes(10),
            LagCriticalThreshold = TimeSpan.FromHours(1),
            DeadLetterWarningThreshold = 50,
            DeadLetterCriticalThreshold = 500
        };

        options.Enabled.Should().BeTrue();
        options.Tags.Should().BeEquivalentTo(new[] { "custom", "tag" });
        options.QueueDepthWarningThreshold.Should().Be(500);
        options.QueueDepthCriticalThreshold.Should().Be(5000);
        options.LagWarningThreshold.Should().Be(TimeSpan.FromMinutes(10));
        options.LagCriticalThreshold.Should().Be(TimeSpan.FromHours(1));
        options.DeadLetterWarningThreshold.Should().Be(50);
        options.DeadLetterCriticalThreshold.Should().Be(500);
    }

    #endregion

    #region InboxHealthMetrics Tests

    [Fact]
    public void InboxHealthMetrics_ValidInstantiation_StoresValues()
    {
        var oldestPending = DateTime.UtcNow.AddMinutes(-5);
        var metrics = new InboxHealthMetrics(
            PendingCount: 100,
            CapturedCount: 10,
            DeadLetterCount: 5,
            OldestPendingMessageAt: oldestPending);

        metrics.PendingCount.Should().Be(100);
        metrics.CapturedCount.Should().Be(10);
        metrics.DeadLetterCount.Should().Be(5);
        metrics.OldestPendingMessageAt.Should().Be(oldestPending);
    }

    [Fact]
    public void InboxHealthMetrics_NullOldestPending_Allowed()
    {
        var metrics = new InboxHealthMetrics(
            PendingCount: 50,
            CapturedCount: 5,
            DeadLetterCount: 0,
            OldestPendingMessageAt: null);

        metrics.OldestPendingMessageAt.Should().BeNull();
    }

    [Fact]
    public void InboxHealthMetrics_EqualRecords_AreEqual()
    {
        var timestamp = DateTime.UtcNow;
        var metrics1 = new InboxHealthMetrics(10, 5, 2, timestamp);
        var metrics2 = new InboxHealthMetrics(10, 5, 2, timestamp);

        metrics1.Should().Be(metrics2);
    }

    #endregion

    #region CheckHealthAsync - Inbox Not Running Tests

    [Fact]
    public async Task CheckHealthAsync_InboxNotRunning_ReturnsDegraded()
    {
        var inboxManager = CreateMockInboxManager(isRunning: false);
        var healthCheck = new InboxHealthCheck(inboxManager, _inboxName, _options, _dateTimeProvider);

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Degraded);
        result.Description.Should().Contain("not started");
    }

    #endregion

    #region CheckHealthAsync - Provider Without Health Support Tests

    [Fact]
    public async Task CheckHealthAsync_ProviderNoHealthSupport_ReturnsHealthy()
    {
        var storageProvider = Substitute.For<IInboxStorageProvider>();
        var inboxManager = CreateMockInboxManagerWithProvider(storageProvider);
        var healthCheck = new InboxHealthCheck(inboxManager, _inboxName, _options, _dateTimeProvider);

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Healthy);
        result.Description.Should().Contain("does not support health checks");
    }

    #endregion

    #region CheckHealthAsync - Queue Depth Tests

    [Fact]
    public async Task CheckHealthAsync_QueueDepthExceedsCritical_ReturnsUnhealthy()
    {
        var metrics = new InboxHealthMetrics(
            PendingCount: 9000,
            CapturedCount: 2000,
            DeadLetterCount: 0,
            OldestPendingMessageAt: null);

        var inboxManager = CreateMockInboxManagerWithMetrics(metrics);
        var healthCheck = new InboxHealthCheck(inboxManager, _inboxName, _options, _dateTimeProvider);

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Description.Should().Contain("Queue depth");
        result.Description.Should().Contain("critical threshold");
    }

    [Fact]
    public async Task CheckHealthAsync_QueueDepthExceedsWarning_ReturnsDegraded()
    {
        var metrics = new InboxHealthMetrics(
            PendingCount: 800,
            CapturedCount: 300,
            DeadLetterCount: 0,
            OldestPendingMessageAt: null);

        var inboxManager = CreateMockInboxManagerWithMetrics(metrics);
        var healthCheck = new InboxHealthCheck(inboxManager, _inboxName, _options, _dateTimeProvider);

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Degraded);
        result.Description.Should().Contain("Queue depth");
        result.Description.Should().Contain("warning threshold");
    }

    [Fact]
    public async Task CheckHealthAsync_QueueDepthBelowWarning_ReturnsHealthy()
    {
        var metrics = new InboxHealthMetrics(
            PendingCount: 500,
            CapturedCount: 100,
            DeadLetterCount: 0,
            OldestPendingMessageAt: null);

        var inboxManager = CreateMockInboxManagerWithMetrics(metrics);
        var healthCheck = new InboxHealthCheck(inboxManager, _inboxName, _options, _dateTimeProvider);

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Healthy);
    }

    #endregion

    #region CheckHealthAsync - Lag Tests

    [Fact]
    public async Task CheckHealthAsync_LagExceedsCritical_ReturnsUnhealthy()
    {
        var now = DateTime.UtcNow;
        _dateTimeProvider.GetUtcNow().Returns(now);

        var metrics = new InboxHealthMetrics(
            PendingCount: 10,
            CapturedCount: 0,
            DeadLetterCount: 0,
            OldestPendingMessageAt: now.AddMinutes(-45));

        var inboxManager = CreateMockInboxManagerWithMetrics(metrics);
        var healthCheck = new InboxHealthCheck(inboxManager, _inboxName, _options, _dateTimeProvider);

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Description.Should().Contain("Processing lag");
        result.Description.Should().Contain("critical threshold");
    }

    [Fact]
    public async Task CheckHealthAsync_LagExceedsWarning_ReturnsDegraded()
    {
        var now = DateTime.UtcNow;
        _dateTimeProvider.GetUtcNow().Returns(now);

        var metrics = new InboxHealthMetrics(
            PendingCount: 10,
            CapturedCount: 0,
            DeadLetterCount: 0,
            OldestPendingMessageAt: now.AddMinutes(-10));

        var inboxManager = CreateMockInboxManagerWithMetrics(metrics);
        var healthCheck = new InboxHealthCheck(inboxManager, _inboxName, _options, _dateTimeProvider);

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Degraded);
        result.Description.Should().Contain("Processing lag");
        result.Description.Should().Contain("warning threshold");
    }

    [Fact]
    public async Task CheckHealthAsync_NoOldestPending_NoLagCalculation_ReturnsHealthy()
    {
        var metrics = new InboxHealthMetrics(
            PendingCount: 0,
            CapturedCount: 0,
            DeadLetterCount: 0,
            OldestPendingMessageAt: null);

        var inboxManager = CreateMockInboxManagerWithMetrics(metrics);
        var healthCheck = new InboxHealthCheck(inboxManager, _inboxName, _options, _dateTimeProvider);

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Healthy);
        result.Data.Should().NotContainKey("lagSeconds");
    }

    #endregion

    #region CheckHealthAsync - Dead Letter Tests

    [Fact]
    public async Task CheckHealthAsync_DeadLetterExceedsCritical_ReturnsUnhealthy()
    {
        var metrics = new InboxHealthMetrics(
            PendingCount: 10,
            CapturedCount: 0,
            DeadLetterCount: 1500,
            OldestPendingMessageAt: null);

        var inboxManager = CreateMockInboxManagerWithMetrics(metrics);
        var healthCheck = new InboxHealthCheck(inboxManager, _inboxName, _options, _dateTimeProvider);

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Description.Should().Contain("Dead letter count");
        result.Description.Should().Contain("critical threshold");
    }

    [Fact]
    public async Task CheckHealthAsync_DeadLetterExceedsWarning_ReturnsDegraded()
    {
        var metrics = new InboxHealthMetrics(
            PendingCount: 10,
            CapturedCount: 0,
            DeadLetterCount: 150,
            OldestPendingMessageAt: null);

        var inboxManager = CreateMockInboxManagerWithMetrics(metrics);
        var healthCheck = new InboxHealthCheck(inboxManager, _inboxName, _options, _dateTimeProvider);

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Degraded);
        result.Description.Should().Contain("Dead letter count");
        result.Description.Should().Contain("warning threshold");
    }

    #endregion

    #region CheckHealthAsync - All Metrics Normal Tests

    [Fact]
    public async Task CheckHealthAsync_AllMetricsNormal_ReturnsHealthy()
    {
        var now = DateTime.UtcNow;
        _dateTimeProvider.GetUtcNow().Returns(now);

        var metrics = new InboxHealthMetrics(
            PendingCount: 50,
            CapturedCount: 10,
            DeadLetterCount: 5,
            OldestPendingMessageAt: now.AddMinutes(-1));

        var inboxManager = CreateMockInboxManagerWithMetrics(metrics);
        var healthCheck = new InboxHealthCheck(inboxManager, _inboxName, _options, _dateTimeProvider);

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Healthy);
        result.Description.Should().Contain("healthy");
    }

    [Fact]
    public async Task CheckHealthAsync_HealthyResult_IncludesDataDictionary()
    {
        var now = DateTime.UtcNow;
        var oldestPending = now.AddMinutes(-2);
        _dateTimeProvider.GetUtcNow().Returns(now);

        var metrics = new InboxHealthMetrics(
            PendingCount: 50,
            CapturedCount: 10,
            DeadLetterCount: 5,
            OldestPendingMessageAt: oldestPending);

        var inboxManager = CreateMockInboxManagerWithMetrics(metrics);
        var healthCheck = new InboxHealthCheck(inboxManager, _inboxName, _options, _dateTimeProvider);

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        result.Data.Should().ContainKey("inbox");
        result.Data.Should().ContainKey("pendingCount");
        result.Data.Should().ContainKey("capturedCount");
        result.Data.Should().ContainKey("deadLetterCount");
        result.Data.Should().ContainKey("lagSeconds");
        result.Data.Should().ContainKey("oldestPendingAt");

        result.Data["inbox"].Should().Be(_inboxName);
        result.Data["pendingCount"].Should().Be(50L);
        result.Data["capturedCount"].Should().Be(10L);
        result.Data["deadLetterCount"].Should().Be(5L);
    }

    #endregion

    #region CheckHealthAsync - Threshold Priority Tests

    [Fact]
    public async Task CheckHealthAsync_MultipleCriticalThresholdsExceeded_ReturnsFirstCritical()
    {
        var now = DateTime.UtcNow;
        _dateTimeProvider.GetUtcNow().Returns(now);

        // All critical thresholds exceeded
        var metrics = new InboxHealthMetrics(
            PendingCount: 15000,
            CapturedCount: 0,
            DeadLetterCount: 2000,
            OldestPendingMessageAt: now.AddHours(-1));

        var inboxManager = CreateMockInboxManagerWithMetrics(metrics);
        var healthCheck = new InboxHealthCheck(inboxManager, _inboxName, _options, _dateTimeProvider);

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        // Queue depth is checked first
        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Description.Should().Contain("Queue depth");
    }

    [Fact]
    public async Task CheckHealthAsync_CriticalTakesPriorityOverWarning()
    {
        var now = DateTime.UtcNow;
        _dateTimeProvider.GetUtcNow().Returns(now);

        // Warning queue depth, critical dead letter
        var metrics = new InboxHealthMetrics(
            PendingCount: 1100,
            CapturedCount: 0,
            DeadLetterCount: 1500,
            OldestPendingMessageAt: null);

        var inboxManager = CreateMockInboxManagerWithMetrics(metrics);
        var healthCheck = new InboxHealthCheck(inboxManager, _inboxName, _options, _dateTimeProvider);

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        // Critical dead letter takes priority over warning queue depth
        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Description.Should().Contain("Dead letter count");
    }

    #endregion

    #region HealthCheckBuilderExtensions Tests

    [Fact]
    public void AddInboxHealthChecks_NoRegistry_ReturnsBuilder()
    {
        var services = new ServiceCollection();
        var healthChecksBuilder = Substitute.For<IHealthChecksBuilder>();
        healthChecksBuilder.Services.Returns(services);

        var result = healthChecksBuilder.AddInboxHealthChecks();

        result.Should().BeSameAs(healthChecksBuilder);
    }

    [Fact]
    public void AddInboxHealthChecks_SingleInbox_RegistersHealthCheck()
    {
        var services = new ServiceCollection();
        var registry = new InboxConfigurationRegistry();
        services.AddSingleton(registry);

        var configuration = CreateMockConfiguration("test-inbox", enabled: true);
        registry.Register(configuration);

        var healthChecksBuilder = Substitute.For<IHealthChecksBuilder>();
        healthChecksBuilder.Services.Returns(services);

        var registrations = new List<HealthCheckRegistration>();
        healthChecksBuilder.Add(Arg.Do<HealthCheckRegistration>(r => registrations.Add(r)));

        healthChecksBuilder.AddInboxHealthChecks();

        registrations.Should().HaveCount(1);
        registrations[0].Name.Should().Be("inbox:test-inbox");
    }

    [Fact]
    public void AddInboxHealthChecks_MultipleInboxes_RegistersAllHealthChecks()
    {
        var services = new ServiceCollection();
        var registry = new InboxConfigurationRegistry();
        services.AddSingleton(registry);

        registry.Register(CreateMockConfiguration("inbox-1", enabled: true));
        registry.Register(CreateMockConfiguration("inbox-2", enabled: true));
        registry.Register(CreateMockConfiguration("inbox-3", enabled: true));

        var healthChecksBuilder = Substitute.For<IHealthChecksBuilder>();
        healthChecksBuilder.Services.Returns(services);

        var registrations = new List<HealthCheckRegistration>();
        healthChecksBuilder.Add(Arg.Do<HealthCheckRegistration>(r => registrations.Add(r)));

        healthChecksBuilder.AddInboxHealthChecks();

        registrations.Should().HaveCount(3);
        registrations.Select(r => r.Name).Should().BeEquivalentTo(
            new[] { "inbox:inbox-1", "inbox:inbox-2", "inbox:inbox-3" });
    }

    [Fact]
    public void AddInboxHealthChecks_DisabledInbox_SkipsRegistration()
    {
        var services = new ServiceCollection();
        var registry = new InboxConfigurationRegistry();
        services.AddSingleton(registry);

        registry.Register(CreateMockConfiguration("enabled-inbox", enabled: true));
        registry.Register(CreateMockConfiguration("disabled-inbox", enabled: false));

        var healthChecksBuilder = Substitute.For<IHealthChecksBuilder>();
        healthChecksBuilder.Services.Returns(services);

        var registrations = new List<HealthCheckRegistration>();
        healthChecksBuilder.Add(Arg.Do<HealthCheckRegistration>(r => registrations.Add(r)));

        healthChecksBuilder.AddInboxHealthChecks();

        registrations.Should().HaveCount(1);
        registrations[0].Name.Should().Be("inbox:enabled-inbox");
    }

    [Fact]
    public void AddInboxHealthChecks_UsesConfiguredTags()
    {
        var services = new ServiceCollection();
        var registry = new InboxConfigurationRegistry();
        services.AddSingleton(registry);

        var healthCheckOptions = new InboxHealthCheckOptions
        {
            Enabled = true,
            Tags = ["custom", "tags"]
        };
        var configuration = CreateMockConfiguration("test-inbox", healthCheckOptions);
        registry.Register(configuration);

        var healthChecksBuilder = Substitute.For<IHealthChecksBuilder>();
        healthChecksBuilder.Services.Returns(services);

        var registrations = new List<HealthCheckRegistration>();
        healthChecksBuilder.Add(Arg.Do<HealthCheckRegistration>(r => registrations.Add(r)));

        healthChecksBuilder.AddInboxHealthChecks();

        registrations[0].Tags.Should().BeEquivalentTo(new[] { "custom", "tags" });
    }

    #endregion

    #region Helper Methods

    private InboxManager CreateMockInboxManager(bool isRunning)
    {
        var configurationRegistry = new InboxConfigurationRegistry();
        var serviceProvider = CreateServiceProvider();
        var lifecycle = Substitute.For<IInboxLifecycle>();
        lifecycle.IsRunning.Returns(isRunning);
        var inboxFactory = Substitute.For<IInboxFactory>();
        var logger = Substitute.For<ILogger<InboxManager>>();

        return new InboxManager(
            configurationRegistry,
            serviceProvider,
            lifecycle,
            Enumerable.Empty<IInboxLifecycleHook>(),
            inboxFactory,
            logger);
    }

    private InboxManager CreateMockInboxManagerWithProvider(IInboxStorageProvider storageProvider)
    {
        var configurationRegistry = new InboxConfigurationRegistry();
        var serviceProvider = CreateServiceProvider();
        var lifecycle = Substitute.For<IInboxLifecycle>();
        lifecycle.IsRunning.Returns(true);

        var inbox = CreateMockInbox(storageProvider);
        var inboxFactory = Substitute.For<IInboxFactory>();
        inboxFactory.Create(Arg.Any<IInboxConfiguration>()).Returns(inbox);

        var configuration = CreateMockConfiguration(_inboxName, enabled: false);
        configurationRegistry.Register(configuration);

        var logger = Substitute.For<ILogger<InboxManager>>();

        return new InboxManager(
            configurationRegistry,
            serviceProvider,
            lifecycle,
            Enumerable.Empty<IInboxLifecycleHook>(),
            inboxFactory,
            logger);
    }

    private InboxManager CreateMockInboxManagerWithMetrics(InboxHealthMetrics metrics)
    {
        var storageProvider = Substitute.For<IInboxStorageProvider, ISupportHealthCheck>();
        ((ISupportHealthCheck)storageProvider).GetHealthMetricsAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(metrics));

        return CreateMockInboxManagerWithProvider(storageProvider);
    }

    private InboxBase CreateMockInbox(IInboxStorageProvider storageProvider)
    {
        return new TestInboxBase(
            Substitute.For<IInboxConfiguration>(),
            storageProvider,
            Substitute.For<Rh.Inbox.Abstractions.Serialization.IInboxMessagePayloadSerializer>(),
            Substitute.For<IDateTimeProvider>());
    }

    private IServiceProvider CreateServiceProvider()
    {
        var services = new ServiceCollection();
        var strategyFactory = Substitute.For<IInboxProcessingStrategyFactory>();
        services.AddSingleton(strategyFactory);
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        return services.BuildServiceProvider();
    }

    private IInboxConfiguration CreateMockConfiguration(string inboxName, bool enabled)
    {
        var healthCheckOptions = new InboxHealthCheckOptions { Enabled = enabled };
        return CreateMockConfiguration(inboxName, healthCheckOptions);
    }

    private IInboxConfiguration CreateMockConfiguration(string inboxName, IInboxHealthCheckOptions healthCheckOptions)
    {
        var configuration = Substitute.For<IInboxConfiguration>();
        configuration.InboxName.Returns(inboxName);
        configuration.HealthCheckOptions.Returns(healthCheckOptions);
        configuration.MetadataRegistry.Returns(Substitute.For<IInboxMessageMetadataRegistry>());
        return configuration;
    }

    #endregion

    #region Test Helpers

    private sealed class NullLogger<T> : ILogger<T>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => false;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) { }
    }

    internal class TestInboxBase : InboxBase
    {
        public TestInboxBase(
            IInboxConfiguration configuration,
            IInboxStorageProvider storageProvider,
            Rh.Inbox.Abstractions.Serialization.IInboxMessagePayloadSerializer serializer,
            IDateTimeProvider dateTimeProvider)
            : base(configuration, storageProvider, serializer, dateTimeProvider)
        {
        }

        public override InboxType Type => InboxType.Default;
    }

    #endregion
}
