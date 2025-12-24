using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Lifecycle;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Configuration;
using Rh.Inbox.Configuration.Registry;
using Rh.Inbox.Exceptions;
using Rh.Inbox.Health;
using Rh.Inbox.Inboxes;
using Rh.Inbox.Inboxes.Factory;
using Rh.Inbox.Management;
using Rh.Inbox.Processing.Strategies;
using Rh.Inbox.Processing.Strategies.Factory;
using Rh.Inbox.Tests.Unit.TestHelpers;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Management;

public class InboxManagerTests
{
    private readonly InboxConfigurationRegistry _configurationRegistry;
    private readonly IServiceProvider _serviceProvider;
    private readonly IInboxLifecycle _lifecycle;
    private readonly IInboxFactory _inboxFactory;
    private readonly ILogger<InboxManager> _logger;

    public InboxManagerTests()
    {
        _configurationRegistry = new InboxConfigurationRegistry();
        _serviceProvider = Substitute.For<IServiceProvider>();
        _lifecycle = Substitute.For<IInboxLifecycle>();
        _inboxFactory = Substitute.For<IInboxFactory>();
        _logger = Substitute.For<ILogger<InboxManager>>();
    }

    private InboxManager CreateManager(IEnumerable<IInboxLifecycleHook>? hooks = null)
    {
        return new InboxManager(
            _configurationRegistry,
            _serviceProvider,
            _lifecycle,
            hooks ?? Enumerable.Empty<IInboxLifecycleHook>(),
            _inboxFactory,
            _logger);
    }

    #region IsRunning Tests

    [Fact]
    public void IsRunning_DelegatesToLifecycle()
    {
        _lifecycle.IsRunning.Returns(true);
        var manager = CreateManager();

        var result = manager.IsRunning;

        result.Should().BeTrue();
        _ = _lifecycle.Received(1).IsRunning;
    }

    [Fact]
    public void IsRunning_WhenLifecycleNotRunning_ReturnsFalse()
    {
        _lifecycle.IsRunning.Returns(false);
        var manager = CreateManager();

        var result = manager.IsRunning;

        result.Should().BeFalse();
    }

    #endregion

    #region StartAsync Tests

    [Fact]
    public async Task StartAsync_WhenNotRunning_StartsLifecycle()
    {
        _lifecycle.IsRunning.Returns(false);
        var manager = CreateManager();

        await manager.StartAsync(CancellationToken.None);

        _lifecycle.Received(1).Start();
    }

    [Fact]
    public async Task StartAsync_WhenAlreadyRunning_DoesNotStartAgain()
    {
        _lifecycle.IsRunning.Returns(true);
        var manager = CreateManager();

        await manager.StartAsync(CancellationToken.None);

        _lifecycle.DidNotReceive().Start();
    }

    [Fact]
    public async Task StartAsync_CallsLifecycleHooksOnStart()
    {
        _lifecycle.IsRunning.Returns(false);
        var hook1 = Substitute.For<IInboxLifecycleHook>();
        var hook2 = Substitute.For<IInboxLifecycleHook>();
        hook1.OnStart(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        hook2.OnStart(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);

        var manager = CreateManager(new[] { hook1, hook2 });

        await manager.StartAsync(CancellationToken.None);

        await hook1.Received(1).OnStart(Arg.Any<CancellationToken>());
        await hook2.Received(1).OnStart(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task StartAsync_WhenHookThrows_StopsLifecycleAndRethrows()
    {
        _lifecycle.IsRunning.Returns(false);
        var hook = Substitute.For<IInboxLifecycleHook>();
        hook.OnStart(Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException("Hook failed"));

        var manager = CreateManager(new[] { hook });

        var act = () => manager.StartAsync(CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Hook failed");
        _lifecycle.Received(1).Stop();
    }

    [Fact]
    public async Task StartAsync_MultipleCallsWhileRunning_OnlyStartsOnce()
    {
        var callCount = 0;
        _lifecycle.IsRunning.Returns(_ => callCount > 0);
        _lifecycle.When(x => x.Start()).Do(_ => callCount++);

        var manager = CreateManager();

        await manager.StartAsync(CancellationToken.None);
        await manager.StartAsync(CancellationToken.None);
        await manager.StartAsync(CancellationToken.None);

        _lifecycle.Received(1).Start();
    }

    #endregion

    #region Inbox Initialization Tests

    [Fact]
    public void InboxManager_WithRegisteredConfiguration_CallsInboxFactoryDuringConstruction()
    {
        var (serviceProvider, configuration, _) = SetupInboxInitialization("test-inbox");

        // Factory is called during construction (in InitializeInboxes)
        _ = CreateManagerWithServiceProvider(serviceProvider);

        _inboxFactory.Received(1).Create(configuration);
    }

    [Fact]
    public void InboxManager_WithMultipleConfigurations_InitializesAllDuringConstruction()
    {
        var serviceProvider = CreateServiceProviderWithProcessingLoopDependencies();

        var config1 = CreateAndRegisterConfiguration("inbox-1");
        var config2 = CreateAndRegisterConfiguration("inbox-2");
        var config3 = CreateAndRegisterConfiguration("inbox-3");

        var mockInbox1 = CreateMockInbox("inbox-1");
        var mockInbox2 = CreateMockInbox("inbox-2");
        var mockInbox3 = CreateMockInbox("inbox-3");

        _inboxFactory.Create(config1).Returns(mockInbox1);
        _inboxFactory.Create(config2).Returns(mockInbox2);
        _inboxFactory.Create(config3).Returns(mockInbox3);

        // Factory calls happen during construction (in InitializeInboxes)
        _ = CreateManagerWithServiceProvider(serviceProvider);

        _inboxFactory.Received(1).Create(config1);
        _inboxFactory.Received(1).Create(config2);
        _inboxFactory.Received(1).Create(config3);
    }

    [Fact]
    public void InboxManager_AfterConstruction_InboxIsAccessibleViaGetInbox()
    {
        var (serviceProvider, _, mockInbox) = SetupInboxInitialization("my-inbox");

        var manager = CreateManagerWithServiceProvider(serviceProvider);

        // Inbox is accessible immediately after construction, no StartAsync needed
        var inbox = manager.GetInbox("my-inbox");
        inbox.Should().BeSameAs(mockInbox);
    }

    [Fact]
    public void InboxManager_AfterConstruction_InboxInternalIsAccessible()
    {
        var (serviceProvider, _, mockInbox) = SetupInboxInitialization("internal-inbox");

        var manager = CreateManagerWithServiceProvider(serviceProvider);

        // Inbox internal is accessible immediately after construction, no StartAsync needed
        var inbox = manager.GetInboxInternal("internal-inbox");
        inbox.Should().BeSameAs(mockInbox);
    }

    [Fact]
    public void InboxManager_WithNoConfigurations_DoesNotCallFactory()
    {
        // Factory should not be called if there are no configurations
        _ = CreateManager();

        _inboxFactory.DidNotReceiveWithAnyArgs().Create(default!);
    }

    [Fact]
    public void InboxManager_WhenFactoryThrows_ThrowsDuringConstruction()
    {
        var configuration = CreateAndRegisterConfiguration("failing-inbox");
        _inboxFactory.Create(configuration).Throws(new InvalidOperationException("Factory failed"));

        // Factory is called during construction (in InitializeInboxes)
        var act = () => CreateManager();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("Factory failed");
    }

    private (IServiceProvider, InboxConfiguration, InboxBase) SetupInboxInitialization(string inboxName, bool hasRegisteredMessages = true)
    {
        var serviceProvider = CreateServiceProviderWithProcessingLoopDependencies();
        var configuration = CreateAndRegisterConfiguration(inboxName, hasRegisteredMessages);
        var mockInbox = CreateMockInbox(inboxName);
        _inboxFactory.Create(configuration).Returns(mockInbox);

        return (serviceProvider, configuration, mockInbox);
    }

    private IServiceProvider CreateServiceProviderWithProcessingLoopDependencies()
    {
        var services = new ServiceCollection();

        var strategyFactory = Substitute.For<IInboxProcessingStrategyFactory>();
        var strategy = Substitute.For<IInboxProcessingStrategy>();
        strategyFactory.Create(Arg.Any<InboxBase>()).Returns(strategy);

        services.AddSingleton(strategyFactory);
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));

        return services.BuildServiceProvider();
    }

    private InboxConfiguration CreateAndRegisterConfiguration(string inboxName, bool hasRegisteredMessages = true)
    {
        var options = TestConfigurationFactory.CreateOptions(inboxName);
        var metadataRegistry = Substitute.For<IInboxMessageMetadataRegistry>();
        metadataRegistry.HasRegisteredMessages.Returns(hasRegisteredMessages);

        var configuration = new InboxConfiguration
        {
            InboxName = inboxName,
            InboxType = InboxType.Default,
            Options = options,
            MetadataRegistry = metadataRegistry,
            StorageProviderFactoryFunc = _ => Substitute.For<IInboxStorageProviderFactory>(),
            SerializerFactoryFunc = _ => Substitute.For<IInboxSerializerFactory>(),
            HealthCheckOptions = new InboxHealthCheckOptions(),
            DateTimeProvider = Substitute.For<IDateTimeProvider>()
        };

        _configurationRegistry.Register(configuration);
        return configuration;
    }

    private InboxBase CreateMockInbox(string inboxName)
    {
        var options = TestConfigurationFactory.CreateOptions(inboxName);
        var metadataRegistry = Substitute.For<IInboxMessageMetadataRegistry>();

        var configuration = new InboxConfiguration
        {
            InboxName = inboxName,
            InboxType = InboxType.Default,
            Options = options,
            MetadataRegistry = metadataRegistry,
            StorageProviderFactoryFunc = _ => Substitute.For<IInboxStorageProviderFactory>(),
            SerializerFactoryFunc = _ => Substitute.For<IInboxSerializerFactory>(),
            HealthCheckOptions = new InboxHealthCheckOptions(),
            DateTimeProvider = Substitute.For<IDateTimeProvider>()
        };

        var storageProvider = Substitute.For<IInboxStorageProvider>();
        var serializer = Substitute.For<IInboxMessagePayloadSerializer>();
        var dateTimeProvider = Substitute.For<IDateTimeProvider>();

        var inbox = Substitute.For<InboxBase>(configuration, storageProvider, serializer, dateTimeProvider);
        return inbox;
    }

    private InboxManager CreateManagerWithServiceProvider(IServiceProvider serviceProvider)
    {
        return new InboxManager(
            _configurationRegistry,
            serviceProvider,
            _lifecycle,
            Enumerable.Empty<IInboxLifecycleHook>(),
            _inboxFactory,
            _logger);
    }

    private sealed class NullLogger<T> : ILogger<T>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => false;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) { }
    }

    #endregion

    #region Inbox Without Handlers Tests

    [Fact]
    public void InboxWithoutHandlers_IsAccessibleBeforeStart()
    {
        var (serviceProvider, _, mockInbox) = SetupInboxInitialization("no-handlers-inbox", hasRegisteredMessages: false);

        var manager = CreateManagerWithServiceProvider(serviceProvider);

        // Inbox should be accessible immediately after manager creation, before StartAsync
        var inbox = manager.GetInbox("no-handlers-inbox");
        inbox.Should().BeSameAs(mockInbox);
    }

    [Fact]
    public void InboxWithoutHandlers_InternalIsAccessibleBeforeStart()
    {
        var (serviceProvider, _, mockInbox) = SetupInboxInitialization("no-handlers-internal", hasRegisteredMessages: false);

        var manager = CreateManagerWithServiceProvider(serviceProvider);

        var inbox = manager.GetInboxInternal("no-handlers-internal");
        inbox.Should().BeSameAs(mockInbox);
    }

    [Fact]
    public async Task InboxWithoutHandlers_StartAsyncSucceeds()
    {
        _lifecycle.IsRunning.Returns(false);
        var (serviceProvider, _, _) = SetupInboxInitialization("no-handlers-start", hasRegisteredMessages: false);

        var manager = CreateManagerWithServiceProvider(serviceProvider);

        // StartAsync should succeed even though no processing loop was created
        await manager.StartAsync(CancellationToken.None);

        _lifecycle.Received(1).Start();
    }

    [Fact]
    public async Task InboxWithoutHandlers_StopAsyncSucceeds()
    {
        _lifecycle.IsRunning.Returns(true);
        var (serviceProvider, _, _) = SetupInboxInitialization("no-handlers-stop", hasRegisteredMessages: false);

        var manager = CreateManagerWithServiceProvider(serviceProvider);

        // StopAsync should succeed even though no processing loop was created
        await manager.StopAsync(CancellationToken.None);

        _lifecycle.Received(1).Stop();
    }

    [Fact]
    public void InboxWithHandlers_IsAccessibleBeforeStart()
    {
        var (serviceProvider, _, mockInbox) = SetupInboxInitialization("with-handlers-inbox", hasRegisteredMessages: true);

        var manager = CreateManagerWithServiceProvider(serviceProvider);

        // Inbox with handlers should also be accessible before StartAsync
        var inbox = manager.GetInbox("with-handlers-inbox");
        inbox.Should().BeSameAs(mockInbox);
    }

    [Fact]
    public async Task MixedInboxes_WithAndWithoutHandlers_AllAccessible()
    {
        _lifecycle.IsRunning.Returns(false);
        var serviceProvider = CreateServiceProviderWithProcessingLoopDependencies();

        var configWithHandlers = CreateAndRegisterConfiguration("with-handlers", hasRegisteredMessages: true);
        var configWithoutHandlers = CreateAndRegisterConfiguration("without-handlers", hasRegisteredMessages: false);

        var inboxWithHandlers = CreateMockInbox("with-handlers");
        var inboxWithoutHandlers = CreateMockInbox("without-handlers");

        _inboxFactory.Create(configWithHandlers).Returns(inboxWithHandlers);
        _inboxFactory.Create(configWithoutHandlers).Returns(inboxWithoutHandlers);

        var manager = CreateManagerWithServiceProvider(serviceProvider);

        // Both should be accessible
        manager.GetInbox("with-handlers").Should().BeSameAs(inboxWithHandlers);
        manager.GetInbox("without-handlers").Should().BeSameAs(inboxWithoutHandlers);

        // StartAsync should work
        await manager.StartAsync(CancellationToken.None);
        _lifecycle.Received(1).Start();
    }

    #endregion

    #region StopAsync Tests

    [Fact]
    public async Task StopAsync_WhenRunning_StopsLifecycle()
    {
        _lifecycle.IsRunning.Returns(true);
        var manager = CreateManager();

        await manager.StopAsync(CancellationToken.None);

        _lifecycle.Received(1).Stop();
    }

    [Fact]
    public async Task StopAsync_WhenNotRunning_DoesNotStopAgain()
    {
        _lifecycle.IsRunning.Returns(false);
        var manager = CreateManager();

        await manager.StopAsync(CancellationToken.None);

        _lifecycle.DidNotReceive().Stop();
    }

    [Fact]
    public async Task StopAsync_CallsLifecycleHooksOnStop()
    {
        _lifecycle.IsRunning.Returns(true);
        var hook1 = Substitute.For<IInboxLifecycleHook>();
        var hook2 = Substitute.For<IInboxLifecycleHook>();
        hook1.OnStop(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        hook2.OnStop(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);

        var manager = CreateManager(new[] { hook1, hook2 });

        await manager.StopAsync(CancellationToken.None);

        await hook1.Received(1).OnStop(Arg.Any<CancellationToken>());
        await hook2.Received(1).OnStop(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task StopAsync_WhenHookThrows_ContinuesWithOtherHooks()
    {
        _lifecycle.IsRunning.Returns(true);
        var hook1 = Substitute.For<IInboxLifecycleHook>();
        var hook2 = Substitute.For<IInboxLifecycleHook>();
        hook1.OnStop(Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException("Hook1 failed"));
        hook2.OnStop(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);

        var manager = CreateManager(new[] { hook1, hook2 });

        // Should not throw - errors in hooks are logged but don't stop the process
        await manager.StopAsync(CancellationToken.None);

        await hook2.Received(1).OnStop(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task StopAsync_AlsoCallsHooksWhenNotRunning()
    {
        _lifecycle.IsRunning.Returns(false);
        var hook = Substitute.For<IInboxLifecycleHook>();
        hook.OnStop(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);

        var manager = CreateManager(new[] { hook });

        await manager.StopAsync(CancellationToken.None);

        // Hooks are called in finally block, so they're called even if not running
        await hook.Received(1).OnStop(Arg.Any<CancellationToken>());
    }

    #endregion

    #region GetInbox Tests

    [Fact]
    public void GetInbox_WithoutName_UsesDefaultName()
    {
        var manager = CreateManager();

        var act = () => manager.GetInbox();

        // Should throw because no inbox is registered
        act.Should().Throw<InboxNotFoundException>();
    }

    [Fact]
    public void GetInbox_UnregisteredInbox_ThrowsInboxNotFoundException()
    {
        var manager = CreateManager();

        var act = () => manager.GetInbox("non-existent");

        act.Should().Throw<InboxNotFoundException>()
            .Which.InboxName.Should().Be("non-existent");
    }

    #endregion

    #region GetInboxInternal Tests

    [Fact]
    public void GetInboxInternal_UnregisteredInbox_ThrowsInboxNotFoundException()
    {
        var manager = CreateManager();

        var act = () => manager.GetInboxInternal("non-existent");

        act.Should().Throw<InboxNotFoundException>()
            .Which.InboxName.Should().Be("non-existent");
    }

    #endregion

    #region DisposeAsync Tests

    [Fact]
    public async Task DisposeAsync_CanBeCalledMultipleTimes()
    {
        var manager = CreateManager();

        var act = async () =>
        {
            await manager.DisposeAsync();
            await manager.DisposeAsync();
            await manager.DisposeAsync();
        };

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task DisposeAsync_WhenRunning_StopsFirst()
    {
        _lifecycle.IsRunning.Returns(true); // Returns true so StopAsync proceeds to call Stop()
        var manager = CreateManager();

        await manager.DisposeAsync();

        _lifecycle.Received(1).Stop();
    }

    [Fact]
    public async Task DisposeAsync_WhenNotRunning_DoesNotStop()
    {
        _lifecycle.IsRunning.Returns(false);
        var manager = CreateManager();

        await manager.DisposeAsync();

        _lifecycle.DidNotReceive().Stop();
    }

    #endregion
}