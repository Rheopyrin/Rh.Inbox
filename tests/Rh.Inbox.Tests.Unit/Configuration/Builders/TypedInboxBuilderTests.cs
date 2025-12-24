using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Rh.Inbox.Abstractions.Builders;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Configuration.Builders;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Configuration.Builders;

public class TypedInboxBuilderTests
{
    private readonly IServiceCollection _services;

    public TypedInboxBuilderTests()
    {
        _services = new ServiceCollection();
    }

    private IDefaultInboxBuilder CreateBuilder(string inboxName = "test-inbox")
    {
        var parentBuilder = new InboxBuilder(_services, inboxName);
        return parentBuilder.AsDefault();
    }

    #region UseStorageProviderFactory Tests

    [Fact]
    public void UseStorageProviderFactory_WithInstance_ReturnsBuilder()
    {
        var builder = CreateBuilder();
        var factory = Substitute.For<IInboxStorageProviderFactory>();

        var result = builder.UseStorageProviderFactory(factory);

        result.Should().BeSameAs(builder);
    }

    [Fact]
    public void UseStorageProviderFactory_WithNull_ThrowsArgumentNullException()
    {
        var builder = CreateBuilder();

        var act = () => builder.UseStorageProviderFactory((IInboxStorageProviderFactory)null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void UseStorageProviderFactory_WithFunc_ReturnsBuilder()
    {
        var builder = CreateBuilder();
        Func<IServiceProvider, IInboxStorageProviderFactory> factoryFunc = _ => Substitute.For<IInboxStorageProviderFactory>();

        var result = builder.UseStorageProviderFactory(factoryFunc);

        result.Should().BeSameAs(builder);
    }

    [Fact]
    public void UseStorageProviderFactory_WithNullFunc_ThrowsArgumentNullException()
    {
        var builder = CreateBuilder();

        var act = () => builder.UseStorageProviderFactory((Func<IServiceProvider, IInboxStorageProviderFactory>)null!);

        act.Should().Throw<ArgumentNullException>();
    }

    #endregion

    #region ConfigureServices Tests

    [Fact]
    public void ConfigureServices_StoresActionForDeferredExecution()
    {
        var builder = CreateBuilder();
        var called = false;

        // ConfigureServices stores the action for deferred execution during Build
        // It doesn't call the action immediately
        builder.ConfigureServices(_ =>
        {
            called = true;
        });

        // Action should NOT be called immediately - it's stored for later
        called.Should().BeFalse("ConfigureServices should store the action, not call it immediately");
    }

    [Fact]
    public void ConfigureServices_ReturnsBuilder()
    {
        var builder = CreateBuilder();

        var result = builder.ConfigureServices(_ => { });

        result.Should().BeSameAs(builder);
    }

    [Fact]
    public void ConfigureServices_WithNull_ThrowsArgumentNullException()
    {
        var builder = CreateBuilder();

        var act = () => builder.ConfigureServices(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    #endregion

    #region ConfigureOptions Tests

    [Fact]
    public void ConfigureOptions_CallsConfigureAction()
    {
        var builder = CreateBuilder();
        IConfigureInboxOptions? capturedOptions = null;

        builder.ConfigureOptions(options =>
        {
            capturedOptions = options;
            options.ReadBatchSize = 50;
        });

        capturedOptions.Should().NotBeNull();
        capturedOptions!.ReadBatchSize.Should().Be(50);
    }

    [Fact]
    public void ConfigureOptions_ReturnsBuilder()
    {
        var builder = CreateBuilder();

        var result = builder.ConfigureOptions(_ => { });

        result.Should().BeSameAs(builder);
    }

    [Fact]
    public void ConfigureOptions_WithNull_ThrowsArgumentNullException()
    {
        var builder = CreateBuilder();

        var act = () => builder.ConfigureOptions(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    #endregion

    #region UseSerializerFactory Tests

    [Fact]
    public void UseSerializerFactory_WithInstance_ReturnsBuilder()
    {
        var builder = CreateBuilder();
        var factory = Substitute.For<IInboxSerializerFactory>();

        var result = builder.UseSerializerFactory(factory);

        result.Should().BeSameAs(builder);
    }

    [Fact]
    public void UseSerializerFactory_WithNull_ThrowsArgumentNullException()
    {
        var builder = CreateBuilder();

        var act = () => builder.UseSerializerFactory((IInboxSerializerFactory)null!);

        act.Should().Throw<ArgumentNullException>();
    }

    #endregion

    #region RegisterMessage Tests

    [Fact]
    public void RegisterMessage_ReturnsBuilder()
    {
        var builder = CreateBuilder();

        var result = builder.RegisterMessage<TestMessage>();

        result.Should().BeSameAs(builder);
    }

    [Fact]
    public void RegisterMessage_WithCustomMessageType_ReturnsBuilder()
    {
        var builder = CreateBuilder();

        var result = builder.RegisterMessage<TestMessage>("custom.message.type");

        result.Should().BeSameAs(builder);
    }

    #endregion

    #region ConfigureHealthCheck Tests

    [Fact]
    public void ConfigureHealthCheck_CallsConfigureAction()
    {
        var builder = CreateBuilder();
        IInboxHealthCheckOptions? capturedOptions = null;

        builder.ConfigureHealthCheck(options =>
        {
            capturedOptions = options;
            options.Enabled = true;
        });

        capturedOptions.Should().NotBeNull();
        capturedOptions!.Enabled.Should().BeTrue();
    }

    [Fact]
    public void ConfigureHealthCheck_ReturnsBuilder()
    {
        var builder = CreateBuilder();

        var result = builder.ConfigureHealthCheck(_ => { });

        result.Should().BeSameAs(builder);
    }

    [Fact]
    public void ConfigureHealthCheck_WithNull_ThrowsArgumentNullException()
    {
        var builder = CreateBuilder();

        var act = () => builder.ConfigureHealthCheck(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    #endregion

    #region InboxName Property Tests

    [Fact]
    public void InboxName_ReturnsConfiguredName()
    {
        var builder = CreateBuilder("my-inbox");

        builder.InboxName.Should().Be("my-inbox");
    }

    #endregion

    #region Fluent Chaining Tests

    [Fact]
    public void FluentChaining_AllMethodsCanBeChained()
    {
        var builder = CreateBuilder();
        var factory = Substitute.For<IInboxStorageProviderFactory>();
        var serializerFactory = Substitute.For<IInboxSerializerFactory>();

        var result = builder
            .UseStorageProviderFactory(factory)
            .ConfigureServices(_ => { })
            .ConfigureOptions(opts => opts.ReadBatchSize = 50)
            .UseSerializerFactory(serializerFactory)
            .RegisterMessage<TestMessage>()
            .ConfigureHealthCheck(opts => opts.Enabled = true);

        result.Should().BeSameAs(builder);
    }

    #endregion

    private class TestMessage { }
}
