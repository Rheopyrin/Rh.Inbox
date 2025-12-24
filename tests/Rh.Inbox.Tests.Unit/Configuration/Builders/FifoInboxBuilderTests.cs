using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Rh.Inbox.Abstractions.Builders;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Configuration.Builders;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Configuration.Builders;

public class FifoInboxBuilderTests
{
    private readonly IServiceCollection _services;

    public FifoInboxBuilderTests()
    {
        _services = new ServiceCollection();
    }

    private IFifoInboxBuilder CreateBuilder(string inboxName = "test-inbox")
    {
        var parentBuilder = new InboxBuilder(_services, inboxName);
        return parentBuilder.AsFifo();
    }

    #region RegisterHandler Tests

    [Fact]
    public void RegisterHandler_GenericType_ReturnsBuilder()
    {
        var builder = CreateBuilder();

        var result = builder.RegisterHandler<TestFifoHandler, TestFifoMessage>();

        result.Should().BeSameAs(builder);
    }

    [Fact]
    public void RegisterHandler_FactoryDelegate_ReturnsBuilder()
    {
        var builder = CreateBuilder();

        var result = builder.RegisterHandler<TestFifoMessage>(sp => new TestFifoHandler());

        result.Should().BeSameAs(builder);
    }

    [Fact]
    public void RegisterHandler_Instance_ReturnsBuilder()
    {
        var builder = CreateBuilder();
        var handler = new TestFifoHandler();

        var result = builder.RegisterHandler(handler);

        result.Should().BeSameAs(builder);
    }

    #endregion

    #region Explicit Interface Implementation Tests

    [Fact]
    public void ExplicitInterface_UseStorageProviderFactory_ReturnsBuilder()
    {
        IFifoInboxBuilder builder = CreateBuilder();
        var factory = Substitute.For<IInboxStorageProviderFactory>();

        var result = builder.UseStorageProviderFactory(factory);

        result.Should().BeSameAs(builder);
    }

    [Fact]
    public void ExplicitInterface_UseStorageProviderFactory_Func_ReturnsBuilder()
    {
        IFifoInboxBuilder builder = CreateBuilder();

        var result = builder.UseStorageProviderFactory(sp => Substitute.For<IInboxStorageProviderFactory>());

        result.Should().BeSameAs(builder);
    }

    [Fact]
    public void ExplicitInterface_UseSerializerFactory_ReturnsBuilder()
    {
        IFifoInboxBuilder builder = CreateBuilder();
        var factory = Substitute.For<IInboxSerializerFactory>();

        var result = builder.UseSerializerFactory(factory);

        result.Should().BeSameAs(builder);
    }

    [Fact]
    public void ExplicitInterface_ConfigureServices_ReturnsBuilder()
    {
        IFifoInboxBuilder builder = CreateBuilder();

        var result = builder.ConfigureServices(_ => { });

        result.Should().BeSameAs(builder);
    }

    [Fact]
    public void ExplicitInterface_ConfigureOptions_ReturnsBuilder()
    {
        IFifoInboxBuilder builder = CreateBuilder();

        var result = builder.ConfigureOptions(_ => { });

        result.Should().BeSameAs(builder);
    }

    [Fact]
    public void ExplicitInterface_ConfigureOptions_WithNull_ThrowsArgumentNullException()
    {
        IFifoInboxBuilder builder = CreateBuilder();

        var act = () => builder.ConfigureOptions(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void ExplicitInterface_ConfigureHealthCheck_ReturnsBuilder()
    {
        IFifoInboxBuilder builder = CreateBuilder();

        var result = builder.ConfigureHealthCheck(_ => { });

        result.Should().BeSameAs(builder);
    }

    [Fact]
    public void ExplicitInterface_ConfigureHealthCheck_WithNull_ThrowsArgumentNullException()
    {
        IFifoInboxBuilder builder = CreateBuilder();

        var act = () => builder.ConfigureHealthCheck(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void ExplicitInterface_RegisterMessage_ReturnsBuilder()
    {
        IFifoInboxBuilder builder = CreateBuilder();

        var result = builder.RegisterMessage<TestFifoMessage>();

        result.Should().BeSameAs(builder);
    }

    [Fact]
    public void ExplicitInterface_PostConfigure_ReturnsBuilder()
    {
        IFifoInboxBuilder builder = CreateBuilder();

        var result = builder.PostConfigure((config, services) => { });

        result.Should().BeSameAs(builder);
    }

    #endregion

    #region Fluent Chaining Tests

    [Fact]
    public void FluentChaining_AllMethodsCanBeChained()
    {
        var builder = CreateBuilder();
        var storageFactory = Substitute.For<IInboxStorageProviderFactory>();
        var serializerFactory = Substitute.For<IInboxSerializerFactory>();
        var handler = new TestFifoHandler();

        var result = builder
            .UseStorageProviderFactory(storageFactory)
            .UseSerializerFactory(serializerFactory)
            .ConfigureServices(_ => { })
            .ConfigureOptions(opts => opts.ReadBatchSize = 50)
            .ConfigureHealthCheck(opts => opts.Enabled = true)
            .RegisterMessage<TestFifoMessage>()
            .RegisterHandler(handler);

        result.Should().BeSameAs(builder);
    }

    #endregion

    #region Test Helpers

    private class TestFifoMessage : IHasGroupId
    {
        public string GroupId { get; set; } = "test-group";
        public string GetGroupId() => GroupId;
    }

    private class TestFifoHandler : IFifoInboxHandler<TestFifoMessage>
    {
        public Task<InboxHandleResult> HandleAsync(InboxMessageEnvelope<TestFifoMessage> message, CancellationToken token)
        {
            return Task.FromResult(InboxHandleResult.Success);
        }
    }

    #endregion
}
