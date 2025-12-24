using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Configuration.Builders;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Configuration.Builders;

public class InboxBuilderTests
{
    private readonly IServiceCollection _services;

    public InboxBuilderTests()
    {
        _services = new ServiceCollection();
    }

    private InboxBuilder CreateBuilder(string inboxName = "test-inbox")
    {
        return new InboxBuilder(_services, inboxName);
    }

    #region Type Selection Tests

    [Fact]
    public void AsDefault_ReturnsDefaultInboxBuilder()
    {
        var builder = CreateBuilder();

        var result = builder.AsDefault();

        result.Should().BeOfType<DefaultInboxBuilder>();
    }

    [Fact]
    public void AsBatched_ReturnsBatchedInboxBuilder()
    {
        var builder = CreateBuilder();

        var result = builder.AsBatched();

        result.Should().BeOfType<BatchedInboxBuilder>();
    }

    [Fact]
    public void AsFifo_ReturnsFifoInboxBuilder()
    {
        var builder = CreateBuilder();

        var result = builder.AsFifo();

        result.Should().BeOfType<FifoInboxBuilder>();
    }

    [Fact]
    public void AsFifoBatched_ReturnsFifoBatchedInboxBuilder()
    {
        var builder = CreateBuilder();

        var result = builder.AsFifoBatched();

        result.Should().BeOfType<FifoBatchedInboxBuilder>();
    }

    #endregion

    #region InboxName Tests

    [Fact]
    public void AsDefault_PreservesInboxName()
    {
        var builder = CreateBuilder("my-custom-inbox");

        var result = builder.AsDefault();

        result.InboxName.Should().Be("my-custom-inbox");
    }

    [Fact]
    public void AsBatched_PreservesInboxName()
    {
        var builder = CreateBuilder("batched-inbox");

        var result = builder.AsBatched();

        result.InboxName.Should().Be("batched-inbox");
    }

    [Fact]
    public void AsFifo_PreservesInboxName()
    {
        var builder = CreateBuilder("fifo-inbox");

        var result = builder.AsFifo();

        result.InboxName.Should().Be("fifo-inbox");
    }

    [Fact]
    public void AsFifoBatched_PreservesInboxName()
    {
        var builder = CreateBuilder("fifo-batched-inbox");

        var result = builder.AsFifoBatched();

        result.InboxName.Should().Be("fifo-batched-inbox");
    }

    #endregion
}
