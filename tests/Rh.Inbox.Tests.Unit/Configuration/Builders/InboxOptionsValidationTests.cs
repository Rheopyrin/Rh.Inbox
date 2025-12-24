using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Configuration.Builders;
using Rh.Inbox.Exceptions;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Configuration.Builders;

public class InboxOptionsValidationTests
{
    private readonly IServiceCollection _services;

    public InboxOptionsValidationTests()
    {
        _services = new ServiceCollection();
    }

    private ITypedInboxBuilder CreateBuilder(Action<IConfigureInboxOptions>? configure = null)
    {
        var storageFactory = Substitute.For<IInboxStorageProviderFactory>();
        var handler = Substitute.For<IInboxHandler<TestMessage>>();

        var builder = new InboxBuilder(_services, "test-inbox")
            .AsDefault()
            .UseStorageProviderFactory(storageFactory)
            .RegisterHandler(handler);

        if (configure != null)
        {
            builder.ConfigureOptions(configure);
        }

        return (ITypedInboxBuilder)builder;
    }

    public class TestMessage { }

    #region DeadLetterMaxMessageLifetime Validation Tests

    [Fact]
    public void Build_WithDeadLetterEnabledAndNegativeLifetime_ThrowsInvalidInboxConfigurationException()
    {
        var builder = CreateBuilder(o =>
        {
            o.EnableDeadLetter = true;
            o.DeadLetterMaxMessageLifetime = TimeSpan.FromMinutes(-1);
        });

        var act = () => builder.Build();

        act.Should().Throw<InvalidInboxConfigurationException>()
            .WithMessage("*DeadLetterMaxMessageLifetime*cannot be negative*");
    }

    [Fact]
    public void Build_WithDeadLetterDisabledAndNegativeLifetime_DoesNotThrow()
    {
        var builder = CreateBuilder(o =>
        {
            o.EnableDeadLetter = false;
            o.DeadLetterMaxMessageLifetime = TimeSpan.FromMinutes(-1);
        });

        var act = () => builder.Build();

        act.Should().NotThrow<InvalidInboxConfigurationException>();
    }

    [Fact]
    public void Build_WithDeadLetterEnabledAndZeroLifetime_DoesNotThrow()
    {
        var builder = CreateBuilder(o =>
        {
            o.EnableDeadLetter = true;
            o.DeadLetterMaxMessageLifetime = TimeSpan.Zero;
        });

        var act = () => builder.Build();

        act.Should().NotThrow<InvalidInboxConfigurationException>();
    }

    [Theory]
    [InlineData(1)] // 1 millisecond
    [InlineData(1000)] // 1 second
    [InlineData(60000)] // 1 minute
    [InlineData(86400000)] // 1 day
    public void Build_WithDeadLetterEnabledAndPositiveLifetime_DoesNotThrow(int milliseconds)
    {
        var builder = CreateBuilder(o =>
        {
            o.EnableDeadLetter = true;
            o.DeadLetterMaxMessageLifetime = TimeSpan.FromMilliseconds(milliseconds);
        });

        var act = () => builder.Build();

        act.Should().NotThrow<InvalidInboxConfigurationException>();
    }

    #endregion

    #region DeduplicationInterval Validation Tests

    [Fact]
    public void Build_WithDeduplicationEnabledAndNegativeInterval_ThrowsInvalidInboxConfigurationException()
    {
        var builder = CreateBuilder(o =>
        {
            o.EnableDeduplication = true;
            o.DeduplicationInterval = TimeSpan.FromMinutes(-1);
        });

        var act = () => builder.Build();

        act.Should().Throw<InvalidInboxConfigurationException>()
            .WithMessage("*DeduplicationInterval*cannot be negative*");
    }

    [Fact]
    public void Build_WithDeduplicationDisabledAndNegativeInterval_DoesNotThrow()
    {
        var builder = CreateBuilder(o =>
        {
            o.EnableDeduplication = false;
            o.DeduplicationInterval = TimeSpan.FromMinutes(-1);
        });

        var act = () => builder.Build();

        act.Should().NotThrow<InvalidInboxConfigurationException>();
    }

    [Fact]
    public void Build_WithDeduplicationEnabledAndZeroInterval_DoesNotThrow()
    {
        var builder = CreateBuilder(o =>
        {
            o.EnableDeduplication = true;
            o.DeduplicationInterval = TimeSpan.Zero;
        });

        var act = () => builder.Build();

        act.Should().NotThrow<InvalidInboxConfigurationException>();
    }

    [Theory]
    [InlineData(1)] // 1 millisecond
    [InlineData(1000)] // 1 second
    [InlineData(60000)] // 1 minute
    [InlineData(3600000)] // 1 hour
    public void Build_WithDeduplicationEnabledAndPositiveInterval_DoesNotThrow(int milliseconds)
    {
        var builder = CreateBuilder(o =>
        {
            o.EnableDeduplication = true;
            o.DeduplicationInterval = TimeSpan.FromMilliseconds(milliseconds);
        });

        var act = () => builder.Build();

        act.Should().NotThrow<InvalidInboxConfigurationException>();
    }

    #endregion

    #region EnableDeduplication Tests

    [Fact]
    public void Build_WithEnableDeduplicationFalse_DoesNotValidateInterval()
    {
        // When deduplication is disabled, interval validation is skipped
        var builder = CreateBuilder(o =>
        {
            o.EnableDeduplication = false;
            o.DeduplicationInterval = TimeSpan.FromMinutes(-100);
        });

        var act = () => builder.Build();

        act.Should().NotThrow<InvalidInboxConfigurationException>();
    }

    [Fact]
    public void Build_WithEnableDeduplicationTrue_ValidatesInterval()
    {
        // When deduplication is enabled, negative interval should throw
        var builder = CreateBuilder(o =>
        {
            o.EnableDeduplication = true;
            o.DeduplicationInterval = TimeSpan.FromMinutes(-1);
        });

        var act = () => builder.Build();

        act.Should().Throw<InvalidInboxConfigurationException>()
            .WithMessage("*DeduplicationInterval*");
    }

    #endregion
}
