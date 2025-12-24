using FluentAssertions;
using Rh.Inbox.Configuration;
using Rh.Inbox.Configuration.Registry;
using Rh.Inbox.Tests.Unit.TestHelpers;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Configuration.Registry;

public class InboxConfigurationRegistryTests
{
    private readonly InboxConfigurationRegistry _registry = new();

    private InboxConfiguration CreateConfiguration(string inboxName)
    {
        return TestConfigurationFactory.CreateConfiguration(inboxName);
    }

    #region Register Tests

    [Fact]
    public void Register_ValidConfiguration_Succeeds()
    {
        var config = CreateConfiguration("test-inbox");

        var act = () => _registry.Register(config);

        act.Should().NotThrow();
    }

    [Fact]
    public void Register_DuplicateName_ThrowsInvalidOperationException()
    {
        var config1 = CreateConfiguration("duplicate");
        var config2 = CreateConfiguration("duplicate");
        _registry.Register(config1);

        var act = () => _registry.Register(config2);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*'duplicate'*already registered*");
    }

    [Fact]
    public void Register_MultipleUniqueConfigurations_Succeeds()
    {
        var config1 = CreateConfiguration("inbox-1");
        var config2 = CreateConfiguration("inbox-2");
        var config3 = CreateConfiguration("inbox-3");

        var act = () =>
        {
            _registry.Register(config1);
            _registry.Register(config2);
            _registry.Register(config3);
        };

        act.Should().NotThrow();
    }

    #endregion

    #region Get Tests

    [Fact]
    public void Get_RegisteredInbox_ReturnsConfiguration()
    {
        var config = CreateConfiguration("my-inbox");
        _registry.Register(config);

        var result = _registry.Get("my-inbox");

        result.Should().BeSameAs(config);
    }

    [Fact]
    public void Get_UnregisteredInbox_ThrowsInvalidOperationException()
    {
        var act = () => _registry.Get("non-existent");

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*'non-existent'*not registered*");
    }

    [Fact]
    public void Get_AfterMultipleRegistrations_ReturnsCorrectConfiguration()
    {
        var config1 = CreateConfiguration("inbox-1");
        var config2 = CreateConfiguration("inbox-2");
        _registry.Register(config1);
        _registry.Register(config2);

        var result1 = _registry.Get("inbox-1");
        var result2 = _registry.Get("inbox-2");

        result1.Should().BeSameAs(config1);
        result2.Should().BeSameAs(config2);
    }

    #endregion

    #region GetDefault Tests

    [Fact]
    public void GetDefault_WhenDefaultRegistered_ReturnsDefaultConfiguration()
    {
        var config = CreateConfiguration(InboxOptions.DefaultInboxName);
        _registry.Register(config);

        var result = _registry.GetDefault();

        result.Should().BeSameAs(config);
    }

    [Fact]
    public void GetDefault_WhenDefaultNotRegistered_ThrowsInvalidOperationException()
    {
        var act = () => _registry.GetDefault();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage($"*'{InboxOptions.DefaultInboxName}'*not registered*");
    }

    #endregion

    #region TryGet Tests

    [Fact]
    public void TryGet_RegisteredInbox_ReturnsTrueAndConfiguration()
    {
        var config = CreateConfiguration("test-inbox");
        _registry.Register(config);

        var result = _registry.TryGet("test-inbox", out var configuration);

        result.Should().BeTrue();
        configuration.Should().BeSameAs(config);
    }

    [Fact]
    public void TryGet_UnregisteredInbox_ReturnsFalseAndNull()
    {
        var result = _registry.TryGet("non-existent", out var configuration);

        result.Should().BeFalse();
        configuration.Should().BeNull();
    }

    #endregion

    #region GetAll Tests

    [Fact]
    public void GetAll_EmptyRegistry_ReturnsEmptyCollection()
    {
        var result = _registry.GetAll();

        result.Should().BeEmpty();
    }

    [Fact]
    public void GetAll_WithRegistrations_ReturnsAllConfigurations()
    {
        var config1 = CreateConfiguration("inbox-1");
        var config2 = CreateConfiguration("inbox-2");
        var config3 = CreateConfiguration("inbox-3");
        _registry.Register(config1);
        _registry.Register(config2);
        _registry.Register(config3);

        var result = _registry.GetAll().ToList();

        result.Should().HaveCount(3);
        result.Should().Contain(config1);
        result.Should().Contain(config2);
        result.Should().Contain(config3);
    }

    #endregion
}
