using FluentAssertions;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Processing.Utility;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Processing.Utility;

public class InboxMessageMetadataRegistryTests
{
    #region Register Tests

    [Fact]
    public void Register_WithDefaultMessageType_UsesFullTypeName()
    {
        var registry = new InboxMessageMetadataRegistry();

        registry.Register<SimpleMessage>();

        var messageType = registry.GetMessageType<SimpleMessage>();
        messageType.Should().Be(typeof(SimpleMessage).FullName);
    }

    [Fact]
    public void Register_WithCustomMessageType_UsesCustomName()
    {
        var registry = new InboxMessageMetadataRegistry();
        const string customType = "custom.message.type";

        registry.Register<SimpleMessage>(customType);

        var messageType = registry.GetMessageType<SimpleMessage>();
        messageType.Should().Be(customType);
    }

    [Fact]
    public void Register_WithAttributeMessageType_UsesAttributeName()
    {
        var registry = new InboxMessageMetadataRegistry();

        registry.Register<MessageWithAttribute>();

        var messageType = registry.GetMessageType<MessageWithAttribute>();
        messageType.Should().Be("attributed-message");
    }

    [Fact]
    public void Register_CustomTypeOverridesAttribute()
    {
        var registry = new InboxMessageMetadataRegistry();
        const string customType = "override-type";

        registry.Register<MessageWithAttribute>(customType);

        var messageType = registry.GetMessageType<MessageWithAttribute>();
        messageType.Should().Be(customType);
    }

    [Fact]
    public void Register_DuplicateType_ThrowsInvalidOperationException()
    {
        var registry = new InboxMessageMetadataRegistry();
        registry.Register<SimpleMessage>();

        var act = () => registry.Register<SimpleMessage>();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*already registered*");
    }

    [Fact]
    public void Register_DuplicateMessageTypeName_ThrowsInvalidOperationException()
    {
        var registry = new InboxMessageMetadataRegistry();
        const string sharedTypeName = "shared-type";

        registry.Register<SimpleMessage>(sharedTypeName);
        var act = () => registry.Register<AnotherMessage>(sharedTypeName);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*'shared-type' is already registered*");
    }

    [Fact]
    public void Register_NonGenericMethod_WorksCorrectly()
    {
        var registry = new InboxMessageMetadataRegistry();

        registry.Register(typeof(SimpleMessage), "non-generic-type");

        var messageType = registry.GetMessageType(typeof(SimpleMessage));
        messageType.Should().Be("non-generic-type");
    }

    [Fact]
    public void Register_MultipleTypes_AllRegisteredCorrectly()
    {
        var registry = new InboxMessageMetadataRegistry();

        registry.Register<SimpleMessage>("type-1");
        registry.Register<AnotherMessage>("type-2");
        registry.Register<MessageWithAttribute>();

        registry.GetMessageType<SimpleMessage>().Should().Be("type-1");
        registry.GetMessageType<AnotherMessage>().Should().Be("type-2");
        registry.GetMessageType<MessageWithAttribute>().Should().Be("attributed-message");
    }

    #endregion

    #region GetMessageType Tests

    [Fact]
    public void GetMessageType_RegisteredType_ReturnsMessageType()
    {
        var registry = new InboxMessageMetadataRegistry();
        registry.Register<SimpleMessage>("test-type");

        var result = registry.GetMessageType<SimpleMessage>();

        result.Should().Be("test-type");
    }

    [Fact]
    public void GetMessageType_UnregisteredType_ThrowsInvalidOperationException()
    {
        var registry = new InboxMessageMetadataRegistry();

        var act = () => registry.GetMessageType<SimpleMessage>();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*not registered*");
    }

    [Fact]
    public void GetMessageType_NonGenericMethod_WorksCorrectly()
    {
        var registry = new InboxMessageMetadataRegistry();
        registry.Register(typeof(SimpleMessage), "test-type");

        var result = registry.GetMessageType(typeof(SimpleMessage));

        result.Should().Be("test-type");
    }

    #endregion

    #region GetClrType Tests

    [Fact]
    public void GetClrType_RegisteredMessageType_ReturnsClrType()
    {
        var registry = new InboxMessageMetadataRegistry();
        registry.Register<SimpleMessage>("test-type");

        var result = registry.GetClrType("test-type");

        result.Should().Be(typeof(SimpleMessage));
    }

    [Fact]
    public void GetClrType_UnregisteredMessageType_ReturnsNull()
    {
        var registry = new InboxMessageMetadataRegistry();

        var result = registry.GetClrType("unknown-type");

        result.Should().BeNull();
    }

    [Fact]
    public void GetClrType_AfterMultipleRegistrations_ReturnsCorrectType()
    {
        var registry = new InboxMessageMetadataRegistry();
        registry.Register<SimpleMessage>("type-1");
        registry.Register<AnotherMessage>("type-2");

        registry.GetClrType("type-1").Should().Be(typeof(SimpleMessage));
        registry.GetClrType("type-2").Should().Be(typeof(AnotherMessage));
    }

    #endregion

    #region GetAllMessageTypes Tests

    [Fact]
    public void GetAllMessageTypes_EmptyRegistry_ReturnsEmpty()
    {
        var registry = new InboxMessageMetadataRegistry();

        var result = registry.GetAllMessageTypes();

        result.Should().BeEmpty();
    }

    [Fact]
    public void GetAllMessageTypes_WithRegistrations_ReturnsAllTypes()
    {
        var registry = new InboxMessageMetadataRegistry();
        registry.Register<SimpleMessage>();
        registry.Register<AnotherMessage>();
        registry.Register<MessageWithAttribute>();

        var result = registry.GetAllMessageTypes().ToList();

        result.Should().HaveCount(3);
        result.Should().Contain(typeof(SimpleMessage));
        result.Should().Contain(typeof(AnotherMessage));
        result.Should().Contain(typeof(MessageWithAttribute));
    }

    #endregion

    #region Thread Safety Tests

    [Fact]
    public async Task Register_ConcurrentRegistrations_ThreadSafe()
    {
        var registry = new InboxMessageMetadataRegistry();
        var types = new[]
        {
            (typeof(Type1), "type-1"),
            (typeof(Type2), "type-2"),
            (typeof(Type3), "type-3"),
            (typeof(Type4), "type-4"),
            (typeof(Type5), "type-5")
        };

        var tasks = types.Select(t => Task.Run(() => registry.Register(t.Item1, t.Item2)));

        await Task.WhenAll(tasks);

        registry.GetAllMessageTypes().Should().HaveCount(5);
        foreach (var (type, name) in types)
        {
            registry.GetMessageType(type).Should().Be(name);
            registry.GetClrType(name).Should().Be(type);
        }
    }

    [Fact]
    public async Task GetMessageType_ConcurrentReads_ThreadSafe()
    {
        var registry = new InboxMessageMetadataRegistry();
        registry.Register<SimpleMessage>("test-type");

        var tasks = Enumerable.Range(0, 100)
            .Select(_ => Task.Run(() => registry.GetMessageType<SimpleMessage>()));

        var results = await Task.WhenAll(tasks);

        results.Should().AllBe("test-type");
    }

    #endregion

    #region Test Message Types

    private class SimpleMessage { }

    private class AnotherMessage { }

    [InboxMessage(MessageType = "attributed-message")]
    private class MessageWithAttribute { }

    private class Type1 { }
    private class Type2 { }
    private class Type3 { }
    private class Type4 { }
    private class Type5 { }

    #endregion
}
