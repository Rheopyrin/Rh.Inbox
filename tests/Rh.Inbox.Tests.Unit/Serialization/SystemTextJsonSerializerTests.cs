using System.Text.Json;
using FluentAssertions;
using Rh.Inbox.Serialization;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Serialization;

public class SystemTextJsonSerializerTests
{
    #region SystemTextJsonInboxMessagePayloadSerializer Tests

    [Fact]
    public void Serializer_DefaultConstructor_UsesCamelCase()
    {
        var serializer = new SystemTextJsonInboxMessagePayloadSerializer();
        var message = new TestMessage { MessageId = 123, MessageText = "Hello" };

        var json = serializer.Serialize(message);

        json.Should().Contain("messageId");
        json.Should().Contain("messageText");
        json.Should().NotContain("MessageId");
    }

    [Fact]
    public void Serializer_CustomOptions_UsesProvidedOptions()
    {
        var options = new JsonSerializerOptions { PropertyNamingPolicy = null };
        var serializer = new SystemTextJsonInboxMessagePayloadSerializer(options);
        var message = new TestMessage { MessageId = 123, MessageText = "Hello" };

        var json = serializer.Serialize(message);

        json.Should().Contain("MessageId");
        json.Should().Contain("MessageText");
    }

    [Fact]
    public void Serialize_GenericMethod_SerializesCorrectly()
    {
        var serializer = new SystemTextJsonInboxMessagePayloadSerializer();
        var message = new TestMessage { MessageId = 42, MessageText = "Test" };

        var json = serializer.Serialize(message);

        json.Should().Contain("42");
        json.Should().Contain("Test");
    }

    [Fact]
    public void Serialize_WithType_SerializesCorrectly()
    {
        var serializer = new SystemTextJsonInboxMessagePayloadSerializer();
        object message = new TestMessage { MessageId = 99, MessageText = "Typed" };

        var json = serializer.Serialize(message, typeof(TestMessage));

        json.Should().Contain("99");
        json.Should().Contain("Typed");
    }

    [Fact]
    public void Deserialize_GenericMethod_DeserializesCorrectly()
    {
        var serializer = new SystemTextJsonInboxMessagePayloadSerializer();
        var json = """{"messageId":123,"messageText":"Hello"}""";

        var result = serializer.Deserialize<TestMessage>(json);

        result.Should().NotBeNull();
        result!.MessageId.Should().Be(123);
        result.MessageText.Should().Be("Hello");
    }

    [Fact]
    public void Deserialize_WithType_DeserializesCorrectly()
    {
        var serializer = new SystemTextJsonInboxMessagePayloadSerializer();
        var json = """{"messageId":456,"messageText":"World"}""";

        var result = serializer.Deserialize(json, typeof(TestMessage)) as TestMessage;

        result.Should().NotBeNull();
        result!.MessageId.Should().Be(456);
        result.MessageText.Should().Be("World");
    }

    [Fact]
    public void Serialize_Deserialize_RoundTrip_PreservesData()
    {
        var serializer = new SystemTextJsonInboxMessagePayloadSerializer();
        var original = new TestMessage { MessageId = 789, MessageText = "Round trip" };

        var json = serializer.Serialize(original);
        var deserialized = serializer.Deserialize<TestMessage>(json);

        deserialized.Should().BeEquivalentTo(original);
    }

    [Fact]
    public void Deserialize_NullJson_ReturnsNull()
    {
        var serializer = new SystemTextJsonInboxMessagePayloadSerializer();

        var result = serializer.Deserialize<TestMessage>("null");

        result.Should().BeNull();
    }

    [Fact]
    public void Serialize_ComplexObject_SerializesNestedProperties()
    {
        var serializer = new SystemTextJsonInboxMessagePayloadSerializer();
        var message = new ComplexMessage
        {
            Id = 1,
            Nested = new NestedData { Value = "nested value" },
            Items = new List<string> { "a", "b", "c" }
        };

        var json = serializer.Serialize(message);

        json.Should().Contain("nested value");
        json.Should().Contain("\"a\"");
    }

    #endregion

    #region SystemTextJsonInboxSerializerFactory Tests

    [Fact]
    public void Factory_DefaultConstructor_CreatesSerializer()
    {
        var factory = new SystemTextJsonInboxSerializerFactory();

        var serializer = factory.Create("test-inbox");

        serializer.Should().NotBeNull();
        serializer.Should().BeOfType<SystemTextJsonInboxMessagePayloadSerializer>();
    }

    [Fact]
    public void Factory_CustomOptions_CreatesSerializerWithOptions()
    {
        var options = new JsonSerializerOptions { PropertyNamingPolicy = null };
        var factory = new SystemTextJsonInboxSerializerFactory(options);

        var serializer = factory.Create("test-inbox");
        var json = serializer.Serialize(new TestMessage { MessageId = 1, MessageText = "Test" });

        json.Should().Contain("MessageId"); // PascalCase because we set PropertyNamingPolicy to null
    }

    [Fact]
    public void Factory_Create_ReturnsNewInstanceEachTime()
    {
        var factory = new SystemTextJsonInboxSerializerFactory();

        var serializer1 = factory.Create("inbox-1");
        var serializer2 = factory.Create("inbox-2");

        serializer1.Should().NotBeSameAs(serializer2);
    }

    [Fact]
    public void Factory_Create_IgnoresInboxName()
    {
        var factory = new SystemTextJsonInboxSerializerFactory();
        var message = new TestMessage { MessageId = 1, MessageText = "Test" };

        var serializer1 = factory.Create("inbox-1");
        var serializer2 = factory.Create("inbox-2");
        var json1 = serializer1.Serialize(message);
        var json2 = serializer2.Serialize(message);

        json1.Should().Be(json2);
    }

    #endregion

    #region Test Helper Classes

    private class TestMessage
    {
        public int MessageId { get; set; }
        public string MessageText { get; set; } = string.Empty;
    }

    private class ComplexMessage
    {
        public int Id { get; set; }
        public NestedData? Nested { get; set; }
        public List<string> Items { get; set; } = new();
    }

    private class NestedData
    {
        public string Value { get; set; } = string.Empty;
    }

    #endregion
}
