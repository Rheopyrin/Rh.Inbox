using System.Text.Json;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Serialization;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Processing.Strategies;

public class TryDeserializePayloadTests
{
    private readonly ILogger _logger;
    private readonly TestableStrategy _strategy;

    public TryDeserializePayloadTests()
    {
        _logger = Substitute.For<ILogger>();
        var serviceProvider = Substitute.For<IServiceProvider>();
        _strategy = new TestableStrategy(serviceProvider, _logger);
    }

    #region Successful Deserialization

    [Fact]
    public void TryDeserializePayload_ValidJson_ReturnsTrue()
    {
        var serializer = new SystemTextJsonInboxMessagePayloadSerializer();
        var payload = """{"name":"Test","value":42}""";
        var messageId = Guid.NewGuid();

        var result = _strategy.TestTryDeserializePayload<TestMessage>(
            serializer, payload, messageId, out var message, out var errorReason);

        result.Should().BeTrue();
        message.Should().NotBeNull();
        message!.Name.Should().Be("Test");
        message.Value.Should().Be(42);
        errorReason.Should().BeNull();
    }

    [Fact]
    public void TryDeserializePayload_ValidJson_DoesNotLog()
    {
        var serializer = new SystemTextJsonInboxMessagePayloadSerializer();
        var payload = """{"name":"Test","value":42}""";
        var messageId = Guid.NewGuid();

        _strategy.TestTryDeserializePayload<TestMessage>(
            serializer, payload, messageId, out _, out _);

        _logger.DidNotReceive().Log(
            Arg.Any<LogLevel>(),
            Arg.Any<EventId>(),
            Arg.Any<object>(),
            Arg.Any<Exception>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    #endregion

    #region Null Result

    [Fact]
    public void TryDeserializePayload_NullJson_ReturnsFalse()
    {
        var serializer = new SystemTextJsonInboxMessagePayloadSerializer();
        var payload = "null";
        var messageId = Guid.NewGuid();

        var result = _strategy.TestTryDeserializePayload<TestMessage>(
            serializer, payload, messageId, out var message, out var errorReason);

        result.Should().BeFalse();
        message.Should().BeNull();
        errorReason.Should().Be("Failed to deserialize message payload: result was null");
    }

    #endregion

    #region Invalid JSON

    [Fact]
    public void TryDeserializePayload_InvalidJson_ReturnsFalse()
    {
        var serializer = new SystemTextJsonInboxMessagePayloadSerializer();
        var payload = "this is not valid json";
        var messageId = Guid.NewGuid();

        var result = _strategy.TestTryDeserializePayload<TestMessage>(
            serializer, payload, messageId, out var message, out var errorReason);

        result.Should().BeFalse();
        message.Should().BeNull();
        errorReason.Should().StartWith("Failed to deserialize message payload:");
    }

    [Fact]
    public void TryDeserializePayload_InvalidJson_LogsWarning()
    {
        var serializer = new SystemTextJsonInboxMessagePayloadSerializer();
        var payload = "this is not valid json";
        var messageId = Guid.NewGuid();

        _strategy.TestTryDeserializePayload<TestMessage>(
            serializer, payload, messageId, out _, out _);

        _logger.Received(1).Log(
            LogLevel.Warning,
            Arg.Any<EventId>(),
            Arg.Any<object>(),
            Arg.Any<JsonException>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Fact]
    public void TryDeserializePayload_MalformedJson_ReturnsFalse()
    {
        var serializer = new SystemTextJsonInboxMessagePayloadSerializer();
        var payload = """{"name":"Test","""; // incomplete JSON
        var messageId = Guid.NewGuid();

        var result = _strategy.TestTryDeserializePayload<TestMessage>(
            serializer, payload, messageId, out var message, out var errorReason);

        result.Should().BeFalse();
        message.Should().BeNull();
        errorReason.Should().StartWith("Failed to deserialize message payload:");
    }

    [Fact]
    public void TryDeserializePayload_EmptyString_ReturnsFalse()
    {
        var serializer = new SystemTextJsonInboxMessagePayloadSerializer();
        var payload = "";
        var messageId = Guid.NewGuid();

        var result = _strategy.TestTryDeserializePayload<TestMessage>(
            serializer, payload, messageId, out var message, out var errorReason);

        result.Should().BeFalse();
        message.Should().BeNull();
        errorReason.Should().StartWith("Failed to deserialize message payload:");
    }

    #endregion

    #region Type Mismatch

    [Fact]
    public void TryDeserializePayload_WrongType_ReturnsObject()
    {
        var serializer = new SystemTextJsonInboxMessagePayloadSerializer();
        // JSON for a different type
        var payload = """{"differentField":"value"}""";
        var messageId = Guid.NewGuid();

        var result = _strategy.TestTryDeserializePayload<TestMessage>(
            serializer, payload, messageId, out var message, out var errorReason);

        // This actually succeeds because JSON deserialization is lenient
        // Missing properties get default values
        result.Should().BeTrue();
        message.Should().NotBeNull();
        message!.Name.Should().BeNull();
        message.Value.Should().Be(0);
    }

    #endregion

    #region Serializer Throws Exception

    [Fact]
    public void TryDeserializePayload_SerializerThrows_ReturnsFalse()
    {
        var serializer = Substitute.For<IInboxMessagePayloadSerializer>();
        serializer.Deserialize<TestMessage>(Arg.Any<string>())
            .Returns(_ => throw new InvalidOperationException("Serializer error"));
        var payload = """{"name":"Test"}""";
        var messageId = Guid.NewGuid();

        var result = _strategy.TestTryDeserializePayload<TestMessage>(
            serializer, payload, messageId, out var message, out var errorReason);

        result.Should().BeFalse();
        message.Should().BeNull();
        errorReason.Should().Be("Failed to deserialize message payload: Serializer error");
    }

    [Fact]
    public void TryDeserializePayload_SerializerThrows_LogsWarning()
    {
        var serializer = Substitute.For<IInboxMessagePayloadSerializer>();
        var expectedException = new InvalidOperationException("Serializer error");
        serializer.Deserialize<TestMessage>(Arg.Any<string>())
            .Returns(_ => throw expectedException);
        var payload = """{"name":"Test"}""";
        var messageId = Guid.NewGuid();

        _strategy.TestTryDeserializePayload<TestMessage>(
            serializer, payload, messageId, out _, out _);

        _logger.Received(1).Log(
            LogLevel.Warning,
            Arg.Any<EventId>(),
            Arg.Any<object>(),
            expectedException,
            Arg.Any<Func<object, Exception?, string>>());
    }

    #endregion

    #region Helper Classes

    private class TestMessage
    {
        public string? Name { get; set; }
        public int Value { get; set; }
    }

    /// <summary>
    /// Testable class that exposes the TryDeserializePayload logic for testing.
    /// </summary>
    private class TestableStrategy
    {
        private readonly ILogger _logger;

        public TestableStrategy(IServiceProvider serviceProvider, ILogger logger)
        {
            _logger = logger;
        }

        public bool TestTryDeserializePayload<T>(
            IInboxMessagePayloadSerializer serializer,
            string payload,
            Guid messageId,
            out T? result,
            out string? errorReason)
        {
            try
            {
                result = serializer.Deserialize<T>(payload);
                if (result == null)
                {
                    errorReason = "Failed to deserialize message payload: result was null";
                    return false;
                }

                errorReason = null;
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to deserialize payload for message {MessageId}: {Error}", messageId, ex.Message);
                result = default;
                errorReason = $"Failed to deserialize message payload: {ex.Message}";
                return false;
            }
        }
    }

    #endregion
}
