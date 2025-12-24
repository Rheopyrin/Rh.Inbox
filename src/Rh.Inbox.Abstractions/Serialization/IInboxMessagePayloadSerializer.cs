namespace Rh.Inbox.Abstractions.Serialization;

/// <summary>
/// Serializer for inbox message payloads.
/// </summary>
public interface IInboxMessagePayloadSerializer
{
    /// <summary>
    /// Serializes a message to a string.
    /// </summary>
    /// <typeparam name="T">The type of message to serialize.</typeparam>
    /// <param name="message">The message to serialize.</param>
    /// <returns>The serialized message as a string.</returns>
    string Serialize<T>(T message);

    /// <summary>
    /// Serializes a message to a string using the specified type.
    /// </summary>
    /// <param name="message">The message to serialize.</param>
    /// <param name="type">The type of the message.</param>
    /// <returns>The serialized message as a string.</returns>
    string Serialize(object message, Type type);

    /// <summary>
    /// Deserializes a string payload to a message.
    /// </summary>
    /// <typeparam name="T">The type of message to deserialize to.</typeparam>
    /// <param name="payload">The serialized payload.</param>
    /// <returns>The deserialized message, or null if deserialization fails.</returns>
    T? Deserialize<T>(string payload);

    /// <summary>
    /// Deserializes a string payload to a message using the specified type.
    /// </summary>
    /// <param name="payload">The serialized payload.</param>
    /// <param name="type">The type to deserialize to.</param>
    /// <returns>The deserialized message, or null if deserialization fails.</returns>
    object? Deserialize(string payload, Type type);
}