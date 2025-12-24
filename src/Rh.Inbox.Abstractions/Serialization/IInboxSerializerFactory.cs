namespace Rh.Inbox.Abstractions.Serialization;

/// <summary>
/// Factory for creating inbox message payload serializers.
/// Implement this interface to provide custom serialization for specific inboxes.
/// </summary>
public interface IInboxSerializerFactory
{
    /// <summary>
    /// Creates a serializer instance for the specified inbox.
    /// </summary>
    IInboxMessagePayloadSerializer Create(string inboxName);
}
