namespace Rh.Inbox.Abstractions.Configuration;

public interface IInboxMessageMetadataRegistry
{
    void Register<TMessage>(string? messageType = null);

    void Register(Type messageType, string? customMessageType = null);

    string GetMessageType<TMessage>();

    string GetMessageType(Type messageType);

    Type? GetClrType(string messageType);

    IEnumerable<Type> GetAllMessageTypes();

    bool HasRegisteredMessages { get; }
}