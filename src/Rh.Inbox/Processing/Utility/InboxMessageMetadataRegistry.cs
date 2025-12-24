using System.Collections.Concurrent;
using System.Reflection;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Messages;

namespace Rh.Inbox.Processing.Utility;

internal sealed class InboxMessageMetadataRegistry : IInboxMessageMetadataRegistry
{
    private readonly ConcurrentDictionary<Type, string> _typeToMessageType = new();
    private readonly ConcurrentDictionary<string, Type> _messageTypeToClrType = new();

    public bool HasRegisteredMessages => !_typeToMessageType.IsEmpty;

    public void Register<TMessage>(string? messageType = null)
    {
        Register(typeof(TMessage), messageType);
    }

    public void Register(Type messageType, string? customMessageType = null)
    {
        var resolvedMessageType = ResolveMessageType(messageType, customMessageType);

        if (!_typeToMessageType.TryAdd(messageType, resolvedMessageType))
        {
            throw new InvalidOperationException(
                $"Message type {messageType.FullName} is already registered.");
        }

        if (!_messageTypeToClrType.TryAdd(resolvedMessageType, messageType))
        {
            throw new InvalidOperationException(
                $"Message type name '{resolvedMessageType}' is already registered for {_messageTypeToClrType[resolvedMessageType].FullName}.");
        }
    }

    public string GetMessageType<TMessage>()
    {
        return GetMessageType(typeof(TMessage));
    }

    public string GetMessageType(Type messageType)
    {
        if (_typeToMessageType.TryGetValue(messageType, out var typeName))
        {
            return typeName;
        }

        throw new InvalidOperationException(
            $"Message type {messageType.FullName} is not registered. Call Register<{messageType.Name}>() first.");
    }

    public Type? GetClrType(string messageType)
    {
        _messageTypeToClrType.TryGetValue(messageType, out var clrType);
        return clrType;
    }

    public IEnumerable<Type> GetAllMessageTypes()
    {
        return _typeToMessageType.Keys;
    }

    private static string ResolveMessageType(Type messageType, string? customMessageType)
    {
        if (!string.IsNullOrWhiteSpace(customMessageType))
        {
            return customMessageType;
        }

        var attribute = messageType.GetCustomAttribute<InboxMessageAttribute>();
        if (!string.IsNullOrWhiteSpace(attribute?.MessageType))
        {
            return attribute.MessageType;
        }

        return messageType.FullName ?? messageType.Name;
    }
}