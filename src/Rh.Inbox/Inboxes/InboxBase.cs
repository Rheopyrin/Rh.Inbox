using Rh.Inbox.Abstractions;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;

namespace Rh.Inbox.Inboxes;

internal abstract class InboxBase : IInbox
{
    protected readonly IInboxConfiguration Configuration;
    protected readonly IInboxStorageProvider StorageProvider;
    protected readonly IInboxMessagePayloadSerializer Serializer;
    private readonly IDateTimeProvider _dateTimeProvider;

    public string Name => Configuration.InboxName;
    public abstract InboxType Type { get; }

    protected InboxBase(
        IInboxConfiguration configuration,
        IInboxStorageProvider storageProvider,
        IInboxMessagePayloadSerializer serializer,
        IDateTimeProvider dateTimeProvider)
    {
        Configuration = configuration;
        StorageProvider = storageProvider;
        Serializer = serializer;
        _dateTimeProvider = dateTimeProvider;
    }

    internal IInboxStorageProvider GetStorageProvider() => StorageProvider;

    internal IInboxConfiguration GetConfiguration() => Configuration;

    internal IInboxMessagePayloadSerializer GetSerializer() => Serializer;

    internal virtual InboxMessage CreateInboxMessage<TMessage>(TMessage message) where TMessage : class
    {
        var id = message is IHasExternalId hasId ? hasId.GetId() : Guid.NewGuid();
        var groupId = message is IHasGroupId hasGroupId ? hasGroupId.GetGroupId() : null;
        var collapseKey = message is IHasCollapseKey hasCollapseKey ? hasCollapseKey.GetCollapseKey() : null;
        var deduplicationId = message is IHasDeduplicationId hasDeduplicationId ? hasDeduplicationId.GetDeduplicationId() : null;
        var receivedAt = message is IHasReceivedAt hasReceivedAt ? hasReceivedAt.GetReceivedAt() : _dateTimeProvider.GetUtcNow();
        var messageType = Configuration.MetadataRegistry.GetMessageType<TMessage>();

        return new InboxMessage
        {
            Id = id,
            MessageType = messageType,
            Payload = Serializer.Serialize(message),
            GroupId = groupId,
            CollapseKey = collapseKey,
            DeduplicationId = deduplicationId,
            AttemptsCount = 0,
            ReceivedAt = receivedAt
        };
    }
}