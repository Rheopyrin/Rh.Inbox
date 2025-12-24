using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;

namespace Rh.Inbox.Inboxes.Implementation;

internal sealed class DefaultInbox : InboxBase
{
    public override InboxType Type => InboxType.Default;

    public DefaultInbox(
        IInboxConfiguration configuration,
        IInboxStorageProvider storageProvider,
        IInboxMessagePayloadSerializer serializer,
        IDateTimeProvider dateTimeProvider)
        : base(configuration, storageProvider, serializer, dateTimeProvider)
    {
    }
}