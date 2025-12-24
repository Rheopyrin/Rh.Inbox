using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Exceptions;

namespace Rh.Inbox.Inboxes.Implementation;

internal class FifoInbox : InboxBase
{
    public override InboxType Type => InboxType.Fifo;

    public FifoInbox(
        IInboxConfiguration configuration,
        IInboxStorageProvider storageProvider,
        IInboxMessagePayloadSerializer serializer,
        IDateTimeProvider dateTimeProvider)
        : base(configuration, storageProvider, serializer, dateTimeProvider)
    {
    }

    internal override InboxMessage CreateInboxMessage<TMessage>(TMessage message)
    {
        var inboxMessage = base.CreateInboxMessage(message);

        return string.IsNullOrWhiteSpace(inboxMessage.GroupId)
            ? throw new InvalidInboxMessageException(Configuration.InboxName, "Message must have a GroupId set when using in the fifo inbox.")
            : inboxMessage;
    }
}