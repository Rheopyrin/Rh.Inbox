using Rh.Inbox.Abstractions.Configuration;

namespace Rh.Inbox.Inboxes.Factory;

internal interface IInboxFactory
{
    InboxBase Create(IInboxConfiguration configuration);
}