using Rh.Inbox.Inboxes;

namespace Rh.Inbox.Processing.Strategies.Factory;

internal interface IInboxProcessingStrategyFactory
{
    IInboxProcessingStrategy Create(InboxBase inbox);
}