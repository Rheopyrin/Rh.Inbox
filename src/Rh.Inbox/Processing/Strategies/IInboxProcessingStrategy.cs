using Rh.Inbox.Abstractions.Messages;

namespace Rh.Inbox.Processing.Strategies;

internal interface IInboxProcessingStrategy
{
    Task ProcessAsync(string processorId, IReadOnlyList<InboxMessage> messages, CancellationToken token);
}