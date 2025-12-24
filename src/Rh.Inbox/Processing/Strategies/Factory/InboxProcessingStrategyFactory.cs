using Microsoft.Extensions.Logging;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Inboxes;
using Rh.Inbox.Processing.Strategies.Implementation;

namespace Rh.Inbox.Processing.Strategies.Factory;

internal class InboxProcessingStrategyFactory : IInboxProcessingStrategyFactory
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<InboxProcessingStrategyFactory> _logger;

    public InboxProcessingStrategyFactory(IServiceProvider serviceProvider, ILogger<InboxProcessingStrategyFactory> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    public IInboxProcessingStrategy Create(InboxBase inbox)
    {
        return inbox.Type switch
        {
            InboxType.Default => new DefaultInboxProcessingStrategy(inbox, _serviceProvider, _logger),
            InboxType.Batched => new BatchedInboxProcessingStrategy(inbox, _serviceProvider, _logger),
            InboxType.Fifo => new FifoInboxProcessingStrategy(inbox, _serviceProvider, _logger),
            InboxType.FifoBatched => new FifoBatchedInboxProcessingStrategy(inbox, _serviceProvider, _logger),
            _ => throw new InvalidOperationException($"Unknown inbox type: {inbox.Type}")
        };
    }
}