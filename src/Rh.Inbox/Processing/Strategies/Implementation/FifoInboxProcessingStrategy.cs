using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Inboxes;
using Rh.Inbox.Processing.Utility;

namespace Rh.Inbox.Processing.Strategies.Implementation;

internal sealed class FifoInboxProcessingStrategy : InboxProcessingStrategyBase
{
    private delegate Task ProcessMessageDelegate(
        InboxMessage message,
        IInboxMessagePayloadSerializer serializer,
        IInboxStorageProvider storageProvider,
        CancellationToken token);

    private readonly BoundedDelegateCache<ProcessMessageDelegate> _delegateCache;

    public FifoInboxProcessingStrategy(
        InboxBase inbox,
        IServiceProvider serviceProvider,
        ILogger logger)
        : base(inbox, serviceProvider, logger)
    {
        _delegateCache = new BoundedDelegateCache<ProcessMessageDelegate>(this, nameof(ProcessMessageAsync));
    }

    public override async Task ProcessAsync(string processorId, IReadOnlyList<InboxMessage> messages, CancellationToken token)
    {
        var configuration = GetConfiguration();
        var storageProvider = GetStorageProvider();
        var serializer = GetSerializer();
        var groupLocksProvider = storageProvider as ISupportGroupLocksReleaseStorageProvider;

        // Group by GroupId ONLY - strict FIFO per group
        var messagesByGroup = messages
            .GroupBy(m => m.GroupId ?? string.Empty)
            .ToList();

        // Process groups in parallel
        await ProcessInParallelAsync(messagesByGroup, async (group, ct) =>
        {
            var groupId = group.Key;

            try
            {
                // Sequential processing WITHIN the group - strict FIFO guarantee
                // Order preserved from storage - no explicit sorting needed
                foreach (var message in group)
                {
                    var messageType = configuration.MetadataRegistry.GetClrType(message.MessageType);
                    if (messageType == null)
                    {
                        Logger.LogWarning("Unknown message type: {MessageType}", message.MessageType);
                        await MoveToDeadLetterAsync(message, $"Unknown message type: {message.MessageType}", storageProvider, ct);
                        continue;
                    }

                    var processDelegate = _delegateCache.GetOrAdd(messageType);
                    await processDelegate(message, serializer, storageProvider, ct);
                }
            }
            finally
            {
                if (!string.IsNullOrEmpty(groupId) && groupLocksProvider != null)
                {
                    await groupLocksProvider.ReleaseGroupLocksAsync([groupId], CancellationToken.None);
                }
            }
        }, token);
    }

    private async Task ProcessMessageAsync<TMessage>(
        InboxMessage message,
        IInboxMessagePayloadSerializer serializer,
        IInboxStorageProvider storageProvider,
        CancellationToken token) where TMessage : class, IHasGroupId
    {
        var payload = serializer.Deserialize<TMessage>(message.Payload);
        if (payload == null)
        {
            await storageProvider.MoveToDeadLetterAsync(
                message.Id, "Failed to deserialize message payload", token);
            return;
        }

        using var scope = ServiceProvider.CreateScope();
        var handler = scope.ServiceProvider.GetKeyedService<IFifoInboxHandler<TMessage>>(Inbox.Name);

        if (handler == null)
        {
            Logger.LogWarning("No FIFO handler registered for message type: {MessageType}", typeof(TMessage).FullName);
            await storageProvider.MoveToDeadLetterAsync(
                message.Id, $"No FIFO handler registered for message type: {typeof(TMessage).FullName}", token);
            return;
        }

        try
        {
            var envelope = new InboxMessageEnvelope<TMessage>(message.Id, payload);
            var result = await handler.HandleAsync(envelope, token);
            var messageResult = new InboxMessageResult(message.Id, result);
            var messagesById = new Dictionary<Guid, InboxMessage> { { message.Id, message } };
            await ProcessResultsAsync([messageResult], messagesById, storageProvider, token);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Error executing FIFO handler for message {MessageId}", message.Id);
            await FailMessageAsync(message, storageProvider, token);
        }
    }
}