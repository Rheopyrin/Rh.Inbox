using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Inboxes;
using Rh.Inbox.Processing.Utility;

namespace Rh.Inbox.Processing.Strategies.Implementation;

internal sealed class BatchedInboxProcessingStrategy : InboxProcessingStrategyBase
{
    private delegate Task ProcessMessagesDelegate(
        List<InboxMessage> messages,
        IInboxMessagePayloadSerializer serializer,
        IInboxStorageProvider storageProvider,
        CancellationToken token);

    private readonly BoundedDelegateCache<ProcessMessagesDelegate> _delegateCache;

    public BatchedInboxProcessingStrategy(
        InboxBase inbox,
        IServiceProvider serviceProvider,
        ILogger logger)
        : base(inbox, serviceProvider, logger)
    {
        _delegateCache = new BoundedDelegateCache<ProcessMessagesDelegate>(this, nameof(ProcessMessagesAsync));
    }

    public override async Task ProcessAsync(string processorId, IReadOnlyList<InboxMessage> messages, CancellationToken token)
    {
        var configuration = GetConfiguration();
        var storageProvider = GetStorageProvider();
        var serializer = GetSerializer();

        var messagesByType = messages
            .GroupBy(m => m.MessageType)
            .ToList();

        // Process type groups in parallel - each group handles its own results internally
        await ProcessInParallelAsync(messagesByType, async (group, ct) =>
        {
            var messageType = configuration.MetadataRegistry.GetClrType(group.Key);
            if (messageType == null)
            {
                Logger.LogWarning("Unknown message type: {MessageType}", group.Key);
                var reason = $"Unknown message type: {group.Key}";
                await storageProvider.MoveToDeadLetterBatchAsync(
                    group.Select(m => (m.Id, reason)).ToList(), ct);
                return;
            }

            var processDelegate = _delegateCache.GetOrAdd(messageType);
            await processDelegate(group.ToList(), serializer, storageProvider, ct);
        }, token);
    }

    private async Task ProcessMessagesAsync<TMessage>(
        List<InboxMessage> messages,
        IInboxMessagePayloadSerializer serializer,
        IInboxStorageProvider storageProvider,
        CancellationToken token) where TMessage : class
    {
        using var scope = ServiceProvider.CreateScope();
        var handler = scope.ServiceProvider.GetKeyedService<IBatchedInboxHandler<TMessage>>(Inbox.Name);

        if (handler == null)
        {
            Logger.LogWarning("No handler registered for message type: {MessageType}", typeof(TMessage).FullName);
            var reason = $"No handler registered for message type: {typeof(TMessage).FullName}";
            await storageProvider.MoveToDeadLetterBatchAsync(
                messages.Select(m => (m.Id, reason)).ToList(), token);
            return;
        }

        var envelopes = new List<InboxMessageEnvelope<TMessage>>();
        var messagesById = new Dictionary<Guid, InboxMessage>();
        var deserializationFailures = new List<Guid>();

        foreach (var msg in messages)
        {
            var payload = serializer.Deserialize<TMessage>(msg.Payload);
            if (payload == null)
            {
                deserializationFailures.Add(msg.Id);
                continue;
            }

            envelopes.Add(new InboxMessageEnvelope<TMessage>(msg.Id, payload));
            messagesById[msg.Id] = msg;
        }

        if (deserializationFailures.Count > 0)
        {
            await storageProvider.MoveToDeadLetterBatchAsync(
                deserializationFailures.Select(id => (id, "Failed to deserialize message payload")).ToList(), token);
        }

        if (envelopes.Count == 0)
            return;

        try
        {
            var results = await handler.HandleAsync(envelopes, token);
            await ProcessResultsAsync(results, messagesById, storageProvider, token);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Error executing handler for message type: {MessageType}", typeof(TMessage).FullName);
            await FailMessageBatchAsync(messagesById.Values.ToList(), storageProvider, token);
        }
    }
}