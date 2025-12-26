using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Inboxes;
using Rh.Inbox.Processing.Utility;

namespace Rh.Inbox.Processing.Strategies.Implementation;

internal sealed class FifoBatchedInboxProcessingStrategy : InboxProcessingStrategyBase
{
    private delegate Task ProcessGroupDelegate(
        List<InboxMessage> messages,
        string groupId,
        IInboxMessagePayloadSerializer serializer,
        IInboxStorageProvider storageProvider,
        CancellationToken token);

    private readonly BoundedDelegateCache<ProcessGroupDelegate> _delegateCache;

    public FifoBatchedInboxProcessingStrategy(
        InboxBase inbox,
        IServiceProvider serviceProvider,
        ILogger logger)
        : base(inbox, serviceProvider, logger)
    {
        _delegateCache = new BoundedDelegateCache<ProcessGroupDelegate>(this, nameof(ProcessGroupAsync));
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
            // Order preserved from storage (Redis sorted set scored by ReceivedAt) - no explicit sorting needed
            var groupMessages = group.ToList();

            try
            {
                // Batch consecutive same-type messages, process batches in order
                foreach (var typeBatch in BatchConsecutiveSameType(groupMessages))
                {
                    var messageTypeName = typeBatch[0].MessageType;
                    var messageType = configuration.MetadataRegistry.GetClrType(messageTypeName);

                    if (messageType == null)
                    {
                        Logger.LogWarning("Unknown message type: {MessageType}", messageTypeName);
                        var reason = $"Unknown message type: {messageTypeName}";
                        await storageProvider.MoveToDeadLetterBatchAsync(
                            typeBatch.Select(m => (m.Id, reason)).ToList(), ct);
                        continue;
                    }

                    var processDelegate = _delegateCache.GetOrAdd(messageType);
                    await processDelegate(typeBatch, groupId, serializer, storageProvider, ct);
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

    /// <summary>
    /// Batches consecutive messages of the same type while preserving order.
    /// [A:X, B:X, C:Y, D:X] → [[A:X, B:X], [C:Y], [D:X]]
    /// </summary>
    private static IEnumerable<List<InboxMessage>> BatchConsecutiveSameType(List<InboxMessage> messages)
    {
        if (messages.Count == 0)
        {
            yield break;
        }

        var batch = new List<InboxMessage> { messages[0] };

        for (var i = 1; i < messages.Count; i++)
        {
            if (messages[i].MessageType == batch[0].MessageType)
            {
                batch.Add(messages[i]);
            }
            else
            {
                yield return batch;
                batch = new List<InboxMessage> { messages[i] };
            }
        }

        yield return batch;
    }

    private async Task ProcessGroupAsync<TMessage>(
        List<InboxMessage> messages,
        string groupId,
        IInboxMessagePayloadSerializer serializer,
        IInboxStorageProvider storageProvider,
        CancellationToken token) where TMessage : class, IHasGroupId
    {
        using var scope = ServiceProvider.CreateScope();
        var handler = scope.ServiceProvider.GetKeyedService<IFifoBatchedInboxHandler<TMessage>>(Inbox.Name);

        if (handler == null)
        {
            Logger.LogWarning("No FIFO batched handler registered for message type: {MessageType}", typeof(TMessage).FullName);
            var reason = $"No FIFO batched handler registered for message type: {typeof(TMessage).FullName}";
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
        {
            return;
        }

        try
        {
            IReadOnlyList<InboxMessageResult> results = [];

            var completed = await ExecuteWithTimeoutAsync(
                async ct => { results = await handler.HandleAsync(groupId, envelopes, ct); },
                $"FIFO batch of {envelopes.Count} messages for group '{groupId}' of type {typeof(TMessage).FullName}",
                token);

            // On timeout, create Failed results for all messages in the batch
            if (!completed)
            {
                results = envelopes.Select(e => new InboxMessageResult(e.Id, InboxHandleResult.Failed)).ToArray();
            }

            await ProcessResultsAsync(results, messagesById, storageProvider, token);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Error executing FIFO batched handler for message type: {MessageType}, group: {GroupId}",
                typeof(TMessage).FullName, groupId);
            await FailMessageBatchAsync(messagesById.Values.ToList(), storageProvider, token);
        }
    }
}