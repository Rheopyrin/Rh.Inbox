using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Inboxes;
using Rh.Inbox.Processing.Utility;

namespace Rh.Inbox.Processing.Strategies.Implementation;

internal sealed class BatchedInboxProcessingStrategy : InboxProcessingStrategyBase
{
    private delegate Task ProcessMessagesDelegate(
        IMessageProcessingContext context,
        List<InboxMessage> messages,
        IInboxMessagePayloadSerializer serializer,
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

    public override async Task ProcessAsync(
        string processorId,
        IReadOnlyList<InboxMessage> messages,
        IMessageProcessingContext context,
        CancellationToken token)
    {
        var configuration = GetConfiguration();
        var serializer = GetSerializer();

        var messagesByType = messages
            .GroupBy(m => m.MessageType)
            .ToArray();

        // Process type groups in parallel - each group handles its own results internally
        await ProcessInParallelAsync(messagesByType, async (group, ct) =>
        {
            var messageType = configuration.MetadataRegistry.GetClrType(group.Key);
            if (messageType == null)
            {
                Logger.LogWarning("Unknown message type: {MessageType}", group.Key);
                var reason = $"Unknown message type: {group.Key}";
                await context.MoveToDeadLetterBatchAsync(
                    group.Select(m => (m, reason)).ToArray(), ct);
                return;
            }

            var processDelegate = _delegateCache.GetOrAdd(messageType);
            await processDelegate(context, group.ToList(), serializer, ct);
        }, token);
    }

    private async Task ProcessMessagesAsync<TMessage>(
        IMessageProcessingContext context,
        List<InboxMessage> messages,
        IInboxMessagePayloadSerializer serializer,
        CancellationToken token) where TMessage : class
    {
        using var scope = ServiceProvider.CreateScope();
        var handler = scope.ServiceProvider.GetKeyedService<IBatchedInboxHandler<TMessage>>(Inbox.Name);

        if (handler == null)
        {
            Logger.LogWarning("No handler registered for message type: {MessageType}", typeof(TMessage).FullName);
            var reason = $"No handler registered for message type: {typeof(TMessage).FullName}";
            await context.MoveToDeadLetterBatchAsync(
                messages.Select(m => (m, reason)).ToArray(), token);
            return;
        }

        var envelopes = new List<InboxMessageEnvelope<TMessage>>(messages.Count);
        var successfulMessages = new List<InboxMessage>(messages.Count);
        var deserializationFailures = new List<InboxMessage>(messages.Count);

        foreach (var msg in messages)
        {
            var payload = serializer.Deserialize<TMessage>(msg.Payload);
            if (payload == null)
            {
                deserializationFailures.Add(msg);
                continue;
            }

            envelopes.Add(new InboxMessageEnvelope<TMessage>(msg.Id, payload));
            successfulMessages.Add(msg);
        }

        if (deserializationFailures.Count > 0)
        {
            await context.MoveToDeadLetterBatchAsync(
                deserializationFailures.Select(m => (m, "Failed to deserialize message payload")).ToArray(), token);
        }

        if (envelopes.Count == 0)
        {
            return;
        }

        try
        {
            IReadOnlyList<InboxMessageResult> results = [];

            var completed = await ExecuteWithTimeoutAsync(
                async ct => { results = await handler.HandleAsync(envelopes, ct); },
                $"batch of {envelopes.Count} messages of type {typeof(TMessage).FullName}",
                token);

            if (!completed)
            {
                results = envelopes.Select(e => new InboxMessageResult(e.Id, InboxHandleResult.Failed)).ToArray();
            }

            await context.ProcessResultsBatchAsync(results, token);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Error executing handler for message type: {MessageType}", typeof(TMessage).FullName);
            await context.FailMessageBatchAsync(successfulMessages, token);
        }
    }
}