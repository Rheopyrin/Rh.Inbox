using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Inboxes;
using Rh.Inbox.Processing.Utility;

namespace Rh.Inbox.Processing.Strategies.Implementation;

internal sealed class DefaultInboxProcessingStrategy : InboxProcessingStrategyBase
{
    private delegate Task ProcessMessageDelegate(
        IMessageProcessingContext context,
        InboxMessage message,
        IInboxMessagePayloadSerializer serializer,
        CancellationToken token);

    private readonly BoundedDelegateCache<ProcessMessageDelegate> _delegateCache;

    public DefaultInboxProcessingStrategy(
        InboxBase inbox,
        IServiceProvider serviceProvider,
        ILogger logger)
        : base(inbox, serviceProvider, logger)
    {
        _delegateCache = new BoundedDelegateCache<ProcessMessageDelegate>(this, nameof(ProcessMessageAsync));
    }

    public override async Task ProcessAsync(
        string processorId,
        IReadOnlyList<InboxMessage> messages,
        IMessageProcessingContext context,
        CancellationToken token)
    {
        var configuration = GetConfiguration();
        var serializer = GetSerializer();

        // Process all messages in parallel - each message handles its own result immediately
        await ProcessInParallelAsync(messages, async (message, ct) =>
        {
            var messageType = configuration.MetadataRegistry.GetClrType(message.MessageType);
            if (messageType == null)
            {
                Logger.LogWarning("Unknown message type: {MessageType}", message.MessageType);
                await context.MoveToDeadLetterAsync(message, $"Unknown message type: {message.MessageType}", ct);
                return;
            }

            var processDelegate = _delegateCache.GetOrAdd(messageType);
            await processDelegate(context, message, serializer, ct);
        }, token);
    }

    private async Task ProcessMessageAsync<TMessage>(
        IMessageProcessingContext context,
        InboxMessage message,
        IInboxMessagePayloadSerializer serializer,
        CancellationToken token) where TMessage : class
    {
        var payload = serializer.Deserialize<TMessage>(message.Payload);
        if (payload == null)
        {
            await context.MoveToDeadLetterAsync(message, "Failed to deserialize message payload", token);
            return;
        }

        // Create scope per handler execution (thread-safe for parallel processing)
        using var scope = ServiceProvider.CreateScope();
        var handler = scope.ServiceProvider.GetKeyedService<IInboxHandler<TMessage>>(Inbox.Name);

        if (handler == null)
        {
            Logger.LogWarning("No handler registered for message type: {MessageType}", typeof(TMessage).FullName);
            await context.MoveToDeadLetterAsync(message, $"No handler registered for message type: {typeof(TMessage).FullName}", token);
            return;
        }

        try
        {
            var envelope = new InboxMessageEnvelope<TMessage>(message.Id, payload);
            InboxHandleResult result = default;

            var completed = await ExecuteWithTimeoutAsync(
                async ct => { result = await handler.HandleAsync(envelope, ct); },
                $"message {message.Id}",
                token);

            // Use Failed result on timeout, letting ProcessResultsBatchAsync handle max attempts logic
            var handlerResult = completed ? result : InboxHandleResult.Failed;
            var messageResult = new InboxMessageResult(message.Id, handlerResult);
            await context.ProcessResultsBatchAsync([messageResult], token);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Error executing handler for message {MessageId}", message.Id);
            await context.FailMessageAsync(message, token);
        }
    }
}
