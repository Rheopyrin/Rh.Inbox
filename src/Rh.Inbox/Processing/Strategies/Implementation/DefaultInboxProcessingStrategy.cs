using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Inboxes;
using Rh.Inbox.Processing.Utility;

namespace Rh.Inbox.Processing.Strategies.Implementation;

internal sealed class DefaultInboxProcessingStrategy : InboxProcessingStrategyBase
{
    private delegate Task ProcessMessageDelegate(
        InboxMessage message,
        IInboxMessagePayloadSerializer serializer,
        IInboxStorageProvider storageProvider,
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

    public override async Task ProcessAsync(string processorId, IReadOnlyList<InboxMessage> messages, CancellationToken token)
    {
        var configuration = GetConfiguration();
        var storageProvider = GetStorageProvider();
        var serializer = GetSerializer();

        // Process all messages in parallel - each message handles its own result immediately
        await ProcessInParallelAsync(messages, async (message, ct) =>
        {
            var messageType = configuration.MetadataRegistry.GetClrType(message.MessageType);
            if (messageType == null)
            {
                Logger.LogWarning("Unknown message type: {MessageType}", message.MessageType);
                await MoveToDeadLetterAsync(message, $"Unknown message type: {message.MessageType}", storageProvider, ct);
                return;
            }

            var processDelegate = _delegateCache.GetOrAdd(messageType);
            await processDelegate(message, serializer, storageProvider, ct);
        }, token);
    }

    private async Task ProcessMessageAsync<TMessage>(
        InboxMessage message,
        IInboxMessagePayloadSerializer serializer,
        IInboxStorageProvider storageProvider,
        CancellationToken token) where TMessage : class
    {
        var payload = serializer.Deserialize<TMessage>(message.Payload);
        if (payload == null)
        {
            await storageProvider.MoveToDeadLetterAsync(
                message.Id, "Failed to deserialize message payload", token);
            return;
        }

        // Create scope per handler execution (thread-safe for parallel processing)
        using var scope = ServiceProvider.CreateScope();
        var handler = scope.ServiceProvider.GetKeyedService<IInboxHandler<TMessage>>(Inbox.Name);

        if (handler == null)
        {
            Logger.LogWarning("No handler registered for message type: {MessageType}", typeof(TMessage).FullName);
            await storageProvider.MoveToDeadLetterAsync(
                message.Id, $"No handler registered for message type: {typeof(TMessage).FullName}", token);
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

            // Use Failed result on timeout, letting ProcessResultsAsync handle max attempts logic
            var handlerResult = completed ? result : InboxHandleResult.Failed;
            var messageResult = new InboxMessageResult(message.Id, handlerResult);
            var messagesById = new Dictionary<Guid, InboxMessage> { { message.Id, message } };
            await ProcessResultsAsync([messageResult], messagesById, storageProvider, token);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Error executing handler for message {MessageId}", message.Id);
            await FailMessageAsync(message, storageProvider, token);
        }
    }
}