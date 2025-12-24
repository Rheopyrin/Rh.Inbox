using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;
using Microsoft.Extensions.Logging;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Inboxes;

namespace Rh.Inbox.Processing.Strategies;

internal abstract class InboxProcessingStrategyBase : IInboxProcessingStrategy
{
    private const string DefaultDeadLetterReason = "Handler requested move to dead letter";

    protected readonly InboxBase Inbox;
    protected readonly IServiceProvider ServiceProvider;
    protected readonly ILogger Logger;

    protected InboxProcessingStrategyBase(
        InboxBase inbox,
        IServiceProvider serviceProvider,
        ILogger logger)
    {
        Inbox = inbox;
        ServiceProvider = serviceProvider;
        Logger = logger;
    }

    public abstract Task ProcessAsync(string processorId, IReadOnlyList<InboxMessage> messages, CancellationToken token);

    protected IInboxConfiguration GetConfiguration() => Inbox.GetConfiguration();
    protected IInboxStorageProvider GetStorageProvider() => Inbox.GetStorageProvider();
    protected IInboxMessagePayloadSerializer GetSerializer() => Inbox.GetSerializer();

    private static string GetMaxAttemptsExceededReason(int maxAttempts) =>
        $"Max attempts ({maxAttempts}) exceeded";

    private static string GetDeadLetterReason(string? failureReason, int maxAttempts, bool isMaxAttemptsExceeded) =>
        failureReason ?? (isMaxAttemptsExceeded ? GetMaxAttemptsExceededReason(maxAttempts) : DefaultDeadLetterReason);

    protected async Task ProcessResultsAsync(
        IReadOnlyList<InboxMessageResult> results,
        IReadOnlyDictionary<Guid, InboxMessage> messagesById,
        IInboxStorageProvider storageProvider,
        CancellationToken token)
    {
        if (results.Count == 0) return;

        var options = GetConfiguration().Options;

        // Group results by outcome for batch processing
        var toComplete = new List<Guid>();
        var toFail = new List<Guid>();
        var toRelease = new List<Guid>();
        var toDeadLetter = new List<(Guid MessageId, string Reason)>();

        foreach (var result in results)
        {
            switch (result.Result)
            {
                case InboxHandleResult.Success:
                    toComplete.Add(result.MessageId);
                    break;

                case InboxHandleResult.Failed:
                    // Check if max attempts exceeded - move to dead letter instead of failing
                    if (messagesById.TryGetValue(result.MessageId, out var failedMessage) &&
                        failedMessage.AttemptsCount + 1 >= options.MaxAttempts)
                    {
                        var reason = GetDeadLetterReason(result.FailureReason, options.MaxAttempts, isMaxAttemptsExceeded: true);
                        toDeadLetter.Add((result.MessageId, reason));
                        Logger.LogWarning(
                            "Message {MessageId} exceeded max attempts ({MaxAttempts}), moving to dead letter",
                            result.MessageId, options.MaxAttempts);
                    }
                    else
                    {
                        toFail.Add(result.MessageId);
                    }
                    break;

                case InboxHandleResult.Retry:
                    toRelease.Add(result.MessageId);
                    break;

                case InboxHandleResult.MoveToDeadLetter:
                    toDeadLetter.Add((result.MessageId, GetDeadLetterReason(result.FailureReason, options.MaxAttempts, isMaxAttemptsExceeded: false)));
                    break;
            }
        }

        // Execute all operations in a single connection/transaction
        await storageProvider.ProcessResultsBatchAsync(toComplete, toFail, toRelease, toDeadLetter, token);
    }

    /// <summary>
    /// Executes items in parallel (or sequentially if MaxProcessingThreads=1).
    /// Each item processes its own results immediately upon completion.
    /// </summary>
    protected async Task ProcessInParallelAsync<TItem>(
        IReadOnlyList<TItem> items,
        Func<TItem, CancellationToken, Task> processItem,
        CancellationToken token)
    {
        var options = GetConfiguration().Options;

        if (options.MaxProcessingThreads <= 1)
        {
            // Sequential processing (current behavior)
            foreach (var item in items)
            {
                await processItem(item, token);
            }
        }
        else
        {
            // Parallel processing with bounded concurrency
            var parallelOptions = new ParallelOptions
            {
                MaxDegreeOfParallelism = options.MaxProcessingThreads,
                CancellationToken = token
            };

            await Parallel.ForEachAsync(items, parallelOptions, async (item, ct) =>
            {
                await processItem(item, ct);
            });
        }
    }

    protected async Task MoveToDeadLetterAsync(
        InboxMessage message,
        string reason,
        IInboxStorageProvider storageProvider,
        CancellationToken token)
    {
        await storageProvider.MoveToDeadLetterAsync(message.Id, reason, token);
    }

    protected async Task FailMessageAsync(
        InboxMessage message,
        IInboxStorageProvider storageProvider,
        CancellationToken token)
    {
        var options = GetConfiguration().Options;

        // Check if max attempts will be exceeded after this failure
        if (message.AttemptsCount + 1 >= options.MaxAttempts)
        {
            Logger.LogWarning(
                "Message {MessageId} exceeded max attempts ({MaxAttempts}), moving to dead letter",
                message.Id, options.MaxAttempts);
            await storageProvider.MoveToDeadLetterAsync(message.Id, GetMaxAttemptsExceededReason(options.MaxAttempts), token);
        }
        else
        {
            await storageProvider.FailAsync(message.Id, token);
        }
    }

    protected async Task FailMessageBatchAsync(
        IReadOnlyList<InboxMessage> messages,
        IInboxStorageProvider storageProvider,
        CancellationToken token)
    {
        if (messages.Count == 0) return;

        var options = GetConfiguration().Options;
        var maxAttemptsReason = GetMaxAttemptsExceededReason(options.MaxAttempts);

        var toFail = new List<Guid>();
        var toDeadLetter = new List<(Guid MessageId, string Reason)>();

        foreach (var message in messages)
        {
            if (message.AttemptsCount + 1 >= options.MaxAttempts)
            {
                Logger.LogWarning(
                    "Message {MessageId} exceeded max attempts ({MaxAttempts}), moving to dead letter",
                    message.Id, options.MaxAttempts);
                toDeadLetter.Add((message.Id, maxAttemptsReason));
            }
            else
            {
                toFail.Add(message.Id);
            }
        }

        if (toFail.Count > 0)
        {
            await storageProvider.FailBatchAsync(toFail, token);
        }

        if (toDeadLetter.Count > 0)
        {
            await storageProvider.MoveToDeadLetterBatchAsync(toDeadLetter, token);
        }
    }
}