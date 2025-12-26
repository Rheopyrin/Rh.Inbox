using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;
using Microsoft.Extensions.Logging;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Inboxes;

namespace Rh.Inbox.Processing.Strategies;

internal abstract class InboxProcessingStrategyBase : IInboxProcessingStrategy
{
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

    public abstract Task ProcessAsync(
        string processorId,
        IReadOnlyList<InboxMessage> messages,
        IMessageProcessingContext context,
        CancellationToken token);

    protected IInboxConfiguration GetConfiguration() => Inbox.GetConfiguration();
    protected IInboxStorageProvider GetStorageProvider() => Inbox.GetStorageProvider();
    protected IInboxMessagePayloadSerializer GetSerializer() => Inbox.GetSerializer();

    /// <summary>
    /// Executes an async handler action with a timeout based on MaxProcessingTime.
    /// Creates a linked CancellationToken combining the external token with a timeout.
    /// </summary>
    /// <param name="action">The async action to execute (handler call)</param>
    /// <param name="messageContext">Context for logging (e.g., message ID or batch description)</param>
    /// <param name="externalToken">The external cancellation token</param>
    /// <returns>True if completed successfully; False if timed out</returns>
    protected async Task<bool> ExecuteWithTimeoutAsync(
        Func<CancellationToken, Task> action,
        string messageContext,
        CancellationToken externalToken)
    {
        var options = GetConfiguration().Options;

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
        timeoutCts.CancelAfter(options.MaxProcessingTime);

        try
        {
            await action(timeoutCts.Token);
            return true;
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !externalToken.IsCancellationRequested)
        {
            Logger.LogWarning(
                "Handler execution timed out after {MaxProcessingTime} for {MessageContext}",
                options.MaxProcessingTime,
                messageContext);
            return false;
        }
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
}
