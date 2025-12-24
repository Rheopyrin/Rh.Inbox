using Rh.Inbox.Abstractions;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Configuration;
using Rh.Inbox.Management;

namespace Rh.Inbox.Writers;

internal sealed class InboxWriter : IInboxWriter
{
    private readonly IInboxManagerInternal _inboxManager;

    public InboxWriter(IInboxManagerInternal inboxManager)
    {
        _inboxManager = inboxManager;
    }

    public Task WriteAsync<TMessage>(TMessage message, CancellationToken token = default) where TMessage : class
    {
        return WriteAsync(message, InboxOptions.DefaultInboxName, token);
    }

    public async Task WriteAsync<TMessage>(TMessage message, string inboxName, CancellationToken token = default) where TMessage : class
    {
        var inbox = _inboxManager.GetInboxInternal(inboxName);
        var inboxMessage = inbox.CreateInboxMessage(message);
        await inbox.GetStorageProvider().WriteAsync(inboxMessage, token);
    }

    public Task WriteBatchAsync<TMessage>(IEnumerable<TMessage> messages, CancellationToken token = default) where TMessage : class
    {
        return WriteBatchAsync(messages, InboxOptions.DefaultInboxName, token);
    }

    public async Task WriteBatchAsync<TMessage>(IEnumerable<TMessage> messages, string inboxName, CancellationToken token = default) where TMessage : class
    {
        var inbox = _inboxManager.GetInboxInternal(inboxName);
        var inboxMessages = messages
            .Select(m => inbox.CreateInboxMessage(m))
            .ToList();

        if (inboxMessages.Count == 0)
            return;

        CollapseMessages(inboxMessages);
        token.ThrowIfCancellationRequested();

        var options = inbox.GetConfiguration().Options;
        var storageProvider = inbox.GetStorageProvider();
        var chunks = inboxMessages.Chunk(options.WriteBatchSize).ToList();

        if (options.MaxWriteThreads <= 1 || chunks.Count <= 1)
        {
            await storageProvider.WriteBatchAsync(inboxMessages, token);
        }
        else
        {
            var parallelOptions = new ParallelOptions
            {
                MaxDegreeOfParallelism = options.MaxWriteThreads,
                CancellationToken = token
            };

            await Parallel.ForEachAsync(chunks, parallelOptions, async (chunk, ct) =>
            {
                await storageProvider.WriteBatchAsync(chunk, ct);
            });
        }
    }

    private static void CollapseMessages(List<InboxMessage> messages)
    {
        var seenCollapseKeys = new HashSet<string>();
        var seenDeduplicationIds = new HashSet<string>();

        for (var i = messages.Count - 1; i >= 0; i--)
        {
            var collapseKey = messages[i].CollapseKey;
            var deduplicationId = messages[i].DeduplicationId;

            if (!string.IsNullOrEmpty(collapseKey) && !seenCollapseKeys.Add(collapseKey))
            {
                messages.RemoveAt(i);
                continue;
            }

            if (!string.IsNullOrEmpty(deduplicationId) && !seenDeduplicationIds.Add(deduplicationId))
            {
                messages.RemoveAt(i);
            }
        }
    }
}