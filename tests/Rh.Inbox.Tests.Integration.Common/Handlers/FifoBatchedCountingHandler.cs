using System.Collections.Concurrent;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;

namespace Rh.Inbox.Tests.Integration.Common.Handlers;

public class FifoBatchedCountingHandler<TMessage> : IFifoBatchedInboxHandler<TMessage>
    where TMessage : class, IHasGroupId
{
    private readonly ConcurrentBag<FifoBatchProcessedGroup<TMessage>> _processedGroups = new();
    private readonly string _processorId;
    private int _totalProcessedCount;
    private int _groupSequence;

    public FifoBatchedCountingHandler(string processorId = "default")
    {
        _processorId = processorId;
    }

    public IReadOnlyCollection<FifoBatchProcessedGroup<TMessage>> ProcessedGroups => _processedGroups;
    public int ProcessedCount => _totalProcessedCount;
    public int GroupsProcessed => _processedGroups.Count;

    public Task<IReadOnlyList<InboxMessageResult>> HandleAsync(
        string groupId,
        IReadOnlyList<InboxMessageEnvelope<TMessage>> messages,
        CancellationToken token)
    {
        var groupSeq = Interlocked.Increment(ref _groupSequence);
        var processedMessages = messages.Select(m => m.Payload).ToList();

        _processedGroups.Add(new FifoBatchProcessedGroup<TMessage>(
            groupId,
            processedMessages,
            DateTime.UtcNow,
            _processorId,
            groupSeq));

        Interlocked.Add(ref _totalProcessedCount, messages.Count);

        var results = messages
            .Select(m => new InboxMessageResult(m.Id, InboxHandleResult.Success))
            .ToList();

        return Task.FromResult<IReadOnlyList<InboxMessageResult>>(results);
    }

    public void Clear()
    {
        _processedGroups.Clear();
        _totalProcessedCount = 0;
        _groupSequence = 0;
    }

    /// <summary>
    /// Validates that messages within each group were processed in order.
    /// </summary>
    public bool ValidateGroupOrdering()
    {
        foreach (var group in _processedGroups)
        {
            // Check that messages in the batch maintain their sequence
            var messagesWithSequence = group.Messages
                .OfType<dynamic>()
                .Where(m => HasProperty(m, "Sequence"))
                .ToList();

            if (messagesWithSequence.Count > 1)
            {
                for (int i = 1; i < messagesWithSequence.Count; i++)
                {
                    if ((int)messagesWithSequence[i].Sequence < (int)messagesWithSequence[i - 1].Sequence)
                    {
                        return false;
                    }
                }
            }
        }

        return true;
    }

    private static bool HasProperty(object obj, string propertyName)
    {
        return obj.GetType().GetProperty(propertyName) != null;
    }
}

public record FifoBatchProcessedGroup<TMessage>(
    string GroupId,
    IReadOnlyList<TMessage> Messages,
    DateTime ProcessedAt,
    string ProcessorId,
    int GroupSequence);
