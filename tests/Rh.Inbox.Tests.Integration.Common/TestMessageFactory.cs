using Rh.Inbox.Tests.Integration.Common.TestMessages;

namespace Rh.Inbox.Tests.Integration.Common;

public static class TestMessageFactory
{
    public static List<SimpleMessage> CreateSimpleMessages(int count, string prefix = "msg")
    {
        return Enumerable.Range(0, count)
            .Select(i => new SimpleMessage($"{prefix}-{i}", $"data-{i}"))
            .ToList();
    }

    public static List<FifoMessage> CreateFifoMessages(int count, string groupId)
    {
        return Enumerable.Range(0, count)
            .Select(i => new FifoMessage(groupId, i, $"data-{i}"))
            .ToList();
    }

    public static List<DeduplicatableMessage> CreateDeduplicatableMessages(int count, string prefix = "dedup")
    {
        return Enumerable.Range(0, count)
            .Select(i => new DeduplicatableMessage($"{prefix}-{i}", $"data-{i}"))
            .ToList();
    }

    public static List<LargePayloadMessage> CreateLargePayloadMessages(int count, int payloadSize, string prefix = "large")
    {
        return Enumerable.Range(0, count)
            .Select(i => new LargePayloadMessage($"{prefix}-{i}", PayloadSizes.Generate(payloadSize)))
            .ToList();
    }

    public static IEnumerable<List<T>> BatchMessages<T>(List<T> messages, int batchSize)
    {
        for (int i = 0; i < messages.Count; i += batchSize)
        {
            yield return messages.Skip(i).Take(batchSize).ToList();
        }
    }
}
