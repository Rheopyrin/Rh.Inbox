using Rh.Inbox.Abstractions.Messages;

namespace Rh.Inbox.Tests.Integration.Common.TestMessages;

public record FifoDeduplicatableMessage(string GroupId, string DeduplicationId, int Sequence)
    : IHasGroupId, IHasDeduplicationId
{
    public string GetGroupId() => GroupId;
    public string GetDeduplicationId() => DeduplicationId;
}
