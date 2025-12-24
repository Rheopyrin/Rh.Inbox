using Rh.Inbox.Abstractions.Messages;

namespace Rh.Inbox.Tests.Integration.Common.TestMessages;

public record FifoMessage(string GroupId, int Sequence, string Data) : IHasGroupId
{
    public string GetGroupId() => GroupId;
}
