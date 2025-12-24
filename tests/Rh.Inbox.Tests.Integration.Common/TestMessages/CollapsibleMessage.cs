using Rh.Inbox.Abstractions.Messages;

namespace Rh.Inbox.Tests.Integration.Common.TestMessages;

public record CollapsibleMessage(string CollapseKey, int Version, string Data) : IHasCollapseKey
{
    public string GetCollapseKey() => CollapseKey;
}
