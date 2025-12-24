using Rh.Inbox.Abstractions.Messages;

namespace Rh.Inbox.Tests.Integration.Common.TestMessages;

public record DeduplicatableMessage(string DeduplicationId, string Data) : IHasDeduplicationId
{
    public string GetDeduplicationId() => DeduplicationId;
}
