using System.Collections.Concurrent;

namespace Rh.Inbox.InMemory.Options;

internal interface IProviderOptionsAccessor
{
    InMemoryInboxProviderOptions GetForInbox(string inboxName);
}

internal sealed class ProviderOptionsAccessor : IProviderOptionsAccessor
{
    private readonly ConcurrentDictionary<string, InMemoryInboxProviderOptions> _options = new();

    public InMemoryInboxProviderOptions GetForInbox(string inboxName)
    {
        return _options.GetOrAdd(inboxName, _ => new InMemoryInboxProviderOptions
        {
            DeduplicationStore = new InMemoryDeduplicationStore(),
            DeadLetterStore = new InMemoryDeadLetterStore()
        });
    }
}