using Rh.Inbox.Abstractions.Configuration;

namespace Rh.Inbox.Configuration.Registry;

internal sealed class InboxConfigurationRegistry
{
    private readonly Dictionary<string, IInboxConfiguration> _configurations = new();

    public void Register(IInboxConfiguration configuration)
    {
        if (_configurations.ContainsKey(configuration.InboxName))
        {
            throw new InvalidOperationException(
                $"Rh.Inbox '{configuration.InboxName}' is already registered.");
        }
        _configurations[configuration.InboxName] = configuration;
    }

    public IInboxConfiguration Get(string inboxName)
    {
        if (!_configurations.TryGetValue(inboxName, out var configuration))
        {
            throw new InvalidOperationException(
                $"Rh.Inbox '{inboxName}' is not registered.");
        }
        return configuration;
    }

    public IInboxConfiguration GetDefault()
    {
        return Get(InboxOptions.DefaultInboxName);
    }

    public bool TryGet(string inboxName, out IInboxConfiguration? configuration)
    {
        return _configurations.TryGetValue(inboxName, out configuration);
    }

    public IEnumerable<IInboxConfiguration> GetAll()
    {
        return _configurations.Values;
    }
}