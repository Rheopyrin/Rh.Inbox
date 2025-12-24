using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Storage;

namespace Rh.Inbox.InMemory;

internal sealed class InMemoryInboxStorageProviderFactory(IServiceProvider serviceProvider)
    : IInboxStorageProviderFactory
{
    public IInboxStorageProvider Create(IInboxConfiguration options) =>
        ActivatorUtilities.CreateInstance<InMemoryInboxStorageProvider>(serviceProvider, options);
}