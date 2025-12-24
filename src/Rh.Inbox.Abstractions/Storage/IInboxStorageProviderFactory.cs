using Rh.Inbox.Abstractions.Configuration;

namespace Rh.Inbox.Abstractions.Storage;

/// <summary>
/// Factory interface for creating inbox storage providers.
/// Implement this interface to provide custom storage provider instantiation logic.
/// </summary>
public interface IInboxStorageProviderFactory
{
    /// <summary>
    /// Creates a new storage provider instance with the specified options.
    /// </summary>
    /// <param name="options">Configuration options for the storage provider.</param>
    /// <returns>A configured storage provider instance.</returns>
    IInboxStorageProvider Create(IInboxConfiguration options);
}