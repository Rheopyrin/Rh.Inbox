namespace Rh.Inbox.Abstractions.Storage;

/// <summary>
/// Interface for storage providers that support schema migration.
/// Migration should be run once during pre-deployment (e.g., Kubernetes job),
/// not on every pod startup.
/// </summary>
public interface ISupportMigration : IInboxStorageProvider
{
    /// <summary>
    /// Performs schema migration (table creation, index updates, etc.).
    /// This method should be idempotent and safe to run multiple times.
    /// </summary>
    Task MigrateAsync(CancellationToken token = default);
}
