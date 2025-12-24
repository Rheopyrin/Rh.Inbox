namespace Rh.Inbox.Abstractions;

/// <summary>
/// Service for running inbox storage migrations.
/// Use this in pre-deployment jobs to create tables/indexes before application startup.
/// </summary>
public interface IInboxMigrationService
{
    /// <summary>
    /// Runs migrations for all registered inboxes that support migration.
    /// </summary>
    Task MigrateAsync(CancellationToken token = default);

    /// <summary>
    /// Runs migration for a specific inbox by name.
    /// </summary>
    Task MigrateAsync(string inboxName, CancellationToken token = default);
}
