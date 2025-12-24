namespace Rh.Inbox.Postgres.Options;

/// <summary>
/// Configuration options for Postgres inbox storage.
/// </summary>
public class PostgresInboxOptions
{
    /// <summary>
    /// The default table name prefix for inbox messages.
    /// </summary>
    public const string DefaultTablePrefix = "inbox_messages";

    /// <summary>
    /// The default table name prefix for dead letter messages.
    /// </summary>
    public const string DefaultDeadLetterTablePrefix = "inbox_dead_letters";

    /// <summary>
    /// The default table name prefix for deduplication records.
    /// </summary>
    public const string DefaultDeduplicationTablePrefix = "inbox_dedup";

    /// <summary>
    /// The default table name prefix for FIFO group locks.
    /// </summary>
    public const string DefaultGroupLocksTablePrefix = "inbox_group_locks";

    /// <summary>
    /// Gets or sets the connection string for the Postgres database (required).
    /// </summary>
    public required string ConnectionString { get; init; }

    /// <summary>
    /// Table name for inbox messages. If null, will be generated as "{DefaultTablePrefix}_{inboxName}".
    /// </summary>
    public string? TableName { get; set; }

    /// <summary>
    /// Table name for dead letter messages. If null, will be generated as "{DefaultDeadLetterTablePrefix}_{inboxName}".
    /// </summary>
    public string? DeadLetterTableName { get; set; }

    /// <summary>
    /// Table name for deduplication records. If null, will be generated as "{DefaultDeduplicationTablePrefix}_{inboxName}".
    /// Only used when DeduplicationInterval is set.
    /// </summary>
    public string? DeduplicationTableName { get; set; }

    /// <summary>
    /// Gets or sets whether cleanup tasks should automatically start with the inbox lifecycle.
    /// When true (default), cleanup tasks are registered as lifecycle hooks and start/stop automatically.
    /// When false, cleanup tasks must be started manually via <see cref="Services.IPostgresCleanupTasksManager"/>.
    /// Set to false when running cleanup tasks on a separate host, pod, or as a cronjob.
    /// </summary>
    public bool AutostartCleanupTasks { get; set; } = true;

    /// <summary>
    /// Gets or sets the options for the dead letter cleanup task.
    /// </summary>
    public CleanupTaskOptions DeadLetterCleanup { get; set; } = new();

    /// <summary>
    /// Gets or sets the options for the deduplication cleanup task.
    /// </summary>
    public CleanupTaskOptions DeduplicationCleanup { get; set; } = new();

    /// <summary>
    /// Gets or sets the options for the group locks cleanup task.
    /// </summary>
    public CleanupTaskOptions GroupLocksCleanup { get; set; } = new();
}