using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Rh.Inbox.Abstractions.Builders;
using Rh.Inbox.Abstractions.Lifecycle;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Exceptions;
using Rh.Inbox.Postgres.Connection;
using Rh.Inbox.Postgres.Options;
using Rh.Inbox.Postgres.Services;
using Rh.Inbox.Providers;

namespace Rh.Inbox.Postgres;

/// <summary>
/// Extension methods for configuring Postgres inbox storage.
/// </summary>
public static class PostgresInboxBuilderExtensions
{

    /// <summary>
    /// Configures the inbox to use Postgres storage with custom options.
    /// </summary>
    /// <typeparam name="TBuilder">The builder type.</typeparam>
    /// <param name="builder">The inbox builder.</param>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <param name="configure">Optional action to configure Postgres-specific options.</param>
    /// <returns>The builder for chaining.</returns>
    public static TBuilder UsePostgres<TBuilder>(
        this TBuilder builder,
        string connectionString,
        Action<PostgresInboxOptions>? configure = null)
        where TBuilder : IInboxBuilderBase<TBuilder>
    {
        var options = new PostgresInboxOptions { ConnectionString = connectionString };
        configure?.Invoke(options);

        ValidatePostgresOptions(options);

        builder.ConfigureServices(services =>
        {
            services.TryAddSingleton<INpgsqlDataSourceProvider, NpgsqlDataSourceProvider>();
            services.TryAddSingleton<IDateTimeProvider, DateTimeProvider>();
            services.TryAddKeyedSingleton(builder.InboxName, options);
            services.TryAddSingleton<IProviderOptionsAccessor, ProviderOptionsAccessor>();
            services.TryAddSingleton<PostgresInboxStorageProviderFactory>();

            // Register cleanup tasks manager (always available for external use)
            services.TryAddSingleton<IPostgresCleanupTasksManager, PostgresCleanupTasksManager>();

            // Register as lifecycle hook only when autostart is enabled
            if (options.AutostartCleanupTasks)
            {
                services.AddSingleton<IInboxLifecycleHook>(sp =>
                    (IInboxLifecycleHook)sp.GetRequiredService<IPostgresCleanupTasksManager>());
            }
        });

        builder.UseStorageProviderFactory<PostgresInboxStorageProviderFactory>();

        builder.PostConfigure((configuration, collection) =>
        {
            // Register GroupLocksCleanupService for FIFO/FifoBatched types
            if (configuration.InboxType is InboxType.Fifo or InboxType.FifoBatched)
            {
                collection.AddSingleton<ICleanupTask>(sp =>
                    ActivatorUtilities.CreateInstance<GroupLocksCleanupService>(sp, configuration, options.GroupLocksCleanup));
            }

            // Register DeduplicationCleanupService when deduplication is enabled
            if (configuration.Options.EnableDeduplication && configuration.Options.DeduplicationInterval > TimeSpan.Zero)
            {
                collection.AddSingleton<ICleanupTask>(sp =>
                    ActivatorUtilities.CreateInstance<DeduplicationCleanupService>(sp, configuration, options.DeduplicationCleanup));
            }

            // Register DeadLetterCleanupService when dead letter cleanup is enabled
            if (configuration.Options.EnableDeadLetter && configuration.Options.DeadLetterMaxMessageLifetime > TimeSpan.Zero)
            {
                collection.AddSingleton<ICleanupTask>(sp =>
                    ActivatorUtilities.CreateInstance<DeadLetterCleanupService>(sp, configuration, options.DeadLetterCleanup));
            }
        });

        return builder;
    }

    private static void ValidatePostgresOptions(PostgresInboxOptions options)
    {
        var errors = new List<InboxOptionError>();

        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            errors.Add(new InboxOptionError(nameof(options.ConnectionString), "Connection string is required."));
        }

        // Only validate table names if explicitly set (null means auto-generate)
        if (options.TableName is not null && !Utility.PostgresIdentifierHelper.IsValidIdentifier(options.TableName))
        {
            errors.Add(new InboxOptionError(nameof(options.TableName),
                $"'{options.TableName}' is not a valid PostgreSQL identifier. " +
                "Must start with a letter or underscore, contain only letters, digits, or underscores, " +
                $"and be at most {Utility.PostgresIdentifierHelper.MaxPostgresIdentifierLength} characters."));
        }

        if (options.DeadLetterTableName is not null && !Utility.PostgresIdentifierHelper.IsValidIdentifier(options.DeadLetterTableName))
        {
            errors.Add(new InboxOptionError(nameof(options.DeadLetterTableName),
                $"'{options.DeadLetterTableName}' is not a valid PostgreSQL identifier. " +
                "Must start with a letter or underscore, contain only letters, digits, or underscores, " +
                $"and be at most {Utility.PostgresIdentifierHelper.MaxPostgresIdentifierLength} characters."));
        }

        if (options.DeduplicationTableName is not null && !Utility.PostgresIdentifierHelper.IsValidIdentifier(options.DeduplicationTableName))
        {
            errors.Add(new InboxOptionError(nameof(options.DeduplicationTableName),
                $"'{options.DeduplicationTableName}' is not a valid PostgreSQL identifier. " +
                "Must start with a letter or underscore, contain only letters, digits, or underscores, " +
                $"and be at most {Utility.PostgresIdentifierHelper.MaxPostgresIdentifierLength} characters."));
        }

        if (errors.Count > 0)
        {
            throw new InvalidInboxConfigurationException("Invalid Postgres inbox configuration.", errors);
        }
    }
}