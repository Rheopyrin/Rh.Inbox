using Microsoft.Extensions.Logging;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Configuration.Registry;

namespace Rh.Inbox.Migration;

internal sealed class InboxMigrationService : IInboxMigrationService
{
    private readonly InboxConfigurationRegistry _configurationRegistry;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<InboxMigrationService> _logger;

    public InboxMigrationService(
        InboxConfigurationRegistry configurationRegistry,
        IServiceProvider serviceProvider,
        ILogger<InboxMigrationService> logger)
    {
        _configurationRegistry = configurationRegistry;
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    public async Task MigrateAsync(CancellationToken token = default)
    {
        _logger.LogInformation("Starting inbox migrations...");

        foreach (var configuration in _configurationRegistry.GetAll())
        {
            await MigrateInboxAsync(configuration, token);
        }

        _logger.LogInformation("Rh.Inbox migrations completed.");
    }

    public async Task MigrateAsync(string inboxName, CancellationToken token = default)
    {
        var configuration = _configurationRegistry.Get(inboxName);
        await MigrateInboxAsync(configuration, token);
    }

    private async Task MigrateInboxAsync(IInboxConfiguration configuration, CancellationToken token)
    {
        var storageProviderFactory = configuration.StorageProviderFactoryFunc(_serviceProvider);
        var storageProvider = storageProviderFactory.Create(configuration);

        if (storageProvider is ISupportMigration migratable)
        {
            _logger.LogInformation("Running migration for inbox '{InboxName}'...", configuration.InboxName);
            await migratable.MigrateAsync(token);
            _logger.LogInformation("Migration completed for inbox '{InboxName}'.", configuration.InboxName);
        }
        else
        {
            _logger.LogDebug(
                "Rh.Inbox '{InboxName}' storage provider does not support migration, skipping.",
                configuration.InboxName);
        }
    }
}