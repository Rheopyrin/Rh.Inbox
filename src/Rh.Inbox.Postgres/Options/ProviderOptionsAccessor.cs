using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Postgres.Connection;

namespace Rh.Inbox.Postgres.Options;

internal interface IProviderOptionsAccessor
{
    PostgresInboxProviderOptions GetForInbox(string inboxName);
}

internal sealed class ProviderOptionsAccessor : IProviderOptionsAccessor
{
    private readonly INpgsqlDataSourceProvider _dataSourceProvider;
    private readonly IServiceProvider _serviceProvider;

    public ProviderOptionsAccessor(INpgsqlDataSourceProvider dataSourceProvider, IServiceProvider serviceProvider)
    {
        _dataSourceProvider = dataSourceProvider;
        _serviceProvider = serviceProvider;
    }

    public PostgresInboxProviderOptions GetForInbox(string inboxName)
    {
        var options = _serviceProvider.GetRequiredKeyedService<PostgresInboxOptions>(inboxName);

        return new PostgresInboxProviderOptions
        {
            DataSource = _dataSourceProvider.GetDataSource(options.ConnectionString),
            TableName = options.TableName ?? Utility.PostgresIdentifierHelper.BuildTableName(PostgresInboxOptions.DefaultTablePrefix, inboxName),
            DeadLetterTableName = options.DeadLetterTableName ?? Utility.PostgresIdentifierHelper.BuildTableName(PostgresInboxOptions.DefaultDeadLetterTablePrefix, inboxName),
            DeduplicationTableName = options.DeduplicationTableName ?? Utility.PostgresIdentifierHelper.BuildTableName(PostgresInboxOptions.DefaultDeduplicationTablePrefix, inboxName),
            GroupLocksTableName = Utility.PostgresIdentifierHelper.BuildTableName(PostgresInboxOptions.DefaultGroupLocksTablePrefix, inboxName),
            Retry = options.Retry
        };
    }
}