using Microsoft.Extensions.Hosting;
using Rh.Inbox.Abstractions;

namespace Rh.Inbox.Web;

/// <summary>
/// Hosted service that manages the inbox lifecycle within ASP.NET Core applications.
/// Automatically starts and stops the inbox manager with the application host.
/// </summary>
internal sealed class InboxHostedService : IHostedService
{
    private readonly IInboxManager _inboxManager;

    public InboxHostedService(IInboxManager inboxManager)
    {
        _inboxManager = inboxManager;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        return _inboxManager.StartAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return _inboxManager.StopAsync(cancellationToken);
    }
}
