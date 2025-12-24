using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Rh.Inbox.Abstractions.Lifecycle;

namespace Rh.Inbox.Web;

/// <summary>
/// Extension methods for configuring inbox services for web applications.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Configures the inbox to run as a hosted service, automatically starting and stopping
    /// with the application host. This is the recommended approach for ASP.NET Core applications.
    /// </summary>
    /// <remarks>
    /// This method replaces the default IInboxLifecycle implementation with one that integrates
    /// with the ASP.NET Core hosted service lifecycle. Can be called before or after AddInbox().
    /// </remarks>
    public static IServiceCollection RunInboxAsHostedService(this IServiceCollection services)
    {
        // Remove the default lifecycle if registered
        services.RemoveAll<IInboxLifecycle>();

        // Add the hosted lifecycle and hosted service
        services.TryAddSingleton<IInboxLifecycle, InboxHostedLifecycle>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, InboxHostedService>());

        return services;
    }
}
