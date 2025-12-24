using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Configuration.Registry;
using Rh.Inbox.Management;

namespace Rh.Inbox.Health;

/// <summary>
/// Extension methods for adding inbox health checks.
/// </summary>
public static class HealthCheckBuilderExtensions
{
    /// <summary>
    /// Adds health checks for all registered inboxes that have health checks enabled.
    /// </summary>
    /// <param name="builder">The health checks builder.</param>
    /// <returns>The health checks builder for chaining.</returns>
    public static IHealthChecksBuilder AddInboxHealthChecks(this IHealthChecksBuilder builder)
    {
        var registry = GetRegistryFromServices(builder.Services);
        if (registry == null)
        {
            return builder;
        }

        foreach (var configuration in registry.GetAll())
        {
            if (!configuration.HealthCheckOptions.Enabled)
            {
                continue;
            }

            var healthCheckName = $"inbox:{configuration.InboxName}";
            var options = configuration.HealthCheckOptions;

            builder.Add(new HealthCheckRegistration(
                healthCheckName,
                sp => CreateHealthCheck(sp, configuration.InboxName, options),
                failureStatus: HealthStatus.Unhealthy,
                tags: options.Tags));
        }

        return builder;
    }

    private static InboxConfigurationRegistry? GetRegistryFromServices(IServiceCollection services)
    {
        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(InboxConfigurationRegistry));
        return descriptor?.ImplementationInstance as InboxConfigurationRegistry;
    }

    private static InboxHealthCheck CreateHealthCheck(
        IServiceProvider serviceProvider,
        string inboxName,
        IInboxHealthCheckOptions options)
    {
        var inboxManager = serviceProvider.GetRequiredService<InboxManager>();
        var dateTimeProvider = serviceProvider.GetRequiredService<IDateTimeProvider>();

        return new InboxHealthCheck(inboxManager, inboxName, options, dateTimeProvider);
    }
}