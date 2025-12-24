using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Abstractions.Builders;
using Rh.Inbox.Abstractions.Lifecycle;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Configuration;
using Rh.Inbox.Configuration.Builders;
using Rh.Inbox.Configuration.Registry;
using Rh.Inbox.Inboxes.Factory;
using Rh.Inbox.Lifecycle;
using Rh.Inbox.Management;
using Rh.Inbox.Migration;
using Rh.Inbox.Processing.Strategies.Factory;
using Rh.Inbox.Providers;
using Rh.Inbox.Writers;

namespace Rh.Inbox.Extensions;

/// <summary>
/// Extension methods for registering inbox services with dependency injection.
/// </summary>
public static partial class ServiceCollectionExtensions
{
    private const int MaxInboxNameLength = 128;

    [GeneratedRegex(@"^[a-zA-Z0-9_-]+$", RegexOptions.Compiled)]
    private static partial Regex ValidInboxNamePattern();

    /// <summary>
    /// Adds an inbox with the default name to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">The configuration action for the inbox builder.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddInbox(
        this IServiceCollection services,
        Action<IInboxBuilder> configure)
    {
        return services.AddInbox(InboxOptions.DefaultInboxName, configure);
    }

    /// <summary>
    /// Adds an inbox with the specified name to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="inboxName">The unique name for the inbox.</param>
    /// <param name="configure">The configuration action for the inbox builder.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddInbox(
        this IServiceCollection services,
        string inboxName,
        Action<IInboxBuilder> configure)
    {
        ValidateInboxName(inboxName);
        var registry = GetOrCreateRegistry(services);

        services.TryAddSingleton<IDateTimeProvider, DateTimeProvider>();
        services.TryAddSingleton<IInboxLifecycle>(_ => new InboxLifecycle());
        services.TryAddSingleton<IInboxFactory, InboxFactory>();
        services.TryAddSingleton<IInboxProcessingStrategyFactory, InboxProcessingStrategyFactory>();
        services.TryAddSingleton<InboxManager>();
        services.TryAddSingleton<IInboxManager>(sp => sp.GetRequiredService<InboxManager>());
        services.TryAddSingleton<IInboxManagerInternal>(sp => sp.GetRequiredService<InboxManager>());

        services.TryAddScoped<IInboxWriter, InboxWriter>();
        services.TryAddSingleton<IInboxMigrationService, InboxMigrationService>();

        if (registry.TryGet(inboxName, out _))
        {
            throw new InvalidOperationException(
                $"An inbox with name '{inboxName}' has already been registered. " +
                "Each inbox must have a unique name.");
        }

        var builder = new InboxBuilder(services, inboxName);
        configure(builder);

        var configuration = builder.Build();
        registry.Register(configuration);

        return services;
    }

    private static InboxConfigurationRegistry GetOrCreateRegistry(IServiceCollection services)
    {
        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(InboxConfigurationRegistry));

        if (descriptor?.ImplementationInstance is InboxConfigurationRegistry existingRegistry)
        {
            return existingRegistry;
        }

        var registry = new InboxConfigurationRegistry();
        services.AddSingleton(registry);
        return registry;
    }

    private static void ValidateInboxName(string inboxName)
    {
        if (string.IsNullOrWhiteSpace(inboxName))
        {
            throw new ArgumentException("Inbox name cannot be null or empty.", nameof(inboxName));
        }

        if (inboxName.Length > MaxInboxNameLength)
        {
            throw new ArgumentException(
                $"Inbox name '{inboxName}' exceeds maximum length of {MaxInboxNameLength} characters.",
                nameof(inboxName));
        }

        if (!ValidInboxNamePattern().IsMatch(inboxName))
        {
            throw new ArgumentException(
                $"Inbox name '{inboxName}' contains invalid characters. " +
                "Only letters, digits, hyphens, and underscores are allowed.",
                nameof(inboxName));
        }
    }
}