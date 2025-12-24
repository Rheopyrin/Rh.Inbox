using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Rh.Inbox.Abstractions.Builders;
using Rh.Inbox.Abstractions.Lifecycle;
using Rh.Inbox.InMemory.Options;

namespace Rh.Inbox.InMemory;

/// <summary>
/// Extension methods for configuring in-memory inbox storage.
/// </summary>
public static class InMemoryInboxBuilderExtensions
{
    /// <summary>
    /// Configures the inbox to use in-memory storage.
    /// </summary>
    /// <typeparam name="TBuilder">The builder type.</typeparam>
    /// <param name="builder">The inbox builder.</param>
    /// <returns>The builder for chaining.</returns>
    public static TBuilder UseInMemory<TBuilder>(this TBuilder builder)
        where TBuilder : IInboxBuilderBase<TBuilder>
    {
        return builder.UseInMemory(configureOptions: null);
    }

    /// <summary>
    /// Configures the inbox to use in-memory storage with custom options.
    /// </summary>
    /// <typeparam name="TBuilder">The builder type.</typeparam>
    /// <param name="builder">The inbox builder.</param>
    /// <param name="configureOptions">Optional action to configure in-memory options.</param>
    /// <returns>The builder for chaining.</returns>
    public static TBuilder UseInMemory<TBuilder>(this TBuilder builder, Action<InMemoryInboxOptions>? configureOptions)
        where TBuilder : IInboxBuilderBase<TBuilder>
    {
        var options = new InMemoryInboxOptions();
        configureOptions?.Invoke(options);

        builder.ConfigureServices(services =>
        {
            services.TryAddKeyedSingleton(builder.InboxName, options);
            services.TryAddSingleton<IProviderOptionsAccessor, ProviderOptionsAccessor>();
            services.TryAddSingleton<InMemoryInboxStorageProviderFactory>();
            services.TryAddKeyedSingleton(builder.InboxName, options);
        });

        builder.UseStorageProviderFactory<InMemoryInboxStorageProviderFactory>();

        builder.PostConfigure((configuration, collection) =>
        {
            // Register deduplication cleanup service when deduplication is enabled
            var dedupInterval = configuration.Options.DeduplicationInterval;
            if (configuration.Options.EnableDeduplication && dedupInterval > TimeSpan.Zero)
            {
                collection.AddSingleton<IInboxLifecycleHook>(sp =>
                    ActivatorUtilities.CreateInstance<InMemoryDeduplicationCleanupService>(sp, configuration, options.DeduplicationCleanup));
            }

            // Register dead letter cleanup service when enabled
            var dlqLifetime = configuration.Options.DeadLetterMaxMessageLifetime;
            if (configuration.Options.EnableDeadLetter && dlqLifetime > TimeSpan.Zero)
            {
                collection.AddSingleton<IInboxLifecycleHook>(sp =>
                    ActivatorUtilities.CreateInstance<InMemoryDeadLetterCleanupService>(sp, configuration, options.DeadLetterCleanup));
            }
        });

        return builder;
    }
}