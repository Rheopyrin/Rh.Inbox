using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;

namespace Rh.Inbox.Abstractions.Builders;

/// <summary>
/// Base interface for typed inbox builders with fluent API support.
/// </summary>
/// <typeparam name="TBuilder">The concrete builder type for fluent API chaining.</typeparam>
public interface IInboxBuilderBase<TBuilder> where TBuilder : IInboxBuilderBase<TBuilder>
{
    /// <summary>
    /// Gets the name of the inbox being configured.
    /// </summary>
    string InboxName { get; }

    /// <summary>
    /// Configures services for the inbox storage provider.
    /// Use this to register dependencies needed by the storage provider factory.
    /// </summary>
    /// <param name="configure">The action to configure services.</param>
    /// <returns>The builder for chaining.</returns>
    TBuilder ConfigureServices(Action<IServiceCollection> configure);

    /// <summary>
    /// Registers a post-configuration action that runs after the inbox configuration is built.
    /// </summary>
    /// <param name="action">The action to execute during post-configuration.</param>
    /// <returns>The builder for chaining.</returns>
    TBuilder PostConfigure(Action<IInboxConfiguration, IServiceCollection> action);

    /// <summary>
    /// Configures the inbox options.
    /// </summary>
    /// <param name="configure">The action to configure the options.</param>
    /// <returns>The builder for chaining.</returns>
    TBuilder ConfigureOptions(Action<IConfigureInboxOptions> configure);

    /// <summary>
    /// Configures the inbox to use the specified storage provider factory instance.
    /// </summary>
    /// <param name="factory">The storage provider factory instance.</param>
    /// <returns>The builder for chaining.</returns>
    TBuilder UseStorageProviderFactory(IInboxStorageProviderFactory factory);

    /// <summary>
    /// Configures the inbox to use a storage provider factory resolved from the service provider.
    /// </summary>
    /// <typeparam name="TFactory">The storage provider factory type to resolve.</typeparam>
    /// <returns>The builder for chaining.</returns>
    TBuilder UseStorageProviderFactory<TFactory>()
        where TFactory : class, IInboxStorageProviderFactory;

    /// <summary>
    /// Configures the inbox to use a storage provider factory created by the specified function.
    /// </summary>
    /// <param name="factoryFunc">The function to create the storage provider factory.</param>
    /// <returns>The builder for chaining.</returns>
    TBuilder UseStorageProviderFactory(Func<IServiceProvider, IInboxStorageProviderFactory> factoryFunc);

    /// <summary>
    /// Configures the inbox to use the specified serializer factory instance.
    /// </summary>
    /// <param name="factory">The serializer factory instance.</param>
    /// <returns>The builder for chaining.</returns>
    TBuilder UseSerializerFactory(IInboxSerializerFactory factory);

    /// <summary>
    /// Configures the inbox to use a serializer factory of the specified type.
    /// </summary>
    /// <typeparam name="TFactory">The serializer factory type to use.</typeparam>
    /// <returns>The builder for chaining.</returns>
    TBuilder UseSerializerFactory<TFactory>()
        where TFactory : class, IInboxSerializerFactory;

    /// <summary>
    /// Configures the inbox to use a serializer factory created by the specified function.
    /// </summary>
    /// <typeparam name="TFactory">The serializer factory type.</typeparam>
    /// <param name="factoryFunc">The function to create the serializer factory.</param>
    /// <returns>The builder for chaining.</returns>
    TBuilder UseSerializerFactory<TFactory>(Func<IServiceProvider, TFactory> factoryFunc)
        where TFactory : class, IInboxSerializerFactory;

    /// <summary>
    /// Registers a message type for the inbox without associating a handler.
    /// </summary>
    /// <typeparam name="TMessage">The message type to register.</typeparam>
    /// <param name="messageType">Optional custom message type identifier. If not specified, the type name is used.</param>
    /// <returns>The builder for chaining.</returns>
    TBuilder RegisterMessage<TMessage>(string? messageType = null) where TMessage : class;

    /// <summary>
    /// Configures the health check options for the inbox.
    /// </summary>
    /// <param name="configure">The action to configure the health check options.</param>
    /// <returns>The builder for chaining.</returns>
    TBuilder ConfigureHealthCheck(Action<IInboxHealthCheckOptions> configure);
}
