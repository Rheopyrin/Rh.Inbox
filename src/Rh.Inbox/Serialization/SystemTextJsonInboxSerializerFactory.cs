using System.Text.Json;
using Rh.Inbox.Abstractions.Serialization;

namespace Rh.Inbox.Serialization;

/// <summary>
/// Default factory that creates System.Text.Json-based serializers.
/// </summary>
public class SystemTextJsonInboxSerializerFactory : IInboxSerializerFactory
{
    private readonly JsonSerializerOptions _options;

    /// <summary>
    /// Initializes a new instance with default JSON options.
    /// </summary>
    public SystemTextJsonInboxSerializerFactory()
        : this(new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        })
    {
    }

    /// <summary>
    /// Initializes a new instance with the specified JSON options.
    /// </summary>
    /// <param name="options">The JSON serializer options to use.</param>
    public SystemTextJsonInboxSerializerFactory(JsonSerializerOptions options)
    {
        _options = options;
    }

    /// <inheritdoc />
    public IInboxMessagePayloadSerializer Create(string inboxName)
    {
        return new SystemTextJsonInboxMessagePayloadSerializer(_options);
    }
}