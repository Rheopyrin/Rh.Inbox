using System.Text.Json;
using Rh.Inbox.Abstractions.Serialization;

namespace Rh.Inbox.Serialization;

/// <summary>
/// System.Text.Json implementation of <see cref="IInboxMessagePayloadSerializer"/>.
/// </summary>
public class SystemTextJsonInboxMessagePayloadSerializer : IInboxMessagePayloadSerializer
{
    private readonly JsonSerializerOptions _options;

    /// <summary>
    /// Initializes a new instance with default JSON options.
    /// </summary>
    public SystemTextJsonInboxMessagePayloadSerializer()
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
    public SystemTextJsonInboxMessagePayloadSerializer(JsonSerializerOptions options)
    {
        _options = options;
    }

    /// <inheritdoc />
    public string Serialize<T>(T message)
    {
        return JsonSerializer.Serialize(message, _options);
    }

    /// <inheritdoc />
    public string Serialize(object message, Type type)
    {
        return JsonSerializer.Serialize(message, type, _options);
    }

    /// <inheritdoc />
    public T? Deserialize<T>(string payload)
    {
        return JsonSerializer.Deserialize<T>(payload, _options);
    }

    /// <inheritdoc />
    public object? Deserialize(string payload, Type type)
    {
        return JsonSerializer.Deserialize(payload, type, _options);
    }
}