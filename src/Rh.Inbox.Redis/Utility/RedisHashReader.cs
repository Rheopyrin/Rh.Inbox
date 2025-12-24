using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using StackExchange.Redis;

namespace Rh.Inbox.Redis.Utility;

/// <summary>
/// Provides efficient parsing of Redis hash data returned from Lua scripts.
/// </summary>
internal readonly struct RedisHashReader
{
    private readonly Dictionary<string, string> _fields;

    private RedisHashReader(Dictionary<string, string> fields)
    {
        _fields = fields;
    }

    internal static RedisHashReader Create(RedisResult[] hashData)
    {
        var fields = new Dictionary<string, string>(hashData.Length / 2);

        for (var i = 0; i + 1 < hashData.Length; i += 2)
        {
            var key = (string?)hashData[i];
            var value = (string?)hashData[i + 1];

            if (key is not null && value is not null)
                fields[key] = value;
        }

        return new RedisHashReader(fields);
    }

    internal static RedisHashReader Create(HashEntry[] hashEntries)
    {
        var fields = new Dictionary<string, string>(hashEntries.Length);

        foreach (var entry in hashEntries)
        {
            var key = entry.Name.ToString();
            var value = entry.Value.ToString();
            fields[key] = value;
        }

        return new RedisHashReader(fields);
    }

    internal bool TryGetString(string key, [NotNullWhen(true)] out string? value)
    {
        if (_fields.TryGetValue(key, out value) && !string.IsNullOrEmpty(value))
            return true;

        value = null;
        return false;
    }

    internal string? GetString(string key) =>
        _fields.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : null;

    internal bool TryGetGuid(string key, out Guid value)
    {
        if (_fields.TryGetValue(key, out var str) && Guid.TryParse(str, out value))
            return true;

        value = default;
        return false;
    }

    internal int GetInt(string key, int defaultValue = 0) =>
        _fields.TryGetValue(key, out var value) && int.TryParse(value, CultureInfo.InvariantCulture, out var result)
            ? result
            : defaultValue;

    internal DateTime GetDateTime(string key) =>
        _fields.TryGetValue(key, out var value) &&
        DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var result)
            ? result
            : default;

    internal DateTime? GetNullableDateTime(string key) =>
        _fields.TryGetValue(key, out var value) &&
        !string.IsNullOrEmpty(value) &&
        DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var result)
            ? result
            : null;
}