using System.Text.RegularExpressions;

namespace Rh.Inbox.Redis.Utility;

internal static partial class RedisKeyHelper
{
    internal const int MaxKeyPrefixLength = 100;

    // Valid Redis key prefix: alphanumeric, underscores, hyphens, colons
    [GeneratedRegex(@"^[a-zA-Z0-9_:-]+$", RegexOptions.Compiled)]
    private static partial Regex SafeKeyPrefixPattern();

    [GeneratedRegex(@"[^a-z0-9_-]", RegexOptions.Compiled)]
    private static partial Regex InvalidKeyCharsPattern();

    internal static bool IsValidKeyPrefix(string keyPrefix)
    {
        return !string.IsNullOrWhiteSpace(keyPrefix)
               && keyPrefix.Length <= MaxKeyPrefixLength
               && SafeKeyPrefixPattern().IsMatch(keyPrefix);
    }

    internal static string BuildKeyPrefix(string defaultPrefix, string inboxName)
    {
        var sanitized = SanitizeKeyPart(inboxName);
        var keyPrefix = $"{defaultPrefix}:{sanitized}";

        if (keyPrefix.Length > MaxKeyPrefixLength)
        {
            keyPrefix = keyPrefix[..MaxKeyPrefixLength];
        }

        return keyPrefix;
    }

    internal static string SanitizeKeyPart(string name)
    {
        // Convert to lowercase and replace invalid characters with underscore
        var sanitized = InvalidKeyCharsPattern().Replace(name.ToLowerInvariant(), "_");

        // Ensure it doesn't start with a digit
        if (sanitized.Length > 0 && char.IsDigit(sanitized[0]))
        {
            sanitized = "_" + sanitized;
        }

        return sanitized;
    }
}