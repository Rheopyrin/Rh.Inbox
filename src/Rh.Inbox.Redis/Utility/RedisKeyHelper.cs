using System.Text.RegularExpressions;

namespace Rh.Inbox.Redis.Utility;

internal static partial class RedisKeyHelper
{
    internal const int MaxKeyPrefixLength = 100;

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
        var sanitized = InvalidKeyCharsPattern().Replace(name.ToLowerInvariant(), "_");

        if (sanitized.Length > 0 && char.IsDigit(sanitized[0]))
        {
            sanitized = "_" + sanitized;
        }

        return sanitized;
    }
}