using System.Text.RegularExpressions;

namespace Rh.Inbox.Postgres.Utility;

internal static partial class PostgresIdentifierHelper
{
    internal const int MaxPostgresIdentifierLength = 63;

    [GeneratedRegex(@"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$", RegexOptions.Compiled)]
    private static partial Regex SafeIdentifierPattern();

    [GeneratedRegex(@"[^a-z0-9_]", RegexOptions.Compiled)]
    private static partial Regex InvalidIdentifierCharsPattern();

    internal static bool IsValidIdentifier(string identifier)
    {
        return !string.IsNullOrWhiteSpace(identifier) && SafeIdentifierPattern().IsMatch(identifier);
    }

    internal static string BuildTableName(string prefix, string inboxName)
    {
        var sanitized = SanitizeIdentifier(inboxName);
        var tableName = $"{prefix}_{sanitized}";

        if (tableName.Length > MaxPostgresIdentifierLength)
        {
            tableName = tableName[..MaxPostgresIdentifierLength];
        }

        return tableName;
    }

    internal static string SanitizeIdentifier(string name)
    {
        // Convert to lowercase and replace invalid characters with underscore
        var sanitized = InvalidIdentifierCharsPattern().Replace(name.ToLowerInvariant(), "_");

        // Ensure it starts with a letter or underscore
        if (sanitized.Length > 0 && char.IsDigit(sanitized[0]))
        {
            sanitized = "_" + sanitized;
        }

        return sanitized;
    }
}