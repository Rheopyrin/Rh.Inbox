namespace Rh.Inbox.Redis.Provider.Scripts;

/// <summary>
/// Constants for batch script ARGV layout.
/// </summary>
internal static class ScriptLayout
{
    /// <summary>WriteBatch header: pendingKey, msgKeyBase, inboxName, ttlSeconds, dedupTtlSeconds</summary>
    internal const int WriteBatchHeaderSize = 5;

    /// <summary>WriteBatch per message: id, messageType, payload, groupId, dedupId, attempts, receivedAt, score, dedupKey</summary>
    internal const int WriteBatchFieldsPerMessage = 9;

    /// <summary>WriteBatchWithCollapse header: pendingKey, capturedKey, collapseKey, msgKeyBase, inboxName, ttlSeconds, dedupTtlSeconds</summary>
    internal const int WriteBatchWithCollapseHeaderSize = 7;

    /// <summary>WriteBatchWithCollapse per message: id, messageType, payload, groupId, collapseKeyValue, dedupId, attempts, receivedAt, score, dedupKey</summary>
    internal const int WriteBatchWithCollapseFieldsPerMessage = 10;
}