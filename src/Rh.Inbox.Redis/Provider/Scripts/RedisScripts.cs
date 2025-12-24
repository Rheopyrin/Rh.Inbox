namespace Rh.Inbox.Redis.Provider.Scripts;

/// <summary>
/// Lua scripts for Redis inbox operations.
/// Uses LuaScript.Prepare() for fixed-param scripts and const string for variable-length batch scripts.
/// </summary>
internal static partial class RedisScripts
{
    /// <summary>
    /// Scan multiplier for Default mode.
    /// Scans more messages than batch size to account for already-captured messages.
    /// </summary>
    private const int DefaultScanMultiplier = 3;

    /// <summary>
    /// Scan multiplier for FIFO mode.
    /// Higher than Default because group locking may skip more messages.
    /// </summary>
    private const int FifoScanMultiplier = 5;
}
