namespace Rh.Inbox.Abstractions.Providers;

/// <summary>
/// Abstraction for providing the current UTC time.
/// </summary>
public interface IDateTimeProvider
{
    /// <summary>
    /// Gets the current UTC date and time.
    /// </summary>
    /// <returns>The current UTC date and time.</returns>
    DateTime GetUtcNow();
}