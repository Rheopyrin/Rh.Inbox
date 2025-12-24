using Rh.Inbox.Abstractions.Providers;

namespace Rh.Inbox.Providers;

/// <summary>
/// Default implementation of <see cref="IDateTimeProvider"/> that returns the system UTC time.
/// </summary>
public class DateTimeProvider : IDateTimeProvider
{
    /// <inheritdoc />
    public DateTime GetUtcNow() => DateTime.UtcNow;
}