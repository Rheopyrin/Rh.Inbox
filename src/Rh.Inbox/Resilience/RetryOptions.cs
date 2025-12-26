namespace Rh.Inbox.Resilience;

/// <summary>
/// Configuration options for retry behavior on transient storage operations.
/// </summary>
public sealed class RetryOptions
{
    /// <summary>
    /// Maximum number of retry attempts. Default is 3.
    /// </summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    /// Initial delay before the first retry. Default is 100ms.
    /// </summary>
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Maximum delay between retries. Default is 5 seconds.
    /// </summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Exponential backoff multiplier. Default is 2.0.
    /// </summary>
    public double BackoffMultiplier { get; set; } = 2.0;

    /// <summary>
    /// Whether to add jitter to delay to prevent thundering herd. Default is true.
    /// </summary>
    public bool UseJitter { get; set; } = true;

    /// <summary>
    /// Gets the default retry options (3 retries with exponential backoff).
    /// </summary>
    public static RetryOptions Default => new();

    /// <summary>
    /// Gets options with no retry (for testing or explicit disable).
    /// </summary>
    public static RetryOptions None => new() { MaxRetries = 0 };
}
