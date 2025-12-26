namespace Rh.Inbox.Resilience;

/// <summary>
/// Classifies exceptions as transient (retryable) or permanent.
/// Each storage provider implements this for its specific exception types.
/// </summary>
public interface ITransientExceptionClassifier
{
    /// <summary>
    /// Determines if an exception is transient and the operation should be retried.
    /// </summary>
    /// <param name="exception">The exception to classify.</param>
    /// <returns>True if the exception is transient and retry is appropriate; false otherwise.</returns>
    bool IsTransient(Exception exception);
}
