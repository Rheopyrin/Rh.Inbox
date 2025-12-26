using Microsoft.Extensions.Logging;

namespace Rh.Inbox.Resilience;

/// <summary>
/// Executes operations with retry logic for transient failures.
/// </summary>
public sealed class RetryExecutor
{
    private readonly RetryOptions _options;
    private readonly ITransientExceptionClassifier _classifier;
    private readonly ILogger? _logger;

    public RetryExecutor(
        RetryOptions options,
        ITransientExceptionClassifier classifier,
        ILogger? logger = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _classifier = classifier ?? throw new ArgumentNullException(nameof(classifier));
        _logger = logger;
    }

    /// <summary>
    /// Executes an async operation with retry logic.
    /// </summary>
    public async Task ExecuteAsync(
        Func<CancellationToken, Task> operation,
        CancellationToken cancellationToken)
    {
        await ExecuteAsync<object?>(async ct =>
        {
            await operation(ct).ConfigureAwait(false);
            return null;
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes an async operation with retry logic and returns a result.
    /// </summary>
    public async Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        CancellationToken cancellationToken)
    {
        if (_options.MaxRetries <= 0)
        {
            return await operation(cancellationToken).ConfigureAwait(false);
        }

        var attempt = 0;
        var delay = _options.InitialDelay;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                return await operation(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (attempt < _options.MaxRetries && _classifier.IsTransient(ex))
            {
                attempt++;

                var actualDelay = _options.UseJitter
                    ? AddJitter(delay)
                    : delay;

                _logger?.LogWarning(
                    ex,
                    "Transient error occurred. Retry attempt {Attempt}/{MaxRetries} after {DelayMs}ms",
                    attempt,
                    _options.MaxRetries,
                    (int)actualDelay.TotalMilliseconds);

                await Task.Delay(actualDelay, cancellationToken).ConfigureAwait(false);

                delay = CalculateNextDelay(delay);
            }
        }
    }

    private TimeSpan CalculateNextDelay(TimeSpan currentDelay)
    {
        var nextDelayMs = currentDelay.TotalMilliseconds * _options.BackoffMultiplier;
        var clampedMs = Math.Min(nextDelayMs, _options.MaxDelay.TotalMilliseconds);
        return TimeSpan.FromMilliseconds(clampedMs);
    }

    private static TimeSpan AddJitter(TimeSpan delay)
    {
        // Add +/- 25% jitter to prevent thundering herd
        var jitterFactor = 0.75 + (Random.Shared.NextDouble() * 0.5);
        return TimeSpan.FromMilliseconds(delay.TotalMilliseconds * jitterFactor);
    }
}
