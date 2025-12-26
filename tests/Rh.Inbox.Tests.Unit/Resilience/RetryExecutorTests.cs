using FluentAssertions;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Rh.Inbox.Resilience;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Resilience;

public class RetryExecutorTests
{
    private readonly ITransientExceptionClassifier _classifier;
    private readonly ILogger _logger;

    public RetryExecutorTests()
    {
        _classifier = Substitute.For<ITransientExceptionClassifier>();
        _logger = Substitute.For<ILogger>();
    }

    #region Successful Execution

    [Fact]
    public async Task ExecuteAsync_SuccessfulOperation_ReturnsResult()
    {
        var options = RetryOptions.Default;
        var executor = new RetryExecutor(options, _classifier, _logger);
        var expected = 42;

        var result = await executor.ExecuteAsync(
            ct => Task.FromResult(expected),
            CancellationToken.None);

        result.Should().Be(expected);
    }

    [Fact]
    public async Task ExecuteAsync_SuccessfulOperation_DoesNotRetry()
    {
        var options = RetryOptions.Default;
        var executor = new RetryExecutor(options, _classifier, _logger);
        var callCount = 0;

        await executor.ExecuteAsync(ct =>
        {
            callCount++;
            return Task.FromResult(true);
        }, CancellationToken.None);

        callCount.Should().Be(1);
    }

    [Fact]
    public async Task ExecuteAsync_VoidOperation_Completes()
    {
        var options = RetryOptions.Default;
        var executor = new RetryExecutor(options, _classifier, _logger);
        var executed = false;

        await executor.ExecuteAsync(ct =>
        {
            executed = true;
            return Task.CompletedTask;
        }, CancellationToken.None);

        executed.Should().BeTrue();
    }

    #endregion

    #region Retry on Transient Exceptions

    [Fact]
    public async Task ExecuteAsync_TransientException_RetriesAndSucceeds()
    {
        var options = new RetryOptions { MaxRetries = 3, InitialDelay = TimeSpan.FromMilliseconds(1) };
        var executor = new RetryExecutor(options, _classifier, _logger);
        var callCount = 0;
        var transientException = new InvalidOperationException("Transient");

        _classifier.IsTransient(transientException).Returns(true);

        var result = await executor.ExecuteAsync(ct =>
        {
            callCount++;
            if (callCount < 3)
            {
                throw transientException;
            }
            return Task.FromResult("success");
        }, CancellationToken.None);

        result.Should().Be("success");
        callCount.Should().Be(3);
    }

    [Fact]
    public async Task ExecuteAsync_TransientException_LogsWarningOnEachRetry()
    {
        var options = new RetryOptions { MaxRetries = 3, InitialDelay = TimeSpan.FromMilliseconds(1) };
        var executor = new RetryExecutor(options, _classifier, _logger);
        var callCount = 0;
        var transientException = new InvalidOperationException("Transient");

        _classifier.IsTransient(transientException).Returns(true);

        await executor.ExecuteAsync(ct =>
        {
            callCount++;
            if (callCount < 3)
            {
                throw transientException;
            }
            return Task.FromResult(true);
        }, CancellationToken.None);

        _logger.Received(2).Log(
            LogLevel.Warning,
            Arg.Any<EventId>(),
            Arg.Any<object>(),
            transientException,
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Fact]
    public async Task ExecuteAsync_MaxRetriesExhausted_ThrowsException()
    {
        var options = new RetryOptions { MaxRetries = 2, InitialDelay = TimeSpan.FromMilliseconds(1) };
        var executor = new RetryExecutor(options, _classifier, _logger);
        var transientException = new InvalidOperationException("Transient");

        _classifier.IsTransient(transientException).Returns(true);

        var act = () => executor.ExecuteAsync<int>(ct =>
        {
            throw transientException;
        }, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Transient");
    }

    [Fact]
    public async Task ExecuteAsync_MaxRetriesExhausted_AttemptsCorrectNumberOfTimes()
    {
        var options = new RetryOptions { MaxRetries = 3, InitialDelay = TimeSpan.FromMilliseconds(1) };
        var executor = new RetryExecutor(options, _classifier, _logger);
        var callCount = 0;
        var transientException = new InvalidOperationException("Transient");

        _classifier.IsTransient(transientException).Returns(true);

        try
        {
            await executor.ExecuteAsync<int>(ct =>
            {
                callCount++;
                throw transientException;
            }, CancellationToken.None);
        }
        catch
        {
            // Expected
        }

        // Initial attempt + 3 retries = 4 total attempts
        callCount.Should().Be(4);
    }

    #endregion

    #region Non-Transient Exceptions

    [Fact]
    public async Task ExecuteAsync_NonTransientException_DoesNotRetry()
    {
        var options = new RetryOptions { MaxRetries = 3, InitialDelay = TimeSpan.FromMilliseconds(1) };
        var executor = new RetryExecutor(options, _classifier, _logger);
        var callCount = 0;
        var nonTransientException = new ArgumentException("Non-transient");

        _classifier.IsTransient(nonTransientException).Returns(false);

        var act = () => executor.ExecuteAsync<int>(ct =>
        {
            callCount++;
            throw nonTransientException;
        }, CancellationToken.None);

        await act.Should().ThrowAsync<ArgumentException>();
        callCount.Should().Be(1);
    }

    [Fact]
    public async Task ExecuteAsync_NonTransientException_DoesNotLog()
    {
        var options = new RetryOptions { MaxRetries = 3, InitialDelay = TimeSpan.FromMilliseconds(1) };
        var executor = new RetryExecutor(options, _classifier, _logger);
        var nonTransientException = new ArgumentException("Non-transient");

        _classifier.IsTransient(nonTransientException).Returns(false);

        try
        {
            await executor.ExecuteAsync<int>(ct => throw nonTransientException, CancellationToken.None);
        }
        catch
        {
            // Expected
        }

        _logger.DidNotReceive().Log(
            Arg.Any<LogLevel>(),
            Arg.Any<EventId>(),
            Arg.Any<object>(),
            Arg.Any<Exception>(),
            Arg.Any<Func<object, Exception?, string>>());
    }

    #endregion

    #region Cancellation

    [Fact]
    public async Task ExecuteAsync_CancellationRequested_ThrowsOperationCanceledException()
    {
        var options = RetryOptions.Default;
        var executor = new RetryExecutor(options, _classifier, _logger);
        var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var act = () => executor.ExecuteAsync(
            ct => Task.FromResult(1),
            cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task ExecuteAsync_CancellationDuringRetryDelay_ThrowsOperationCanceledException()
    {
        var options = new RetryOptions { MaxRetries = 3, InitialDelay = TimeSpan.FromSeconds(10) };
        var executor = new RetryExecutor(options, _classifier, _logger);
        var cts = new CancellationTokenSource();
        var transientException = new InvalidOperationException("Transient");

        _classifier.IsTransient(transientException).Returns(true);

        var task = executor.ExecuteAsync<int>(ct =>
        {
            throw transientException;
        }, cts.Token);

        // Cancel after a short delay (before retry delay completes)
        await Task.Delay(50);
        await cts.CancelAsync();

        var act = () => task;
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task ExecuteAsync_OperationThrowsOperationCanceledException_DoesNotRetry()
    {
        var options = new RetryOptions { MaxRetries = 3, InitialDelay = TimeSpan.FromMilliseconds(1) };
        var executor = new RetryExecutor(options, _classifier, _logger);
        var callCount = 0;
        var cts = new CancellationTokenSource();

        // Don't cancel yet - let the operation start
        var act = () => executor.ExecuteAsync<int>(ct =>
        {
            callCount++;
            // Cancel and throw inside the operation
            cts.Cancel();
            throw new OperationCanceledException(cts.Token);
        }, cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
        callCount.Should().Be(1);
    }

    #endregion

    #region Retry Disabled

    [Fact]
    public async Task ExecuteAsync_RetryDisabled_DoesNotRetry()
    {
        var options = RetryOptions.None;
        var executor = new RetryExecutor(options, _classifier, _logger);
        var callCount = 0;
        var transientException = new InvalidOperationException("Transient");

        _classifier.IsTransient(transientException).Returns(true);

        var act = () => executor.ExecuteAsync<int>(ct =>
        {
            callCount++;
            throw transientException;
        }, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>();
        callCount.Should().Be(1);
    }

    [Fact]
    public async Task ExecuteAsync_ZeroMaxRetries_ExecutesOnce()
    {
        var options = new RetryOptions { MaxRetries = 0 };
        var executor = new RetryExecutor(options, _classifier, _logger);

        var result = await executor.ExecuteAsync(
            ct => Task.FromResult(42),
            CancellationToken.None);

        result.Should().Be(42);
    }

    #endregion

    #region Exponential Backoff

    [Fact]
    public async Task ExecuteAsync_ExponentialBackoff_DelaysIncrease()
    {
        var options = new RetryOptions
        {
            MaxRetries = 3,
            InitialDelay = TimeSpan.FromMilliseconds(50),
            BackoffMultiplier = 2.0,
            UseJitter = false
        };
        var executor = new RetryExecutor(options, _classifier, _logger);
        var timestamps = new List<DateTime>();
        var transientException = new InvalidOperationException("Transient");

        _classifier.IsTransient(transientException).Returns(true);

        try
        {
            await executor.ExecuteAsync<int>(ct =>
            {
                timestamps.Add(DateTime.UtcNow);
                throw transientException;
            }, CancellationToken.None);
        }
        catch
        {
            // Expected
        }

        // Verify delays increase (with some tolerance for test execution time)
        timestamps.Should().HaveCount(4); // 1 initial + 3 retries

        var delay1 = (timestamps[1] - timestamps[0]).TotalMilliseconds;
        var delay2 = (timestamps[2] - timestamps[1]).TotalMilliseconds;
        var delay3 = (timestamps[3] - timestamps[2]).TotalMilliseconds;

        // Each delay should be roughly double the previous (with tolerance)
        delay1.Should().BeGreaterOrEqualTo(40); // ~50ms
        delay2.Should().BeGreaterOrEqualTo(80); // ~100ms
        delay3.Should().BeGreaterOrEqualTo(160); // ~200ms
    }

    [Fact]
    public async Task ExecuteAsync_MaxDelayRespected_DoesNotExceedMaxDelay()
    {
        var options = new RetryOptions
        {
            MaxRetries = 5,
            InitialDelay = TimeSpan.FromMilliseconds(100),
            MaxDelay = TimeSpan.FromMilliseconds(150),
            BackoffMultiplier = 2.0,
            UseJitter = false
        };
        var executor = new RetryExecutor(options, _classifier, _logger);
        var timestamps = new List<DateTime>();
        var transientException = new InvalidOperationException("Transient");

        _classifier.IsTransient(transientException).Returns(true);

        try
        {
            await executor.ExecuteAsync<int>(ct =>
            {
                timestamps.Add(DateTime.UtcNow);
                throw transientException;
            }, CancellationToken.None);
        }
        catch
        {
            // Expected
        }

        // Later delays should be capped at MaxDelay
        for (var i = 2; i < timestamps.Count; i++)
        {
            var delay = (timestamps[i] - timestamps[i - 1]).TotalMilliseconds;
            delay.Should().BeLessThan(200); // MaxDelay + tolerance
        }
    }

    #endregion

    #region Constructor Validation

    [Fact]
    public void Constructor_NullOptions_ThrowsArgumentNullException()
    {
        var act = () => new RetryExecutor(null!, _classifier, _logger);

        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("options");
    }

    [Fact]
    public void Constructor_NullClassifier_ThrowsArgumentNullException()
    {
        var act = () => new RetryExecutor(RetryOptions.Default, null!, _logger);

        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("classifier");
    }

    [Fact]
    public void Constructor_NullLogger_DoesNotThrow()
    {
        var act = () => new RetryExecutor(RetryOptions.Default, _classifier, null);

        act.Should().NotThrow();
    }

    #endregion

    #region RetryOptions Tests

    [Fact]
    public void RetryOptions_Default_HasExpectedValues()
    {
        var options = RetryOptions.Default;

        options.MaxRetries.Should().Be(3);
        options.InitialDelay.Should().Be(TimeSpan.FromMilliseconds(100));
        options.MaxDelay.Should().Be(TimeSpan.FromSeconds(5));
        options.BackoffMultiplier.Should().Be(2.0);
        options.UseJitter.Should().BeTrue();
    }

    [Fact]
    public void RetryOptions_None_HasZeroRetries()
    {
        var options = RetryOptions.None;

        options.MaxRetries.Should().Be(0);
    }

    #endregion
}
