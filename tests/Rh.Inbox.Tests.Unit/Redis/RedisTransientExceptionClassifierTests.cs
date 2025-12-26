using FluentAssertions;
using Rh.Inbox.Redis.Resilience;
using StackExchange.Redis;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Redis;

public class RedisTransientExceptionClassifierTests
{
    private readonly RedisTransientExceptionClassifier _classifier = new();

    #region RedisConnectionException

    [Fact]
    public void IsTransient_RedisConnectionException_ReturnsTrue()
    {
        var exception = new RedisConnectionException(ConnectionFailureType.UnableToConnect, "Connection failed");

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue();
    }

    [Theory]
    [InlineData(ConnectionFailureType.UnableToConnect)]
    [InlineData(ConnectionFailureType.SocketFailure)]
    [InlineData(ConnectionFailureType.SocketClosed)]
    [InlineData(ConnectionFailureType.InternalFailure)]
    [InlineData(ConnectionFailureType.ConnectionDisposed)]
    [InlineData(ConnectionFailureType.Loading)]
    [InlineData(ConnectionFailureType.UnableToResolvePhysicalConnection)]
    public void IsTransient_RedisConnectionException_AllFailureTypes_ReturnsTrue(ConnectionFailureType failureType)
    {
        var exception = new RedisConnectionException(failureType, "Connection failed");

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue($"ConnectionFailureType.{failureType} should be transient");
    }

    #endregion

    #region RedisTimeoutException

    [Fact]
    public void IsTransient_RedisTimeoutException_ReturnsTrue()
    {
        var exception = new RedisTimeoutException("Timeout waiting for response", CommandStatus.Unknown);

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue();
    }

    #endregion

    #region RedisServerException

    [Fact]
    public void IsTransient_RedisServerException_OOM_ReturnsTrue()
    {
        var exception = new RedisServerException("OOM command not allowed when used memory > 'maxmemory'");

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_RedisServerException_BUSY_ReturnsTrue()
    {
        var exception = new RedisServerException("BUSY Redis is busy running a script");

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_RedisServerException_LOADING_ReturnsTrue()
    {
        var exception = new RedisServerException("LOADING Redis is loading the dataset in memory");

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_RedisServerException_CLUSTERDOWN_ReturnsTrue()
    {
        var exception = new RedisServerException("CLUSTERDOWN The cluster is down");

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_RedisServerException_READONLY_ReturnsTrue()
    {
        var exception = new RedisServerException("READONLY You can't write against a read only replica");

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_RedisServerException_NonTransient_ReturnsFalse()
    {
        var exception = new RedisServerException("ERR unknown command 'INVALID'");

        var result = _classifier.IsTransient(exception);

        result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_RedisServerException_WrongType_ReturnsFalse()
    {
        var exception = new RedisServerException("WRONGTYPE Operation against a key holding the wrong kind of value");

        var result = _classifier.IsTransient(exception);

        result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_RedisServerException_EmptyMessage_ReturnsFalse()
    {
        var exception = new RedisServerException("");

        var result = _classifier.IsTransient(exception);

        result.Should().BeFalse();
    }

    #endregion

    #region Generic RedisException

    [Fact]
    public void IsTransient_RedisException_ConnectionMessage_ReturnsTrue()
    {
        var exception = new RedisException("No connection is active/available");

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_RedisException_TimeoutMessage_ReturnsTrue()
    {
        var exception = new RedisException("Timeout performing GET");

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_RedisException_SocketMessage_ReturnsTrue()
    {
        var exception = new RedisException("Socket closed");

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_RedisException_NonTransientMessage_ReturnsFalse()
    {
        var exception = new RedisException("Some other error");

        var result = _classifier.IsTransient(exception);

        result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_RedisException_EmptyMessage_ReturnsFalse()
    {
        var exception = new RedisException("");

        var result = _classifier.IsTransient(exception);

        result.Should().BeFalse();
    }

    #endregion

    #region TimeoutException

    [Fact]
    public void IsTransient_TimeoutException_ReturnsTrue()
    {
        var exception = new TimeoutException("Operation timed out");

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue();
    }

    #endregion

    #region OperationCanceledException

    [Fact]
    public void IsTransient_OperationCanceledException_ReturnsFalse()
    {
        var exception = new OperationCanceledException("Operation was canceled");

        var result = _classifier.IsTransient(exception);

        result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_TaskCanceledException_ReturnsFalse()
    {
        var exception = new TaskCanceledException("Task was canceled");

        var result = _classifier.IsTransient(exception);

        result.Should().BeFalse();
    }

    #endregion

    #region Other Exception Types

    [Fact]
    public void IsTransient_ArgumentException_ReturnsFalse()
    {
        var exception = new ArgumentException("Invalid argument");

        var result = _classifier.IsTransient(exception);

        result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_InvalidOperationException_ReturnsFalse()
    {
        var exception = new InvalidOperationException("Invalid operation");

        var result = _classifier.IsTransient(exception);

        result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_NullException_ReturnsFalse()
    {
        // This shouldn't happen in practice, but test defensive behavior
        var result = _classifier.IsTransient(null!);

        result.Should().BeFalse();
    }

    #endregion

    #region Case Insensitivity

    [Theory]
    [InlineData("OOM")]
    [InlineData("oom")]
    [InlineData("Oom")]
    public void IsTransient_RedisServerException_OOM_CaseInsensitive(string oomVariant)
    {
        var exception = new RedisServerException($"{oomVariant} command not allowed");

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue();
    }

    [Theory]
    [InlineData("BUSY")]
    [InlineData("busy")]
    [InlineData("Busy")]
    public void IsTransient_RedisServerException_BUSY_CaseInsensitive(string busyVariant)
    {
        var exception = new RedisServerException($"{busyVariant} Redis is busy");

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue();
    }

    [Theory]
    [InlineData("CONNECTION")]
    [InlineData("connection")]
    [InlineData("Connection")]
    public void IsTransient_RedisException_Connection_CaseInsensitive(string connectionVariant)
    {
        var exception = new RedisException($"No {connectionVariant} available");

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue();
    }

    #endregion
}
