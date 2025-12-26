using FluentAssertions;
using Npgsql;
using Rh.Inbox.Postgres.Resilience;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Postgres;

public class PostgresTransientExceptionClassifierTests
{
    private readonly PostgresTransientExceptionClassifier _classifier = new();

    #region Deadlock Detection

    [Theory]
    [InlineData("40001")] // serialization_failure
    [InlineData("40P01")] // deadlock_detected
    [InlineData("40003")] // statement_completion_unknown
    public void IsTransient_DeadlockSqlState_ReturnsTrue(string sqlState)
    {
        var exception = CreatePostgresException(sqlState);

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue($"SQL state {sqlState} should be transient");
    }

    #endregion

    #region Connection Errors

    [Theory]
    [InlineData("08000")] // connection_exception
    [InlineData("08003")] // connection_does_not_exist
    [InlineData("08006")] // connection_failure
    [InlineData("08001")] // sqlclient_unable_to_establish_sqlconnection
    [InlineData("08004")] // sqlserver_rejected_establishment_of_sqlconnection
    [InlineData("08007")] // transaction_resolution_unknown
    public void IsTransient_ConnectionErrorSqlState_ReturnsTrue(string sqlState)
    {
        var exception = CreatePostgresException(sqlState);

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue($"SQL state {sqlState} should be transient");
    }

    #endregion

    #region Resource Errors

    [Theory]
    [InlineData("53000")] // insufficient_resources
    [InlineData("53100")] // disk_full
    [InlineData("53200")] // out_of_memory
    [InlineData("53300")] // too_many_connections
    public void IsTransient_ResourceErrorSqlState_ReturnsTrue(string sqlState)
    {
        var exception = CreatePostgresException(sqlState);

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue($"SQL state {sqlState} should be transient");
    }

    #endregion

    #region Server State Errors

    [Fact]
    public void IsTransient_ServerStartingUp_ReturnsTrue()
    {
        var exception = CreatePostgresException("57P03"); // cannot_connect_now

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_QueryCanceled_ReturnsTrue()
    {
        var exception = CreatePostgresException("57014"); // query_canceled

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue();
    }

    #endregion

    #region Non-Transient SQL States

    [Theory]
    [InlineData("23505")] // unique_violation
    [InlineData("23503")] // foreign_key_violation
    [InlineData("42P01")] // undefined_table
    [InlineData("42703")] // undefined_column
    [InlineData("22001")] // string_data_right_truncation
    [InlineData("22P02")] // invalid_text_representation
    public void IsTransient_NonTransientSqlState_ReturnsFalse(string sqlState)
    {
        var exception = CreatePostgresException(sqlState);

        var result = _classifier.IsTransient(exception);

        result.Should().BeFalse($"SQL state {sqlState} should not be transient");
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

    [Fact]
    public void IsTransient_TimeoutExceptionWithInner_ReturnsTrue()
    {
        var innerException = new InvalidOperationException("Inner");
        var exception = new TimeoutException("Operation timed out", innerException);

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

    #region NpgsqlException

    [Fact]
    public void IsTransient_NpgsqlExceptionWithSocketException_ReturnsTrue()
    {
        // Socket exceptions are transient - connection issues
        var socketException = new System.Net.Sockets.SocketException();
        var exception = new NpgsqlException("Connection failed", socketException);

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_NpgsqlExceptionWithIOException_ReturnsTrue()
    {
        // IO exceptions are typically transient
        var ioException = new System.IO.IOException("Network error");
        var exception = new NpgsqlException("Connection failed", ioException);

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_NpgsqlExceptionWithoutTransientInner_ReturnsFalse()
    {
        var exception = new NpgsqlException("Non-transient error");

        var result = _classifier.IsTransient(exception);

        result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_NpgsqlExceptionWithTransientPostgresInner_ReturnsTrue()
    {
        var innerException = CreatePostgresException("40001"); // deadlock
        var exception = new NpgsqlException("Wrapper", innerException);

        var result = _classifier.IsTransient(exception);

        result.Should().BeTrue();
    }

    [Fact]
    public void IsTransient_NpgsqlExceptionWithNonTransientPostgresInner_ReturnsFalse()
    {
        var innerException = CreatePostgresException("23505"); // unique_violation
        var exception = new NpgsqlException("Wrapper", innerException);

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
    public void IsTransient_IOException_ReturnsFalse()
    {
        // Plain IOException without being wrapped in NpgsqlException
        var exception = new System.IO.IOException("IO error");

        var result = _classifier.IsTransient(exception);

        result.Should().BeFalse();
    }

    [Fact]
    public void IsTransient_SocketException_ReturnsFalse()
    {
        // Plain SocketException without being wrapped in NpgsqlException
        var exception = new System.Net.Sockets.SocketException();

        var result = _classifier.IsTransient(exception);

        result.Should().BeFalse();
    }

    #endregion

    #region Helper Methods

    private static PostgresException CreatePostgresException(string sqlState)
    {
        return new PostgresException(
            messageText: $"Test error with state {sqlState}",
            severity: "ERROR",
            invariantSeverity: "ERROR",
            sqlState: sqlState);
    }

    #endregion
}
