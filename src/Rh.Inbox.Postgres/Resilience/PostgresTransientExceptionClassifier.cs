using Npgsql;
using Rh.Inbox.Resilience;

namespace Rh.Inbox.Postgres.Resilience;

/// <summary>
/// Classifies PostgreSQL exceptions as transient or permanent.
/// </summary>
internal sealed class PostgresTransientExceptionClassifier : ITransientExceptionClassifier
{
    /// <summary>
    /// PostgreSQL error codes that indicate transient failures.
    /// See: https://www.postgresql.org/docs/current/errcodes-appendix.html
    /// </summary>
    private static readonly HashSet<string> TransientSqlStates = new(StringComparer.OrdinalIgnoreCase)
    {
        // Connection exceptions (Class 08)
        "08000", // connection_exception
        "08003", // connection_does_not_exist
        "08006", // connection_failure
        "08001", // sqlclient_unable_to_establish_sqlconnection
        "08004", // sqlserver_rejected_establishment_of_sqlconnection
        "08007", // transaction_resolution_unknown

        // Transaction rollback (Class 40)
        "40001", // serialization_failure
        "40P01", // deadlock_detected
        "40003", // statement_completion_unknown

        // Insufficient resources (Class 53)
        "53000", // insufficient_resources
        "53100", // disk_full
        "53200", // out_of_memory
        "53300", // too_many_connections

        // Operator intervention (Class 57)
        "57P03", // cannot_connect_now (server starting up)

        // Query canceled
        "57014" // query_canceled
    };

    public bool IsTransient(Exception exception)
    {
        return exception switch
        {
            PostgresException pgEx => IsTransientPostgresException(pgEx),
            NpgsqlException npgsqlEx => IsTransientNpgsqlException(npgsqlEx),
            TimeoutException => true,
            OperationCanceledException => false,
            _ => false
        };
    }

    private static bool IsTransientPostgresException(PostgresException ex)
    {
        if (!string.IsNullOrEmpty(ex.SqlState) && TransientSqlStates.Contains(ex.SqlState))
        {
            return true;
        }

        return false;
    }

    private static bool IsTransientNpgsqlException(NpgsqlException ex)
    {
        // Check inner exception first
        if (ex.InnerException is PostgresException innerPg)
        {
            return IsTransientPostgresException(innerPg);
        }

        // Connection-level failures are typically transient
        return ex.IsTransient;
    }
}
