using Rh.Inbox.Resilience;
using StackExchange.Redis;

namespace Rh.Inbox.Redis.Resilience;

/// <summary>
/// Classifies Redis exceptions as transient or permanent.
/// </summary>
internal sealed class RedisTransientExceptionClassifier : ITransientExceptionClassifier
{
    public bool IsTransient(Exception exception)
    {
        return exception switch
        {
            RedisConnectionException => true,
            RedisTimeoutException => true,
            RedisServerException serverEx => IsTransientServerException(serverEx),
            RedisException redisEx => IsTransientRedisException(redisEx),
            TimeoutException => true,
            OperationCanceledException => false,
            _ => false
        };
    }

    private static bool IsTransientServerException(RedisServerException ex)
    {
        var message = ex.Message;
        if (string.IsNullOrEmpty(message))
        {
            return false;
        }

        // OOM can be transient if it's temporary memory pressure
        if (message.Contains("OOM", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        // BUSY indicates server is executing a script
        if (message.Contains("BUSY", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        // LOADING means server is loading dataset
        if (message.Contains("LOADING", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        // CLUSTERDOWN - cluster is in failure state
        if (message.Contains("CLUSTERDOWN", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        // READONLY - replica cannot accept writes (can happen during failover)
        if (message.Contains("READONLY", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return false;
    }

    private static bool IsTransientRedisException(RedisException ex)
    {
        var message = ex.Message;
        if (string.IsNullOrEmpty(message))
        {
            return false;
        }

        return message.Contains("connection", StringComparison.OrdinalIgnoreCase) ||
               message.Contains("timeout", StringComparison.OrdinalIgnoreCase) ||
               message.Contains("socket", StringComparison.OrdinalIgnoreCase);
    }
}
