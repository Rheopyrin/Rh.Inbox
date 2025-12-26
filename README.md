# Rh.Inbox

A high-performance, flexible message inbox library for .NET applications. Rh.Inbox provides reliable message processing with support for multiple storage providers, various processing behaviors, and robust error handling.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Inbox Types (Behaviors)](#inbox-types-behaviors)
  - [Default](#default)
  - [Batched](#batched)
  - [FIFO](#fifo)
  - [FIFO Batched](#fifo-batched)
  - [Write-Only Inbox](#write-only-inbox)
- [Storage Providers](#storage-providers)
  - [PostgreSQL](#postgresql)
  - [Redis](#redis)
  - [InMemory](#inmemory)
  - [Implementing Custom Storage Provider](#implementing-custom-storage-provider)
- [Configuration](#configuration)
  - [Common Options](#common-options)
  - [Provider-Specific Options](#provider-specific-options)
- [Message Handlers](#message-handlers)
  - [IInboxHandler](#iinboxhandler)
  - [IBatchedInboxHandler](#ibatchedinboxhandler)
  - [IFifoInboxHandler](#ififo-inboxhandler)
  - [IFifoBatchedInboxHandler](#ififobatchedinboxhandler)
  - [Handler Results](#handler-results)
- [Message Configuration](#message-configuration)
  - [InboxMessageAttribute](#inboxmessageattribute)
  - [IHasGroupId](#ihasgroupid)
  - [IHasDeduplicationId](#ihasdeduplicationid)
  - [IHasCollapseKey](#ihascollapsekey)
  - [IHasExternalId](#ihasexternalid)
  - [IHasReceivedAt](#ihasreceivedat)
- [Lifecycle and Hooks](#lifecycle-and-hooks)
  - [IInboxManager](#iinboxmanager)
  - [IInboxWriter](#iinboxwriter)
  - [IInboxLifecycleHook](#iinboxlifecyclehook)
  - [Web Host Integration](#web-host-integration)
- [Migration](#migration)
- [Message Flow](#message-flow)
  - [PostgreSQL Message Flow](#postgresql-message-flow)
  - [Redis Approach](#redis-approach)

## Features

- **Multiple Processing Behaviors**: Default, Batched, FIFO, and FIFO Batched processing modes
- **Multiple Storage Providers**: PostgreSQL, Redis, and InMemory support
- **Message Deduplication**: Prevent duplicate message processing
- **Message Collapsing**: Replace older unprocessed messages with newer ones
- **Dead Letter Queue**: Automatic handling of failed messages
- **Retry Mechanism**: Configurable retry attempts for message processing and transient storage failures with exponential backoff
- **ASP.NET Core Integration**: Seamless hosted service integration
- **Lifecycle Hooks**: Extensible lifecycle management
- **Lock Extension**: Automatic lock renewal for long-running batch processing

## Installation

```bash
# Core package
dotnet add package Rh.Inbox

# PostgreSQL provider
dotnet add package Rh.Inbox.Postgres

# Redis provider
dotnet add package Rh.Inbox.Redis

# ASP.NET Core integration
dotnet add package Rh.Inbox.Web
```

## Quick Start

```csharp
// 1. Define your message
public class OrderCreatedMessage
{
    public string OrderId { get; set; }
    public decimal Amount { get; set; }
}

// 2. Create a handler
public class OrderCreatedHandler : IInboxHandler<OrderCreatedMessage>
{
    public async Task<InboxHandleResult> HandleAsync(
        InboxMessageEnvelope<OrderCreatedMessage> message,
        CancellationToken token)
    {
        var order = message.Payload;
        // Process the order...

        return InboxHandleResult.Success;
    }
}

// 3. Configure services
services.AddInbox("orders", builder =>
{
    builder.AsDefault()
        .UsePostgres(connectionString)
        .ConfigureOptions(o => o.PollingInterval = TimeSpan.FromSeconds(1))
        .RegisterHandler<OrderCreatedHandler, OrderCreatedMessage>();
});

// For ASP.NET Core applications
services.RunInboxAsHostedService();

// 4. Write messages
public class OrderService
{
    private readonly IInboxWriter _writer;

    public OrderService(IInboxWriter writer) => _writer = writer;

    public async Task CreateOrderAsync(Order order)
    {
        var message = new OrderCreatedMessage
        {
            OrderId = order.Id,
            Amount = order.Amount
        };

        await _writer.WriteAsync(message, "orders");
    }
}
```

## Inbox Types (Behaviors)

### Default

Messages are processed one at a time, grouped by message type. No ordering guarantees between messages.

```csharp
services.AddInbox("my-inbox", builder =>
{
    builder.AsDefault()
        .UsePostgres(connectionString)
        .RegisterHandler<MyHandler, MyMessage>();
});
```

**Use when**: You need simple, reliable message processing without ordering requirements.

### Batched

Messages are processed in batches grouped by message type. Improves throughput for high-volume scenarios.

```csharp
services.AddInbox("my-inbox", builder =>
{
    builder.AsBatched()
        .UsePostgres(connectionString)
        .ConfigureOptions(o => o.ReadBatchSize = 100)
        .RegisterHandler<MyBatchedHandler, MyMessage>();
});
```

**Use when**: You need to process many messages efficiently and can handle them in batches.

### FIFO

Messages are processed one at a time with strict ordering within each group. Messages with the same `GroupId` are guaranteed to be processed in order.

```csharp
// Message must implement IHasGroupId
public class UserActivityMessage : IHasGroupId
{
    public string UserId { get; set; }
    public string Action { get; set; }

    public string GetGroupId() => UserId;
}

services.AddInbox("user-activity", builder =>
{
    builder.AsFifo()
        .UsePostgres(connectionString)
        .RegisterHandler<UserActivityHandler, UserActivityMessage>();
});
```

**Use when**: You need strict ordering for messages within a logical group (e.g., per-user, per-account).

### FIFO Batched

Messages are processed in batches grouped by `GroupId` and message type, with ordering guaranteed within each group.

```csharp
services.AddInbox("user-activity", builder =>
{
    builder.AsFifoBatched()
        .UsePostgres(connectionString)
        .ConfigureOptions(o => o.ReadBatchSize = 50)
        .RegisterHandler<UserActivityBatchedHandler, UserActivityMessage>();
});
```

**Use when**: You need ordered batch processing for high-volume scenarios with grouped messages.

### Write-Only Inbox

You can register an inbox without any handlers for write-only scenarios. This is useful when you want to write messages from one service and process them in another.

```csharp
// Service A: Write-only inbox (no handlers, no processing loop)
services.AddInbox("shared-inbox", builder =>
{
    builder.AsDefault()
        .UsePostgres(connectionString);
    // No RegisterHandler call - this inbox only writes messages
});

// Service B: Processing inbox (with handlers)
services.AddInbox("shared-inbox", builder =>
{
    builder.AsDefault()
        .UsePostgres(connectionString)
        .RegisterHandler<MyHandler, MyMessage>();
});
```

When no handlers are registered, the inbox will not start a processing loop, but you can still write messages to it using `IInboxWriter`.

## Storage Providers

### PostgreSQL

Production-ready provider with ACID guarantees and comprehensive indexing.

```csharp
services.AddInbox("my-inbox", builder =>
{
    builder.AsDefault()
        .UsePostgres(options =>
        {
            options.ConnectionString = "Host=localhost;Database=mydb;...";
            options.TableName = "custom_inbox_messages";           // Optional
            options.DeadLetterTableName = "custom_dead_letters";   // Optional
            options.DeduplicationTableName = "custom_dedup";       // Optional

            // Cleanup task configuration
            options.AutostartCleanupTasks = true;                  // Default: true
            options.DeadLetterCleanup.Interval = TimeSpan.FromMinutes(5);
            options.DeadLetterCleanup.BatchSize = 1000;
            options.DeduplicationCleanup.Interval = TimeSpan.FromMinutes(5);
            options.GroupLocksCleanup.Interval = TimeSpan.FromMinutes(5);

            // Retry configuration for transient failures
            options.Retry.MaxRetries = 3;                          // Default: 3
            options.Retry.InitialDelay = TimeSpan.FromMilliseconds(100);
            options.Retry.MaxDelay = TimeSpan.FromSeconds(5);
            options.Retry.BackoffMultiplier = 2.0;
            options.Retry.UseJitter = true;
        })
        .ConfigureOptions(options =>
        {
            options.EnableDeduplication = true;
            options.DeduplicationInterval = TimeSpan.FromHours(1);
        })
        .RegisterHandler<MyHandler, MyMessage>();
});
```

**PostgreSQL-Specific Options:**

| Option | Description | Default |
|--------|-------------|---------|
| `ConnectionString` | PostgreSQL connection string | Required |
| `TableName` | Main inbox table name | `inbox_messages_{inboxName}` |
| `DeadLetterTableName` | Dead letter table name | `inbox_dead_letters_{inboxName}` |
| `DeduplicationTableName` | Deduplication table name | `inbox_dedup_{inboxName}` |
| `AutostartCleanupTasks` | Auto-start cleanup tasks with inbox lifecycle | `true` |
| `DeadLetterCleanup` | Dead letter cleanup task options | See below |
| `DeduplicationCleanup` | Deduplication cleanup task options | See below |
| `GroupLocksCleanup` | Group locks cleanup task options (FIFO) | See below |
| `Retry` | Retry options for transient storage failures | See below |

**Cleanup Task Options:**

Each cleanup task (`DeadLetterCleanup`, `DeduplicationCleanup`, `GroupLocksCleanup`) has the following options:

| Option | Description | Default |
|--------|-------------|---------|
| `BatchSize` | Records to delete per batch | 1000 |
| `Interval` | Time between cleanup cycles | 5 minutes |
| `RestartDelay` | Delay before restart after failure | 30 seconds |

**Retry Options:**

Retry behavior for transient storage failures (connection timeouts, deadlocks, etc.):

| Option | Description | Default |
|--------|-------------|---------|
| `MaxRetries` | Maximum number of retry attempts | 3 |
| `InitialDelay` | Initial delay before the first retry | 100ms |
| `MaxDelay` | Maximum delay between retries | 5 seconds |
| `BackoffMultiplier` | Exponential backoff multiplier | 2.0 |
| `UseJitter` | Add jitter to prevent thundering herd | `true` |

Use `RetryOptions.None` to disable retries entirely.

**Cleanup Task Activation:**

Cleanup tasks are only activated when the corresponding feature is enabled:

| Cleanup Task | Activation Condition |
|--------------|---------------------|
| `DeadLetterCleanup` | `EnableDeadLetter = true` AND `DeadLetterMaxMessageLifetime > 0` |
| `DeduplicationCleanup` | `EnableDeduplication = true` AND `DeduplicationInterval > 0` |
| `GroupLocksCleanup` | Inbox type is `Fifo` or `FifoBatched` |

**Manual Cleanup Task Management:**

When `AutostartCleanupTasks = false`, cleanup tasks must be managed manually via `IPostgresCleanupTasksManager`. This is useful when running cleanup tasks on a separate host, pod, or as a cronjob:

```csharp
// Configure inbox without auto-starting cleanup tasks
services.AddInbox("orders", builder =>
{
    builder.AsDefault()
        .UsePostgres(options =>
        {
            options.ConnectionString = connectionString;
            options.AutostartCleanupTasks = false;  // Disable auto-start
        })
        .RegisterHandler<OrderHandler, OrderMessage>();
});

// Option 1: Run cleanup once (e.g., in a cronjob)
public class CleanupJob
{
    private readonly IPostgresCleanupTasksManager _manager;

    public CleanupJob(IPostgresCleanupTasksManager manager) => _manager = manager;

    public async Task RunAsync(CancellationToken token)
    {
        // Execute all cleanup tasks once (loops until no items remain)
        await _manager.ExecuteAsync(token);

        // Or execute for specific inbox(es)
        await _manager.ExecuteAsync("orders", token);
        await _manager.ExecuteAsync(["orders", "notifications"], token);
    }
}

// Option 2: Run cleanup continuously (e.g., in a dedicated service)
public class CleanupHostedService : BackgroundService
{
    private readonly IPostgresCleanupTasksManager _manager;

    public CleanupHostedService(IPostgresCleanupTasksManager manager) => _manager = manager;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Start continuous cleanup loops
        await _manager.StartAsync(stoppingToken);

        // Wait for shutdown signal
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await _manager.StopAsync(cancellationToken);
        await base.StopAsync(cancellationToken);
    }
}
```

Note: Deduplication is configured via common options (`EnableDeduplication` and `DeduplicationInterval`).

### Redis

High-performance provider using Redis Streams and Sorted Sets.

```csharp
services.AddInbox("my-inbox", builder =>
{
    builder.AsDefault()
        .UseRedis(options =>
        {
            options.ConnectionString = "localhost:6379";
            options.KeyPrefix = "myapp:inbox";                     // Optional
            options.MaxMessageLifetime = TimeSpan.FromHours(24);   // Optional

            // Retry configuration for transient failures
            options.Retry.MaxRetries = 3;                          // Default: 3
            options.Retry.InitialDelay = TimeSpan.FromMilliseconds(100);
            options.Retry.MaxDelay = TimeSpan.FromSeconds(5);
        })
        .ConfigureOptions(options =>
        {
            options.EnableDeduplication = true;
            options.DeduplicationInterval = TimeSpan.FromHours(1);
        })
        .RegisterHandler<MyHandler, MyMessage>();
});
```

**Redis-Specific Options:**

| Option | Description | Default |
|--------|-------------|---------|
| `ConnectionString` | Redis connection string | Required |
| `KeyPrefix` | Prefix for all Redis keys | `inbox:{inboxName}` |
| `MaxMessageLifetime` | TTL for messages | 24 hours |
| `Retry` | Retry options for transient storage failures | See Retry Options |

**Retry Options:**

See PostgreSQL section for full retry options documentation. Redis uses the same `RetryOptions` configuration.

Note: Deduplication is configured via common options (`EnableDeduplication` and `DeduplicationInterval`).

### InMemory

Lightweight provider for testing and development.

```csharp
services.AddInbox("my-inbox", builder =>
{
    builder.AsDefault()
        .UseInMemory(options =>
        {
            // Cleanup task configuration (optional)
            options.DeadLetterCleanup.Interval = TimeSpan.FromMinutes(5);
            options.DeduplicationCleanup.Interval = TimeSpan.FromMinutes(5);
        })
        .ConfigureOptions(options =>
        {
            options.EnableDeduplication = true;
            options.DeduplicationInterval = TimeSpan.FromMinutes(30);
        })
        .RegisterHandler<MyHandler, MyMessage>();
});
```

**InMemory-Specific Options:**

| Option | Description | Default |
|--------|-------------|---------|
| `DeadLetterCleanup` | Dead letter cleanup task options | See below |
| `DeduplicationCleanup` | Deduplication cleanup task options | See below |

**Cleanup Task Options:**

Each cleanup task has the following options:

| Option | Description | Default |
|--------|-------------|---------|
| `Interval` | Time between cleanup cycles | 5 minutes |
| `RestartDelay` | Delay before restart after failure | 30 seconds |

Note: InMemory cleanup tasks do not have `BatchSize` option (cleanup is performed in a single operation).

**Cleanup Task Activation:**

Cleanup tasks are only activated when the corresponding feature is enabled:

| Cleanup Task | Activation Condition |
|--------------|---------------------|
| `DeadLetterCleanup` | `EnableDeadLetter = true` AND `DeadLetterMaxMessageLifetime > 0` |
| `DeduplicationCleanup` | `EnableDeduplication = true` AND `DeduplicationInterval > 0` |

Note: Unlike PostgreSQL, InMemory does not have `GroupLocksCleanup` (group locks are managed in memory) and cleanup tasks always start automatically with the inbox lifecycle (no `AutostartCleanupTasks` option).

### Implementing Custom Storage Provider

You can implement your own storage provider (e.g., SQL Server, MongoDB, DynamoDB) by implementing the required interfaces.

#### Step 1: Implement IInboxStorageProvider

```csharp
public class MyCustomStorageProvider : IInboxStorageProvider
{
    private readonly IInboxConfiguration _configuration;

    public MyCustomStorageProvider(IInboxConfiguration configuration)
    {
        _configuration = configuration;
    }

    public Task WriteAsync(InboxMessage message, CancellationToken token = default)
    {
        // Write message to your storage
        // Handle collapse keys: delete uncaptured messages with same collapse key
    }

    public Task WriteBatchAsync(IEnumerable<InboxMessage> messages, CancellationToken token = default)
    {
        // Write multiple messages in a batch
    }

    public Task<IReadOnlyList<InboxMessage>> ReadAndCaptureAsync(string processorId, CancellationToken token = default)
    {
        // Read available messages (captured_at IS NULL or expired)
        // Mark them as captured (set captured_at, captured_by)
        // Return up to ReadBatchSize messages
        // Use _configuration.Options.MaxProcessingTime to determine lock expiration
    }

    public Task FailAsync(Guid messageId, CancellationToken token = default)
    {
        // Increment attempts_count and release (clear captured_at)
    }

    public Task FailBatchAsync(IReadOnlyList<Guid> messageIds, CancellationToken token = default)
    {
        // Batch version of FailAsync
    }

    public Task MoveToDeadLetterAsync(Guid messageId, string reason, CancellationToken token = default)
    {
        // Move message to dead letter storage with failure reason
    }

    public Task MoveToDeadLetterBatchAsync(IReadOnlyList<(Guid MessageId, string Reason)> messages, CancellationToken token = default)
    {
        // Batch version of MoveToDeadLetterAsync
    }

    public Task ReleaseBatchAsync(IReadOnlyList<Guid> messageIds, CancellationToken token = default)
    {
        // Release captured messages (clear captured_at) without failing
    }

    public Task<IReadOnlyList<DeadLetterMessage>> ReadDeadLettersAsync(int count, CancellationToken token = default)
    {
        // Read messages from dead letter storage
    }

    public Task ProcessResultsBatchAsync(
        IReadOnlyList<Guid> toComplete,
        IReadOnlyList<Guid> toFail,
        IReadOnlyList<Guid> toRelease,
        IReadOnlyList<(Guid MessageId, string Reason)> toDeadLetter,
        CancellationToken token = default)
    {
        // Process all results in a single operation for efficiency
        // Delete completed, fail/release others, move to DLQ
    }

    public Task<int> ExtendLocksAsync(
        string processorId,
        IReadOnlyList<IInboxMessageIdentifiers> capturedMessages,
        DateTime newCapturedAt,
        CancellationToken token = default)
    {
        // Extend lock time for captured messages (update captured_at)
        // Return count of successfully extended locks
    }
}
```

#### Step 2: Implement IInboxStorageProviderFactory

```csharp
public class MyCustomStorageProviderFactory : IInboxStorageProviderFactory
{
    public IInboxStorageProvider Create(IInboxConfiguration options)
    {
        return new MyCustomStorageProvider(options);
    }
}
```

#### Step 3: (Optional) Implement ISupportMigration

If your storage requires schema setup (tables, indexes), implement `ISupportMigration`:

```csharp
public class MyCustomStorageProvider : IInboxStorageProvider, ISupportMigration
{
    // ... IInboxStorageProvider methods ...

    public Task MigrateAsync(CancellationToken token = default)
    {
        // Create tables, indexes, etc.
        // This method should be idempotent (safe to run multiple times)
    }
}
```

#### Step 4: (Optional) Implement ISupportGroupLocksReleaseStorageProvider

For FIFO inbox support, implement group lock management:

```csharp
public class MyCustomFifoStorageProvider : IInboxStorageProvider, ISupportGroupLocksReleaseStorageProvider
{
    // ... IInboxStorageProvider methods ...

    public Task ReleaseGroupLocksAsync(IReadOnlyList<string> groupIds, CancellationToken token = default)
    {
        // Release locks for groups after all messages are processed
        // This allows other workers to process new messages from these groups
    }

    public Task ReleaseMessagesAndGroupLocksAsync(IReadOnlyList<IInboxMessageIdentifiers> messages, CancellationToken token = default)
    {
        // Release messages AND their group locks in one operation
        // Used during graceful shutdown
    }
}
```

#### Step 5: Create Extension Methods

Create extension methods to integrate with the inbox builder:

```csharp
public static class MyCustomInboxBuilderExtensions
{
    public static TBuilder UseMyCustomStorage<TBuilder>(
        this TBuilder builder,
        Action<MyCustomOptions>? configureOptions = null)
        where TBuilder : IInboxBuilderBase<TBuilder>
    {
        var options = new MyCustomOptions();
        configureOptions?.Invoke(options);

        builder.ConfigureServices(services =>
        {
            services.TryAddKeyedSingleton(builder.InboxName, options);
            services.TryAddSingleton<MyCustomStorageProviderFactory>();
        });

        builder.UseStorageProviderFactory<MyCustomStorageProviderFactory>();

        return builder;
    }
}
```

#### Step 6: Use Your Custom Provider

```csharp
services.AddInbox("my-inbox", builder =>
{
    builder.AsDefault()
        .UseMyCustomStorage(options =>
        {
            options.ConnectionString = "...";
        })
        .RegisterHandler<MyHandler, MyMessage>();
});
```

#### Key Implementation Notes

1. **Message Locking**: Use `captured_at` and `captured_by` fields to implement optimistic locking. Messages with `captured_at IS NULL` or `captured_at < now - MaxProcessingTime` are available for processing.

2. **FIFO Ordering**: For FIFO support, ensure only one worker can process messages from a group at a time. Use group locks with TTL based on `MaxProcessingTime`.

3. **Deduplication**: Check `deduplication_id` against recent records within `DeduplicationInterval`. Reject duplicates during write.

4. **Collapsing**: When writing a message with a `collapse_key`, delete any uncaptured messages with the same key.

5. **Atomicity**: Use transactions where possible to ensure consistency, especially for `ProcessResultsBatchAsync`.

6. **Idempotency**: Migration should be idempotent. Use `IF NOT EXISTS` patterns for table/index creation.

## Configuration

### Common Options

All inbox types share these configuration options:

```csharp
services.AddInbox("my-inbox", builder =>
{
    builder.AsDefault()
        .UsePostgres(connectionString)
        .ConfigureOptions(options =>
        {
            options.ReadBatchSize = 100;                           // Messages to read per poll
            options.WriteBatchSize = 100;                          // Messages per write batch
            options.MaxProcessingTime = TimeSpan.FromMinutes(5);   // Max time before release
            options.PollingInterval = TimeSpan.FromSeconds(5);     // Polling frequency
            options.ReadDelay = TimeSpan.Zero;                     // Delay between reads
            options.ShutdownTimeout = TimeSpan.FromSeconds(30);    // Graceful shutdown timeout
            options.MaxAttempts = 3;                               // Retry attempts
            options.EnableDeadLetter = true;                       // Enable DLQ
            options.DeadLetterMaxMessageLifetime = TimeSpan.FromDays(7); // Auto-cleanup after 7 days
            options.EnableDeduplication = true;                    // Enable deduplication
            options.DeduplicationInterval = TimeSpan.FromHours(1); // Track duplicates for 1 hour
        })
        .RegisterHandler<MyHandler, MyMessage>();
});
```

| Option | Description | Default |
|--------|-------------|---------|
| `ReadBatchSize` | Maximum messages to read per polling cycle | 100 |
| `WriteBatchSize` | Maximum messages per write batch | 100 |
| `MaxProcessingTime` | Maximum time a message can be captured before release | 5 minutes |
| `PollingInterval` | Time between polling cycles when no messages found | 5 seconds |
| `ReadDelay` | Delay between consecutive reads | 0 |
| `ShutdownTimeout` | Maximum time to wait for graceful shutdown | 30 seconds |
| `MaxAttempts` | Maximum retry attempts before dead-lettering | 3 |
| `EnableDeadLetter` | Enable dead letter queue for failed messages | false |
| `DeadLetterMaxMessageLifetime` | Maximum lifetime for dead letter messages (requires `EnableDeadLetter`) | 0 (no cleanup) |
| `MaxProcessingThreads` | Maximum concurrent handler executions | 1 |
| `MaxWriteThreads` | Maximum concurrent write operations | 1 |
| `EnableDeduplication` | Enable message deduplication | false |
| `DeduplicationInterval` | Duration to track duplicates (requires `EnableDeduplication`) | 0 (no cleanup) |
| `EnableLockExtension` | Enable automatic lock extension for long-running batches | false |
| `LockExtensionThreshold` | Percentage of MaxProcessingTime before extending locks (0.1-0.9) | 0.5 |

### Lock Extension

When processing large batches of messages, individual messages at the end of the batch may have their locks expire before being processed, allowing other workers to "steal" them and cause duplicate processing.

Enable lock extension to automatically refresh message locks during long-running batch processing:

```csharp
services.AddInbox("my-inbox", builder =>
{
    builder.AsDefault()
        .UsePostgres(connectionString)
        .ConfigureOptions(options =>
        {
            options.MaxProcessingTime = TimeSpan.FromMinutes(5);
            options.EnableLockExtension = true;      // Enable automatic lock extension
            options.LockExtensionThreshold = 0.5;    // Extend at 50% of MaxProcessingTime (2.5 min)
        })
        .RegisterHandler<MyHandler, MyMessage>();
});
```

**How it works:**
- A timer fires at `MaxProcessingTime × LockExtensionThreshold` intervals during batch processing
- Both message capture locks and FIFO group locks are extended
- If extension fails, a warning is logged and processing continues
- Locks will eventually expire if extension repeatedly fails (safety fallback)

**Use when:** Processing batches where individual message handling may take significant time.

### Provider-Specific Options

See the [Storage Providers](#storage-providers) section for provider-specific configuration options.

## Message Handlers

### IInboxHandler

For Default inbox type. Processes messages one at a time.

```csharp
public class OrderHandler : IInboxHandler<OrderMessage>
{
    private readonly IOrderService _orderService;

    public OrderHandler(IOrderService orderService)
    {
        _orderService = orderService;
    }

    public async Task<InboxHandleResult> HandleAsync(
        InboxMessageEnvelope<OrderMessage> message,
        CancellationToken token)
    {
        try
        {
            await _orderService.ProcessAsync(message.Payload, token);
            return InboxHandleResult.Success;
        }
        catch (TransientException)
        {
            return InboxHandleResult.Retry;
        }
        catch (Exception)
        {
            return InboxHandleResult.Failed;
        }
    }
}
```

### IBatchedInboxHandler

For Batched inbox type. Processes messages in batches.

```csharp
public class OrderBatchHandler : IBatchedInboxHandler<OrderMessage>
{
    private readonly IOrderService _orderService;

    public OrderBatchHandler(IOrderService orderService)
    {
        _orderService = orderService;
    }

    public async Task<IReadOnlyList<InboxMessageResult>> HandleAsync(
        IReadOnlyList<InboxMessageEnvelope<OrderMessage>> messages,
        CancellationToken token)
    {
        var results = new List<InboxMessageResult>();

        // Process all messages in a single database transaction
        var orders = messages.Select(m => m.Payload).ToList();
        var processedIds = await _orderService.ProcessBatchAsync(orders, token);

        foreach (var message in messages)
        {
            var result = processedIds.Contains(message.Payload.OrderId)
                ? InboxHandleResult.Success
                : InboxHandleResult.Retry;

            results.Add(new InboxMessageResult(message.Id, result));
        }

        return results;
    }
}
```

### IFifoInboxHandler

For FIFO inbox type. Processes messages with ordering guarantees.

```csharp
public class UserEventHandler : IFifoInboxHandler<UserEventMessage>
{
    public async Task<InboxHandleResult> HandleAsync(
        InboxMessageEnvelope<UserEventMessage> message,
        CancellationToken token)
    {
        // Messages for the same user are processed in order
        await ProcessUserEventAsync(message.Payload, token);
        return InboxHandleResult.Success;
    }
}
```

### IFifoBatchedInboxHandler

For FIFO Batched inbox type. Processes batches with ordering guarantees per group.

```csharp
public class UserEventBatchHandler : IFifoBatchedInboxHandler<UserEventMessage>
{
    public async Task<IReadOnlyList<InboxMessageResult>> HandleAsync(
        string groupId,  // The group ID for all messages in this batch
        IReadOnlyList<InboxMessageEnvelope<UserEventMessage>> messages,
        CancellationToken token)
    {
        var results = new List<InboxMessageResult>();

        // All messages belong to the same group and are in order
        foreach (var message in messages)
        {
            await ProcessUserEventAsync(message.Payload, token);
            results.Add(new InboxMessageResult(message.Id, InboxHandleResult.Success));
        }

        return results;
    }
}
```

### Handler Results

Handlers return results indicating the processing outcome:

| Result | Description |
|--------|-------------|
| `Success` | Message processed successfully, will be removed from inbox |
| `Failed` | Processing failed, will be retried up to `MaxAttempts` |
| `Retry` | Message should be retried immediately |
| `MoveToDeadLetter` | Move message to dead letter queue without further retries |

## Message Configuration

### InboxMessageAttribute

Configure message serialization behavior:

```csharp
[InboxMessage(MessageType = "orders.created.v1")]
public class OrderCreatedMessage
{
    public string OrderId { get; set; }
}
```

This attribute allows you to specify a custom message type name for serialization, useful for maintaining compatibility when renaming or moving message classes.

### IHasGroupId

**Required for FIFO and FIFO Batched inbox types.** Enables message ordering within groups.

```csharp
public class UserActivityMessage : IHasGroupId
{
    public string UserId { get; set; }
    public string Action { get; set; }
    public DateTime Timestamp { get; set; }

    public string GetGroupId() => UserId;
}
```

Messages with the same `GroupId` are guaranteed to be processed in order.

### IHasDeduplicationId

Enable message deduplication to prevent duplicate processing:

```csharp
public class PaymentMessage : IHasDeduplicationId
{
    public string PaymentId { get; set; }
    public decimal Amount { get; set; }

    public string GetDeduplicationId() => PaymentId;
}
```

When a message with a duplicate ID is written within the `DeduplicationInterval`, it will be rejected.

### IHasCollapseKey

Enable message collapsing to replace older unprocessed messages:

```csharp
public class UserStatusMessage : IHasCollapseKey
{
    public string UserId { get; set; }
    public string Status { get; set; }

    public string GetCollapseKey() => $"user-status:{UserId}";
}
```

When a new message with the same collapse key arrives, older uncaptured messages with the same key are automatically removed.

### IHasExternalId

Provide a custom message ID instead of auto-generated GUID:

```csharp
public class ImportedMessage : IHasExternalId
{
    public Guid OriginalId { get; set; }
    public string Data { get; set; }

    public Guid GetId() => OriginalId;
}
```

Useful for idempotency when replaying messages.

### IHasReceivedAt

Provide a custom received timestamp:

```csharp
public class MigratedMessage : IHasReceivedAt
{
    public DateTime OriginalTimestamp { get; set; }
    public string Data { get; set; }

    public DateTime GetReceivedAt() => OriginalTimestamp;
}
```

Useful for preserving original timestamps when migrating messages.

## Lifecycle and Hooks

### IInboxManager

Manages inbox lifecycle and provides access to inbox instances:

```csharp
public class MyService
{
    private readonly IInboxManager _manager;

    public MyService(IInboxManager manager)
    {
        _manager = manager;
    }

    public async Task ManualControlAsync()
    {
        // Start all inboxes
        await _manager.StartAsync();

        // Check if running
        if (_manager.IsRunning)
        {
            // Get specific inbox
            var inbox = _manager.GetInbox("my-inbox");
        }

        // Stop all inboxes
        await _manager.StopAsync();
    }
}
```

### IInboxWriter

Write messages to inboxes. Messages can be written at any time, even before the inbox is started.

```csharp
public class MessagePublisher
{
    private readonly IInboxWriter _writer;

    public MessagePublisher(IInboxWriter writer)
    {
        _writer = writer;
    }

    public async Task PublishAsync()
    {
        // Write single message to specific inbox
        await _writer.WriteAsync(new OrderMessage(), "orders");

        // Write batch of messages
        var messages = new[] { new OrderMessage(), new OrderMessage() };
        await _writer.WriteBatchAsync(messages, "orders");
    }
}
```

### IInboxLifecycleHook

Implement custom lifecycle hooks for startup/shutdown tasks:

```csharp
public class CustomLifecycleHook : IInboxLifecycleHook
{
    public async Task OnStart(CancellationToken token)
    {
        // Called when inbox starts
        await InitializeResourcesAsync(token);
    }

    public async Task OnStop(CancellationToken token)
    {
        // Called when inbox stops
        await CleanupResourcesAsync(token);
    }
}

// Register the hook
services.AddSingleton<IInboxLifecycleHook, CustomLifecycleHook>();
```

Built-in lifecycle hooks include:
- **DeduplicationCleanupService** (Postgres/InMemory): Periodically cleans expired deduplication records

### Web Host Integration

For ASP.NET Core applications, use the hosted service integration:

```csharp
// Program.cs
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddInbox("orders", inbox =>
{
    inbox.AsDefault()
        .UsePostgres(connectionString)
        .RegisterHandler<OrderHandler, OrderMessage>();
});

// Automatically start/stop inbox with the application
builder.Services.RunInboxAsHostedService();

var app = builder.Build();
app.Run();
```

This integrates the inbox with the ASP.NET Core hosted service lifecycle, automatically starting when the application starts and gracefully stopping during shutdown.

## Migration

For providers that require schema setup (PostgreSQL), use the migration service:

```csharp
// Option 1: Migrate all inboxes
public class MigrationJob
{
    private readonly IInboxMigrationService _migrationService;

    public MigrationJob(IInboxMigrationService migrationService)
    {
        _migrationService = migrationService;
    }

    public async Task RunAsync()
    {
        // Migrate all registered inboxes
        await _migrationService.MigrateAsync();

        // Or migrate a specific inbox
        await _migrationService.MigrateAsync("orders");
    }
}

// Option 2: Run migrations at startup
var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddInbox("orders", builder => { /* ... */ });
    })
    .Build();

// Run migrations before starting
var migrationService = host.Services.GetRequiredService<IInboxMigrationService>();
await migrationService.MigrateAsync();

await host.RunAsync();
```

### PostgreSQL Tables Created

The migration creates the following tables:

**Inbox Messages Table** (`inbox_messages_{name}`):
- `id` - Unique message identifier (UUID)
- `inbox_name` - Name of the inbox
- `message_type` - Type name for deserialization
- `payload` - Serialized message content (JSON)
- `group_id` - Group identifier for FIFO ordering
- `collapse_key` - Key for message collapsing
- `deduplication_id` - Key for deduplication
- `attempts_count` - Number of processing attempts
- `received_at` - When the message was written
- `captured_at` - When the message was captured for processing
- `captured_by` - Identifier of the processor

**Dead Letter Table** (`inbox_dead_letters_{name}`):
- Same structure as inbox messages plus:
- `failure_reason` - Description of why the message failed
- `moved_at` - When the message was moved to DLQ

**Deduplication Table** (`inbox_dedup_{name}`):
- `inbox_name` - Name of the inbox
- `deduplication_id` - The deduplication key
- `created_at` - When the deduplication record was created

## Message Flow

### PostgreSQL Message Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                         Write Phase                             │
├─────────────────────────────────────────────────────────────────┤
│  1. IInboxWriter.WriteAsync(message)                            │
│  2. Check deduplication (if IHasDeduplicationId)                │
│     └─ If duplicate within interval → reject                    │
│  3. Delete collapsible messages (if IHasCollapseKey)            │
│  4. Insert into inbox_messages table                            │
│     └─ captured_at = NULL (pending)                             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Processing Loop                            │
├─────────────────────────────────────────────────────────────────┤
│  1. Poll for pending messages (captured_at IS NULL)             │
│     └─ FIFO: Skip groups with in-flight messages                │
│  2. Capture messages (UPDATE captured_at, captured_by)          │
│  3. Deserialize and invoke handler                              │
│  4. Based on result:                                            │
│     ├─ Success → DELETE from inbox                              │
│     ├─ Failed → INCREMENT attempts_count, release               │
│     │   └─ If attempts >= MaxAttempts → move to DLQ             │
│     ├─ Retry → Release (captured_at = NULL)                     │
│     └─ MoveToDeadLetter → Move to DLQ immediately               │
└─────────────────────────────────────────────────────────────────┘
```

**Key PostgreSQL Features:**
- **Row-level locking** with `FOR UPDATE SKIP LOCKED` for concurrent processing
- **Optimized indexes** for pending, captured, and FIFO queries
- **Transactional operations** for atomicity
- **Background cleanup** for expired deduplication records

### Redis Approach

Redis uses a different architecture optimized for high throughput:

```
┌─────────────────────────────────────────────────────────────────┐
│                       Key Structure                             │
├─────────────────────────────────────────────────────────────────┤
│  {prefix}:pending       - Sorted Set (score = timestamp)        │
│  {prefix}:captured      - Sorted Set (score = capture time)     │
│  {prefix}:msg:{id}      - Hash (message data with TTL)          │
│  {prefix}:collapse      - Hash (collapse key → message ID)      │
│  {prefix}:dedup:{id}    - String with TTL (deduplication)       │
│  {prefix}:lock:{group}  - String with TTL (FIFO group lock)     │
│  {prefix}:dlq           - Sorted Set (dead letter queue)        │
│  {prefix}:dlq:{id}      - Hash (dead letter message data)       │
└─────────────────────────────────────────────────────────────────┘
```

**Key Redis Features:**
- **Atomic Lua scripts** for complex operations
- **TTL-based expiration** for messages, deduplication, and group locks (no cleanup jobs needed)
- **Sorted Sets** for efficient range queries and ordering
- **Hash storage** for message data with automatic TTL
- **Pipeline execution** for batch operations

**Processing Flow:**
1. Messages written to `pending` sorted set with timestamp score
2. Processor atomically captures messages (moves to `captured`, sets lock for FIFO)
3. On success: Delete from `captured` and message hash
4. On failure: Increment attempts, clear capture, or move to `dlq`
5. On FIFO completion: Explicitly release group locks or let TTL expire

**FIFO Implementation:**
- Each group has an individual lock key (`{prefix}:lock:{groupId}`) with TTL
- Lua scripts ensure only one worker processes messages from a group at a time
- Multiple messages from the same group can be captured in a single batch by the same worker
- Group locks are released explicitly via `ISupportGroupLocksReleaseStorageProvider.ReleaseGroupLocksAsync()`
- TTL (based on `MaxProcessingTime`) serves as a safety fallback for crashed workers

## License

MIT License - see LICENSE file for details.