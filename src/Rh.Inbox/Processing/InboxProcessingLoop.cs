using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Inboxes;
using Rh.Inbox.Processing.Strategies;
using Rh.Inbox.Processing.Strategies.Factory;

namespace Rh.Inbox.Processing;

internal sealed class InboxProcessingLoop : IDisposable
{
    private static readonly ActivitySource ActivitySource = new(nameof(InboxProcessingLoop));

    private readonly InboxBase _inbox;
    private readonly IInboxProcessingStrategy _strategy;
    private readonly ILogger<InboxProcessingLoop> _logger;
    private readonly string _processorId;

    private Task? _processingTask;
    private CancellationTokenSource? _cts;
    private MessageProcessingContext? _currentContext;

    public InboxProcessingLoop(
        InboxBase inbox,
        IInboxProcessingStrategyFactory inboxProcessingStrategyFactory,
        ILogger<InboxProcessingLoop> logger)
    {
        _inbox = inbox;
        _logger = logger;
        _processorId = $"{Environment.MachineName}-{Guid.NewGuid():N}";
        _strategy = inboxProcessingStrategyFactory.Create(inbox);
    }

    public Task StartAsync(CancellationToken token)
    {
        var configuration = _inbox.GetConfiguration();

        if (!configuration.MetadataRegistry.HasRegisteredMessages)
        {
            _logger.LogWarning("No messages have been registered for inbox '{InboxName}', skipping processing loop.", _inbox.Name);
            return Task.CompletedTask;
        }

        _cts = CancellationTokenSource.CreateLinkedTokenSource(token);
        _processingTask = Task.Factory.StartNew(
                () => ProcessingLoopAsync(configuration, _cts.Token),
                _cts.Token,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default)
            .Unwrap();

        _logger.LogInformation(
            "Processing loop started for inbox '{InboxName}' with processor ID '{ProcessorId}'",
            _inbox.Name, _processorId);

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken token)
    {
        _logger.LogInformation("Stopping processing loop for inbox '{InboxName}'", _inbox.Name);

        _cts?.Cancel();

        if (_processingTask != null)
        {
            var options = _inbox.GetConfiguration().Options;

            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(token);
            timeoutCts.CancelAfter(options.ShutdownTimeout);

            try
            {
                await _processingTask.WaitAsync(timeoutCts.Token);
                _logger.LogInformation("Processing loop stopped gracefully for inbox '{InboxName}'", _inbox.Name);
            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !token.IsCancellationRequested)
            {
                _logger.LogWarning(
                    "Processing loop for inbox '{InboxName}' did not stop within {Timeout}. Forcing shutdown.",
                    _inbox.Name, options.ShutdownTimeout);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Processing loop stop was cancelled for inbox '{InboxName}'", _inbox.Name);
            }
        }
        else
        {
            _logger.LogInformation("Processing loop stopped for inbox '{InboxName}'", _inbox.Name);
        }
    }

    public void Dispose()
    {
        _cts?.Dispose();
    }

    private async Task ProcessingLoopAsync(IInboxConfiguration configuration, CancellationToken token)
    {
        var options = configuration.Options;

        var readStopwatch = Stopwatch.StartNew();

        try
        {
            while (!token.IsCancellationRequested)
            {
                if (!await WaitForReadDelayAsync(options.ReadDelay, readStopwatch, token))
                    break;

                using var activity = ActivitySource.StartActivity("ProcessBatch");

                readStopwatch.Restart();
                var shouldDelay = true;
                try
                {
                    var messagesProcessed = await ProcessBatchAsync(configuration, token);
                    shouldDelay = messagesProcessed == 0;
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in processing loop for inbox '{InboxName}'", _inbox.Name);
                }

                if (shouldDelay)
                {
                    try
                    {
                        await Task.Delay(options.PollingInterval, token);
                    }
                    catch (OperationCanceledException) when (token.IsCancellationRequested)
                    {
                        break;
                    }
                }
            }
        }
        finally
        {
            await ReleaseInFlightMessagesAsync();
        }
    }

    private async Task ReleaseInFlightMessagesAsync()
    {
        var messagesToRelease = GetAndClearContext();

        if (messagesToRelease is not { Count: > 0 })
        {
            return;
        }

        var options = _inbox.GetConfiguration().Options;

        try
        {
            _logger.LogInformation(
                "Releasing {Count} in-flight messages for inbox '{InboxName}' during shutdown",
                messagesToRelease.Count, _inbox.Name);

            var storageProvider = _inbox.GetStorageProvider();

            // Use shutdown timeout for release operations to prevent hanging
            using var timeoutCts = new CancellationTokenSource(options.ShutdownTimeout);

            // Use optimized combined method for FIFO providers, otherwise use standard release
            if (storageProvider is ISupportGroupLocksReleaseStorageProvider groupLocksProvider)
            {
                await groupLocksProvider.ReleaseMessagesAndGroupLocksAsync(messagesToRelease, timeoutCts.Token);
            }
            else
            {
                var messageIds = messagesToRelease.Select(m => m.Id).ToArray();
                await storageProvider.ReleaseBatchAsync(messageIds, timeoutCts.Token);
            }

            _logger.LogDebug(
                "Successfully released {Count} messages for inbox '{InboxName}'",
                messagesToRelease.Count, _inbox.Name);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning(
                "Release of {Count} in-flight messages for inbox '{InboxName}' timed out after {Timeout}. " +
                "Messages will be released after MaxProcessingTime ({MaxProcessingTime}) expires.",
                messagesToRelease.Count, _inbox.Name, options.ShutdownTimeout, options.MaxProcessingTime);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to release {Count} in-flight messages for inbox '{InboxName}' during shutdown. " +
                "Messages will be released after MaxProcessingTime ({MaxProcessingTime}) expires.",
                messagesToRelease.Count, _inbox.Name, options.MaxProcessingTime);
        }
    }

    private IReadOnlyList<InboxMessage>? GetAndClearContext()
    {
        var context = _currentContext;

        if (context == null)
        {
            return null;
        }

        var messages = context.GetInFlightMessages();
        ClearContext();

        return messages;
    }

    private async Task<int> ProcessBatchAsync(IInboxConfiguration configuration, CancellationToken token)
    {
        var storageProvider = _inbox.GetStorageProvider();
        var messages = await storageProvider.ReadAndCaptureAsync(_processorId, token);

        if (messages.Count == 0)
        {
            return 0;
        }

        var context = CreateContext(storageProvider, configuration.Options, messages);

        Timer? lockExtensionTimer = null;
        CancellationTokenSource? timerCts = null;

        try
        {
            if (configuration.Options.EnableLockExtension)
            {
                timerCts = CancellationTokenSource.CreateLinkedTokenSource(token);
                var interval = TimeSpan.FromTicks(
                    (long)(configuration.Options.MaxProcessingTime.Ticks * configuration.Options.LockExtensionThreshold));

                var localTimerCts = timerCts;
                lockExtensionTimer = new Timer(
                    _ => OnLockExtensionTimerElapsed(lockExtensionTimer!, storageProvider, configuration, interval, localTimerCts.Token),
                    null,
                    interval,
                    Timeout.InfiniteTimeSpan);
            }

            _logger.LogDebug(
                "Processing {Count} messages from inbox '{InboxName}'",
                messages.Count, _inbox.Name);

            await _strategy.ProcessAsync(_processorId, messages, context, token);
        }
        finally
        {
            timerCts?.Cancel();

            if (lockExtensionTimer != null)
            {
                await lockExtensionTimer.DisposeAsync();
            }

            timerCts?.Dispose();
            ClearContext();
        }

        return messages.Count;
    }

    private MessageProcessingContext CreateContext(
        IInboxStorageProvider storageProvider,
        IInboxOptions options,
        IReadOnlyList<InboxMessage> messages)
    {
        var context = new MessageProcessingContext(storageProvider, options, _logger, messages);
        _currentContext = context;
        return context;
    }

    private void ClearContext()
    {
        _currentContext?.Clear();
        _currentContext = null;
    }

    private async void OnLockExtensionTimerElapsed(
        Timer timer,
        IInboxStorageProvider storageProvider,
        IInboxConfiguration configuration,
        TimeSpan interval,
        CancellationToken token)
    {
        try
        {
            if (token.IsCancellationRequested)
            {
                return;
            }

            await ExtendLocksAsync(storageProvider, configuration, token);
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                try
                {
                    timer.Change(interval, Timeout.InfiniteTimeSpan);
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }
    }

    private async Task ExtendLocksAsync(
        IInboxStorageProvider storageProvider,
        IInboxConfiguration configuration,
        CancellationToken token)
    {
        if (token.IsCancellationRequested)
        {
            return;
        }

        var messagesToExtend = _currentContext?.GetInFlightMessages();

        if (messagesToExtend is not { Count: > 0 })
        {
            return;
        }

        try
        {
            var newCapturedAt = configuration.DateTimeProvider.GetUtcNow();
            var extended = await storageProvider.ExtendLocksAsync(_processorId, messagesToExtend, newCapturedAt, token);

            if (extended > 0)
            {
                foreach (var message in messagesToExtend)
                {
                    message.CapturedAt = newCapturedAt;
                    message.CapturedBy = _processorId;
                }
            }

            _logger.LogDebug(
                "Extended locks for {ExtendedCount}/{TotalCount} in-flight messages in inbox '{InboxName}'",
                extended, messagesToExtend.Count, _inbox.Name);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Failed to extend locks for {Count} messages in inbox '{InboxName}'. " +
                "Messages may be recaptured after MaxProcessingTime ({MaxProcessingTime}) expires.",
                messagesToExtend.Count, _inbox.Name, configuration.Options.MaxProcessingTime);
        }
    }

    private static async Task<bool> WaitForReadDelayAsync(
        TimeSpan readDelay,
        Stopwatch stopwatch,
        CancellationToken token)
    {
        if (readDelay <= TimeSpan.Zero)
        {
            return true;
        }

        var elapsed = stopwatch.Elapsed;

        if (elapsed >= readDelay)
        {
            return true;
        }

        try
        {
            await Task.Delay(readDelay - elapsed, token);
            return true;
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            return false;
        }
    }
}