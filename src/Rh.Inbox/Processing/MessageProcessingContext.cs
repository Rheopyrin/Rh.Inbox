using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Handlers;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Storage;

namespace Rh.Inbox.Processing;

/// <summary>
/// Implementation of <see cref="IMessageProcessingContext"/> that tracks in-flight messages
/// and delegates storage operations to the underlying storage provider.
/// </summary>
internal sealed class MessageProcessingContext : IMessageProcessingContext
{
    private const string DefaultDeadLetterReason = "Handler requested move to dead letter";

    private readonly IInboxStorageProvider _storageProvider;
    private readonly IInboxOptions _options;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<Guid, InboxMessage> _inFlightMessages = new();

    public MessageProcessingContext(
        IInboxStorageProvider storageProvider,
        IInboxOptions options,
        ILogger logger,
        IReadOnlyList<InboxMessage> messages)
    {
        _storageProvider = storageProvider;
        _options = options;
        _logger = logger;

        foreach (var msg in messages)
        {
            _inFlightMessages.TryAdd(msg.Id, msg);
        }
    }

    public IReadOnlyList<InboxMessage> GetInFlightMessages()
        => _inFlightMessages.Values.ToList();

    public async Task ProcessResultsBatchAsync(IReadOnlyList<InboxMessageResult> results, CancellationToken token)
    {
        if (results.Count == 0)
        {
            return;
        }

        // Group results by outcome for batch processing
        var toComplete = new List<Guid>();
        var toFail = new List<Guid>();
        var toRelease = new List<Guid>();
        var toDeadLetter = new List<(Guid MessageId, string Reason)>();

        foreach (var result in results)
        {
            // Remove from in-flight tracking and get message for max attempts check
            _inFlightMessages.TryRemove(result.MessageId, out var message);

            switch (result.Result)
            {
                case InboxHandleResult.Success:
                    toComplete.Add(result.MessageId);
                    break;

                case InboxHandleResult.Failed:
                    // Check if max attempts exceeded - move to dead letter instead of failing
                    if (message != null && message.AttemptsCount + 1 >= _options.MaxAttempts)
                    {
                        var reason = GetDeadLetterReason(result.FailureReason, isMaxAttemptsExceeded: true);
                        toDeadLetter.Add((result.MessageId, reason));
                        _logger.LogWarning(
                            "Message {MessageId} exceeded max attempts ({MaxAttempts}), moving to dead letter",
                            result.MessageId, _options.MaxAttempts);
                    }
                    else
                    {
                        toFail.Add(result.MessageId);
                    }

                    break;

                case InboxHandleResult.Retry:
                    toRelease.Add(result.MessageId);
                    break;

                case InboxHandleResult.MoveToDeadLetter:
                    toDeadLetter.Add((result.MessageId, GetDeadLetterReason(result.FailureReason, isMaxAttemptsExceeded: false)));
                    break;
            }
        }

        // Execute all operations in a single connection/transaction
        await _storageProvider.ProcessResultsBatchAsync(toComplete, toFail, toRelease, toDeadLetter, token);
    }

    public async Task FailMessageAsync(InboxMessage message, CancellationToken token)
    {
        // Check if max attempts will be exceeded after this failure
        if (message.AttemptsCount + 1 >= _options.MaxAttempts)
        {
            _logger.LogWarning(
                "Message {MessageId} exceeded max attempts ({MaxAttempts}), moving to dead letter",
                message.Id, _options.MaxAttempts);
            await _storageProvider.MoveToDeadLetterAsync(message.Id, GetMaxAttemptsExceededReason(), token);
        }
        else
        {
            await _storageProvider.FailAsync(message.Id, token);
        }

        _inFlightMessages.TryRemove(message.Id, out _);
    }

    public async Task FailMessageBatchAsync(IReadOnlyList<InboxMessage> messages, CancellationToken token)
    {
        if (messages.Count == 0)
        {
            return;
        }

        var maxAttemptsReason = GetMaxAttemptsExceededReason();

        var toFail = new List<Guid>();
        var toDeadLetter = new List<(Guid MessageId, string Reason)>();

        foreach (var message in messages)
        {
            if (message.AttemptsCount + 1 >= _options.MaxAttempts)
            {
                _logger.LogWarning(
                    "Message {MessageId} exceeded max attempts ({MaxAttempts}), moving to dead letter",
                    message.Id, _options.MaxAttempts);
                toDeadLetter.Add((message.Id, maxAttemptsReason));
            }
            else
            {
                toFail.Add(message.Id);
            }
        }

        if (toFail.Count > 0)
        {
            await _storageProvider.FailBatchAsync(toFail, token);
        }

        if (toDeadLetter.Count > 0)
        {
            await _storageProvider.MoveToDeadLetterBatchAsync(toDeadLetter, token);
        }

        // Remove all from in-flight tracking
        foreach (var message in messages)
        {
            _inFlightMessages.TryRemove(message.Id, out _);
        }
    }

    public async Task MoveToDeadLetterAsync(InboxMessage message, string reason, CancellationToken token)
    {
        await _storageProvider.MoveToDeadLetterAsync(message.Id, reason, token);
        _inFlightMessages.TryRemove(message.Id, out _);
    }

    public async Task MoveToDeadLetterBatchAsync(IReadOnlyList<(InboxMessage Message, string Reason)> messages, CancellationToken token)
    {
        if (messages.Count == 0)
        {
            return;
        }

        await _storageProvider.MoveToDeadLetterBatchAsync(
            messages.Select(m => (m.Message.Id, m.Reason)).ToList(),
            token);

        foreach (var (message, _) in messages)
        {
            _inFlightMessages.TryRemove(message.Id, out _);
        }
    }

    public void Clear() => _inFlightMessages.Clear();

    private string GetMaxAttemptsExceededReason() =>
        $"Max attempts ({_options.MaxAttempts}) exceeded";

    private string GetDeadLetterReason(string? failureReason, bool isMaxAttemptsExceeded) =>
        failureReason ?? (isMaxAttemptsExceeded ? GetMaxAttemptsExceededReason() : DefaultDeadLetterReason);
}