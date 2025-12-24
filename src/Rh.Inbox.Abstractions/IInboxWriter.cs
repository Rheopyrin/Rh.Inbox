namespace Rh.Inbox.Abstractions;

/// <summary>
/// Service for writing messages to inboxes.
/// Messages are persisted to storage and will be processed by the appropriate handler.
/// </summary>
public interface IInboxWriter
{
    /// <summary>
    /// Writes a single message to the inbox associated with the message type.
    /// The inbox is determined by the <see cref="Messages.InboxMessageAttribute"/> on the message class.
    /// </summary>
    /// <typeparam name="TMessage">The type of message to write.</typeparam>
    /// <param name="message">The message to write.</param>
    /// <param name="token">Cancellation token.</param>
    Task WriteAsync<TMessage>(TMessage message, CancellationToken token = default) where TMessage : class;

    /// <summary>
    /// Writes a single message to a specific inbox by name.
    /// </summary>
    /// <typeparam name="TMessage">The type of message to write.</typeparam>
    /// <param name="message">The message to write.</param>
    /// <param name="inboxName">The name of the inbox to write to.</param>
    /// <param name="token">Cancellation token.</param>
    Task WriteAsync<TMessage>(TMessage message, string inboxName, CancellationToken token = default) where TMessage : class;

    /// <summary>
    /// Writes multiple messages to the inbox associated with the message type.
    /// The inbox is determined by the <see cref="Messages.InboxMessageAttribute"/> on the message class.
    /// </summary>
    /// <typeparam name="TMessage">The type of messages to write.</typeparam>
    /// <param name="messages">The messages to write.</param>
    /// <param name="token">Cancellation token.</param>
    Task WriteBatchAsync<TMessage>(IEnumerable<TMessage> messages, CancellationToken token = default) where TMessage : class;

    /// <summary>
    /// Writes multiple messages to a specific inbox by name.
    /// </summary>
    /// <typeparam name="TMessage">The type of messages to write.</typeparam>
    /// <param name="messages">The messages to write.</param>
    /// <param name="inboxName">The name of the inbox to write to.</param>
    /// <param name="token">Cancellation token.</param>
    Task WriteBatchAsync<TMessage>(IEnumerable<TMessage> messages, string inboxName, CancellationToken token = default) where TMessage : class;
}
