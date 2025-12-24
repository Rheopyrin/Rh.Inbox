namespace Rh.Inbox.Abstractions.Handlers;

/// <summary>
/// Wraps a message with its inbox-assigned identifier.
/// Used to correlate handler results back to specific messages.
/// </summary>
/// <typeparam name="TMessage">The type of the message payload.</typeparam>
/// <param name="Id">The unique identifier assigned to this message by the inbox.</param>
/// <param name="Payload">The deserialized message content.</param>
public record InboxMessageEnvelope<TMessage>(Guid Id, TMessage Payload) where TMessage : class;
