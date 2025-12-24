namespace Rh.Inbox.Exceptions;

/// <summary>
/// Base exception class for all inbox-related exceptions.
/// </summary>
/// <param name="message">The error message.</param>
/// <param name="inner">The inner exception, if any.</param>
public class InboxBaseException(string message, Exception? inner = null) : Exception(message, inner);