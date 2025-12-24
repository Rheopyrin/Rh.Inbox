namespace Rh.Inbox.Exceptions;

/// <summary>
/// Exception thrown when a message is invalid for an inbox.
/// </summary>
public class InvalidInboxMessageException : InboxException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="InvalidInboxMessageException"/> class.
    /// </summary>
    /// <param name="inboxName">The name of the inbox.</param>
    /// <param name="message">The error message.</param>
    /// <param name="inner">The inner exception, if any.</param>
    public InvalidInboxMessageException(string inboxName, string message, Exception? inner = null)
        : base(inboxName, message, inner)
    {
    }
}