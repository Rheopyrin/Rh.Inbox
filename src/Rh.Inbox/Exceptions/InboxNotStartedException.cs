namespace Rh.Inbox.Exceptions;

/// <summary>
/// Exception thrown when an operation is attempted on an inbox that has not been started.
/// </summary>
public class InboxNotStartedException : InboxBaseException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="InboxNotStartedException"/> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="inner">The inner exception, if any.</param>
    public InboxNotStartedException(string message, Exception? inner = null)
        : base(message, inner)
    {
    }
}