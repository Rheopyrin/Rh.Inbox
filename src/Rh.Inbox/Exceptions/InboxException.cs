namespace Rh.Inbox.Exceptions;

/// <summary>
/// Exception thrown for errors related to a specific inbox.
/// </summary>
public class InboxException : InboxBaseException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="InboxException"/> class.
    /// </summary>
    /// <param name="name">The name of the inbox.</param>
    /// <param name="message">The error message.</param>
    /// <param name="inner">The inner exception, if any.</param>
    public InboxException(string name, string message, Exception? inner = null)
        : base(message, inner)
    {
        InboxName = name;
    }

    /// <summary>
    /// Gets the name of the inbox that caused the exception.
    /// </summary>
    public string InboxName { get; }
}