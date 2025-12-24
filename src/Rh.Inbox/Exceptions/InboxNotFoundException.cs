namespace Rh.Inbox.Exceptions;

/// <summary>
/// Exception thrown when an inbox with the specified name is not found.
/// </summary>
public sealed class InboxNotFoundException : Exception
{
    /// <summary>
    /// Gets the name of the inbox that was not found.
    /// </summary>
    public string InboxName { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="InboxNotFoundException"/> class.
    /// </summary>
    /// <param name="inboxName">The name of the inbox that was not found.</param>
    public InboxNotFoundException(string inboxName)
        : base($"Rh.Inbox '{inboxName}' not found.")
    {
        InboxName = inboxName;
    }
}
