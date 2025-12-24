namespace Rh.Inbox.Exceptions;

/// <summary>
/// Represents a configuration error for an inbox option.
/// </summary>
/// <param name="OptionName">The name of the option with the error.</param>
/// <param name="ErrorMessage">The error message describing the issue.</param>
public record InboxOptionError(string OptionName, string ErrorMessage);

/// <summary>
/// Exception thrown when inbox configuration is invalid.
/// </summary>
public class InvalidInboxConfigurationException : InboxBaseException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="InvalidInboxConfigurationException"/> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="inner">The inner exception, if any.</param>
    public InvalidInboxConfigurationException(string message, Exception? inner = null)
        : base(message, inner)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="InvalidInboxConfigurationException"/> class with validation errors.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="errors">The collection of option validation errors.</param>
    /// <param name="inner">The inner exception, if any.</param>
    public InvalidInboxConfigurationException(string message, IEnumerable<InboxOptionError> errors, Exception? inner = null)
        : base(message, inner)
    {
        Errors = errors.ToArray();
    }

    /// <summary>
    /// Gets the collection of option validation errors.
    /// </summary>
    public InboxOptionError[] Errors { get; } = Array.Empty<InboxOptionError>();
}