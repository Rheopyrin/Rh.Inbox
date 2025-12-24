namespace Rh.Inbox.Abstractions.Messages;

/// <summary>
/// Attribute to configure inbox message settings.
/// Apply this to message classes to customize serialization behavior.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public class InboxMessageAttribute : Attribute
{
    /// <summary>
    /// Gets or sets a custom message type name for serialization.
    /// If not specified, the full type name of the class is used.
    /// Useful for maintaining compatibility when renaming or moving message classes.
    /// </summary>
    public string? MessageType { get; set; }
}
