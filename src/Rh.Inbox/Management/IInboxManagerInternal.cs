using Rh.Inbox.Inboxes;

namespace Rh.Inbox.Management;

/// <summary>
/// Internal interface for inbox manager operations that are not part of the public API.
/// This allows InboxWriter to access internal functionality without coupling to concrete types.
/// </summary>
internal interface IInboxManagerInternal
{
    InboxBase GetInboxInternal(string name);
}