using NSubstitute;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Configuration;
using Rh.Inbox.Health;
using Rh.Inbox.Inboxes;
using Rh.Inbox.Management;
using Rh.Inbox.Tests.Unit.TestHelpers;
using Rh.Inbox.Writers;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Writers;

public class InboxWriterTests
{
    private readonly IInboxManagerInternal _inboxManager;
    private readonly InboxWriter _writer;

    public InboxWriterTests()
    {
        _inboxManager = Substitute.For<IInboxManagerInternal>();
        _writer = new InboxWriter(_inboxManager);
    }

    private (InboxBase inbox, IInboxStorageProvider storageProvider) CreateMockInbox(string inboxName)
    {
        var options = TestConfigurationFactory.CreateOptions(inboxName);
        var metadataRegistry = Substitute.For<IInboxMessageMetadataRegistry>();
        metadataRegistry.GetMessageType(Arg.Any<Type>()).Returns("TestMessage");

        var configuration = new InboxConfiguration
        {
            InboxName = inboxName,
            InboxType = InboxType.Default,
            Options = options,
            MetadataRegistry = metadataRegistry,
            StorageProviderFactoryFunc = _ => Substitute.For<IInboxStorageProviderFactory>(),
            SerializerFactoryFunc = _ => Substitute.For<IInboxSerializerFactory>(),
            HealthCheckOptions = new InboxHealthCheckOptions(),
            DateTimeProvider = Substitute.For<IDateTimeProvider>()
        };

        var storageProvider = Substitute.For<IInboxStorageProvider>();
        var serializer = Substitute.For<IInboxMessagePayloadSerializer>();
        serializer.Serialize(Arg.Any<object>(), Arg.Any<Type>()).Returns("{}");
        var dateTimeProvider = Substitute.For<IDateTimeProvider>();
        dateTimeProvider.GetUtcNow().Returns(DateTime.UtcNow);

        var inbox = Substitute.For<InboxBase>(configuration, storageProvider, serializer, dateTimeProvider);
        return (inbox, storageProvider);
    }

    #region WriteAsync Tests

    [Fact]
    public async Task WriteAsync_WithDefaultInbox_UsesDefaultInboxName()
    {
        var (inbox, _) = CreateMockInbox(InboxOptions.DefaultInboxName);
        _inboxManager.GetInboxInternal(InboxOptions.DefaultInboxName).Returns(inbox);

        await _writer.WriteAsync(new TestMessage(), CancellationToken.None);

        _inboxManager.Received(1).GetInboxInternal(InboxOptions.DefaultInboxName);
    }

    [Fact]
    public async Task WriteAsync_WithNamedInbox_UsesSpecifiedInboxName()
    {
        var (inbox, _) = CreateMockInbox("custom-inbox");
        _inboxManager.GetInboxInternal("custom-inbox").Returns(inbox);

        await _writer.WriteAsync(new TestMessage(), "custom-inbox", CancellationToken.None);

        _inboxManager.Received(1).GetInboxInternal("custom-inbox");
    }

    [Fact]
    public async Task WriteAsync_CallsStorageProviderWrite()
    {
        var (inbox, storageProvider) = CreateMockInbox("test-inbox");
        _inboxManager.GetInboxInternal("test-inbox").Returns(inbox);

        await _writer.WriteAsync(new TestMessage { Id = 123 }, "test-inbox", CancellationToken.None);

        await storageProvider.Received(1).WriteAsync(Arg.Any<InboxMessage>(), Arg.Any<CancellationToken>());
    }

    #endregion

    #region WriteBatchAsync Tests

    [Fact]
    public async Task WriteBatchAsync_EmptyBatch_DoesNotCallStorageProvider()
    {
        var (inbox, storageProvider) = CreateMockInbox("test-inbox");
        _inboxManager.GetInboxInternal("test-inbox").Returns(inbox);

        await _writer.WriteBatchAsync(Array.Empty<TestMessage>(), "test-inbox", CancellationToken.None);

        await storageProvider.DidNotReceive().WriteBatchAsync(Arg.Any<IEnumerable<InboxMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task WriteBatchAsync_DefaultOverload_UsesDefaultInboxName()
    {
        var (inbox, _) = CreateMockInbox(InboxOptions.DefaultInboxName);
        _inboxManager.GetInboxInternal(InboxOptions.DefaultInboxName).Returns(inbox);

        // Empty batch avoids CreateInboxMessage complexity
        await _writer.WriteBatchAsync(Array.Empty<TestMessage>(), CancellationToken.None);

        _inboxManager.Received(1).GetInboxInternal(InboxOptions.DefaultInboxName);
    }

    #endregion

    #region Test Helper Classes

    private class TestMessage
    {
        public int Id { get; set; }
    }

    #endregion
}
