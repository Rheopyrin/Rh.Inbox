using FluentAssertions;
using NSubstitute;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Configuration;
using Rh.Inbox.Health;
using Rh.Inbox.Inboxes.Factory;
using Rh.Inbox.Inboxes.Implementation;
using Rh.Inbox.Tests.Unit.TestHelpers;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Inboxes.Factory;

public class InboxFactoryTests
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IDateTimeProvider _dateTimeProvider;
    private readonly InboxFactory _factory;

    public InboxFactoryTests()
    {
        _serviceProvider = Substitute.For<IServiceProvider>();
        _dateTimeProvider = Substitute.For<IDateTimeProvider>();
        _factory = new InboxFactory(_serviceProvider, _dateTimeProvider);
    }

    #region Create Tests - Inbox Types

    [Fact]
    public void Create_DefaultType_ReturnsDefaultInbox()
    {
        var config = TestConfigurationFactory.CreateValidConfiguration("default-inbox", InboxType.Default);

        var inbox = _factory.Create(config);

        inbox.Should().BeOfType<DefaultInbox>();
        inbox.Name.Should().Be("default-inbox");
        inbox.Type.Should().Be(InboxType.Default);
    }

    [Fact]
    public void Create_BatchedType_ReturnsBatchedInbox()
    {
        var config = TestConfigurationFactory.CreateValidConfiguration("batched-inbox", InboxType.Batched);

        var inbox = _factory.Create(config);

        inbox.Should().BeOfType<BatchedInbox>();
        inbox.Name.Should().Be("batched-inbox");
        inbox.Type.Should().Be(InboxType.Batched);
    }

    [Fact]
    public void Create_FifoType_ReturnsFifoInbox()
    {
        var config = TestConfigurationFactory.CreateValidConfiguration("fifo-inbox", InboxType.Fifo);

        var inbox = _factory.Create(config);

        inbox.Should().BeOfType<FifoInbox>();
        inbox.Name.Should().Be("fifo-inbox");
        inbox.Type.Should().Be(InboxType.Fifo);
    }

    [Fact]
    public void Create_FifoBatchedType_ReturnsFifoBatchedInbox()
    {
        var config = TestConfigurationFactory.CreateValidConfiguration("fifo-batched-inbox", InboxType.FifoBatched);

        var inbox = _factory.Create(config);

        inbox.Should().BeOfType<FifoBatchedInbox>();
        inbox.Name.Should().Be("fifo-batched-inbox");
        inbox.Type.Should().Be(InboxType.FifoBatched);
    }

    [Fact]
    public void Create_UnknownType_ThrowsInvalidOperationException()
    {
        var config = TestConfigurationFactory.CreateValidConfiguration("unknown-inbox", (InboxType)999);

        var act = () => _factory.Create(config);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("Unknown inbox type*");
    }

    #endregion

    #region Create Tests - Storage Provider

    [Fact]
    public void Create_PassesConfigurationToStorageProviderFactory()
    {
        var options = TestConfigurationFactory.CreateOptions(
            inboxName: "options-test",
            readBatchSize: 50,
            maxProcessingTime: TimeSpan.FromMinutes(10),
            enableDeadLetter: false);

        var serializerFactory = Substitute.For<IInboxSerializerFactory>();
        serializerFactory.Create(Arg.Any<string>()).Returns(Substitute.For<IInboxMessagePayloadSerializer>());

        IInboxConfiguration? capturedConfig = null;
        var storageProviderFactory = Substitute.For<IInboxStorageProviderFactory>();
        storageProviderFactory.Create(Arg.Do<IInboxConfiguration>(c => capturedConfig = c))
            .Returns(Substitute.For<IInboxStorageProvider>());

        var config = new InboxConfiguration
        {
            InboxName = "options-test",
            InboxType = InboxType.Default,
            Options = options,
            MetadataRegistry = Substitute.For<IInboxMessageMetadataRegistry>(),
            StorageProviderFactoryFunc = _ => storageProviderFactory,
            SerializerFactoryFunc = _ => serializerFactory,
            HealthCheckOptions = new InboxHealthCheckOptions(),
            DateTimeProvider = Substitute.For<IDateTimeProvider>()
        };

        _factory.Create(config);

        capturedConfig.Should().NotBeNull();
        capturedConfig!.InboxName.Should().Be("options-test");
        capturedConfig.InboxType.Should().Be(InboxType.Default);
        capturedConfig.Options.ReadBatchSize.Should().Be(50);
        capturedConfig.Options.MaxProcessingTime.Should().Be(TimeSpan.FromMinutes(10));
        capturedConfig.Options.EnableDeadLetter.Should().BeFalse();
    }

    #endregion

    #region Create Tests - Serializer

    [Fact]
    public void Create_CallsSerializerFactoryWithInboxName()
    {
        var serializerFactory = Substitute.For<IInboxSerializerFactory>();
        serializerFactory.Create(Arg.Any<string>()).Returns(Substitute.For<IInboxMessagePayloadSerializer>());

        var storageProviderFactory = Substitute.For<IInboxStorageProviderFactory>();
        storageProviderFactory.Create(Arg.Any<IInboxConfiguration>()).Returns(Substitute.For<IInboxStorageProvider>());

        var config = new InboxConfiguration
        {
            InboxName = "serializer-test",
            InboxType = InboxType.Default,
            Options = TestConfigurationFactory.CreateOptions("serializer-test"),
            MetadataRegistry = Substitute.For<IInboxMessageMetadataRegistry>(),
            StorageProviderFactoryFunc = _ => storageProviderFactory,
            SerializerFactoryFunc = _ => serializerFactory,
            HealthCheckOptions = new InboxHealthCheckOptions(),
            DateTimeProvider = Substitute.For<IDateTimeProvider>()
        };

        _factory.Create(config);

        serializerFactory.Received(1).Create("serializer-test");
    }

    #endregion
}
