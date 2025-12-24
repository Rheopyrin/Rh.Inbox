using FluentAssertions;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Configuration;
using Rh.Inbox.Health;
using Rh.Inbox.Inboxes;
using Rh.Inbox.Processing.Strategies.Factory;
using Rh.Inbox.Processing.Strategies.Implementation;
using Rh.Inbox.Tests.Unit.TestHelpers;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Processing.Strategies.Factory;

public class InboxProcessingStrategyFactoryTests
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<InboxProcessingStrategyFactory> _logger;
    private readonly InboxProcessingStrategyFactory _factory;

    public InboxProcessingStrategyFactoryTests()
    {
        _serviceProvider = Substitute.For<IServiceProvider>();
        _logger = Substitute.For<ILogger<InboxProcessingStrategyFactory>>();
        _factory = new InboxProcessingStrategyFactory(_serviceProvider, _logger);
    }

    private InboxBase CreateMockInbox(InboxType type)
    {
        var options = TestConfigurationFactory.CreateOptions("test-inbox");
        var metadataRegistry = Substitute.For<IInboxMessageMetadataRegistry>();

        var configuration = new InboxConfiguration
        {
            InboxName = "test-inbox",
            InboxType = type,
            Options = options,
            MetadataRegistry = metadataRegistry,
            StorageProviderFactoryFunc = _ => Substitute.For<IInboxStorageProviderFactory>(),
            SerializerFactoryFunc = _ => Substitute.For<IInboxSerializerFactory>(),
            HealthCheckOptions = new InboxHealthCheckOptions(),
            DateTimeProvider = Substitute.For<IDateTimeProvider>()
        };

        var storageProvider = Substitute.For<IInboxStorageProvider>();
        var serializer = Substitute.For<IInboxMessagePayloadSerializer>();
        var dateTimeProvider = Substitute.For<IDateTimeProvider>();

        var inbox = Substitute.For<InboxBase>(configuration, storageProvider, serializer, dateTimeProvider);
        inbox.Type.Returns(type);
        return inbox;
    }

    #region Create Tests

    [Fact]
    public void Create_DefaultType_ReturnsDefaultInboxProcessingStrategy()
    {
        var inbox = CreateMockInbox(InboxType.Default);

        var strategy = _factory.Create(inbox);

        strategy.Should().BeOfType<DefaultInboxProcessingStrategy>();
    }

    [Fact]
    public void Create_BatchedType_ReturnsBatchedInboxProcessingStrategy()
    {
        var inbox = CreateMockInbox(InboxType.Batched);

        var strategy = _factory.Create(inbox);

        strategy.Should().BeOfType<BatchedInboxProcessingStrategy>();
    }

    [Fact]
    public void Create_FifoType_ReturnsFifoInboxProcessingStrategy()
    {
        var inbox = CreateMockInbox(InboxType.Fifo);

        var strategy = _factory.Create(inbox);

        strategy.Should().BeOfType<FifoInboxProcessingStrategy>();
    }

    [Fact]
    public void Create_FifoBatchedType_ReturnsFifoBatchedInboxProcessingStrategy()
    {
        var inbox = CreateMockInbox(InboxType.FifoBatched);

        var strategy = _factory.Create(inbox);

        strategy.Should().BeOfType<FifoBatchedInboxProcessingStrategy>();
    }

    [Fact]
    public void Create_UnknownType_ThrowsInvalidOperationException()
    {
        var inbox = CreateMockInbox((InboxType)999);

        var act = () => _factory.Create(inbox);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("Unknown inbox type*");
    }

    #endregion
}
