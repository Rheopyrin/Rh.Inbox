using FluentAssertions;
using NSubstitute;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Configuration;
using Rh.Inbox.Health;
using Rh.Inbox.Tests.Unit.TestHelpers;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Configuration;

public class InboxConfigurationTests
{
    #region Required Properties Tests

    [Fact]
    public void InboxConfiguration_SetsInboxName()
    {
        var config = TestConfigurationFactory.CreateConfiguration("test-inbox");

        config.InboxName.Should().Be("test-inbox");
    }

    [Fact]
    public void InboxConfiguration_SetsOptions()
    {
        var options = TestConfigurationFactory.CreateOptions(inboxName: "test", readBatchSize: 50);
        var config = TestConfigurationFactory.CreateConfiguration("test", options: options);

        config.Options.Should().BeSameAs(options);
        config.Options.ReadBatchSize.Should().Be(50);
    }

    [Fact]
    public void InboxConfiguration_SetsMetadataRegistry()
    {
        var metadataRegistry = Substitute.For<IInboxMessageMetadataRegistry>();
        var config = TestConfigurationFactory.CreateConfiguration("test", metadataRegistry: metadataRegistry);

        config.MetadataRegistry.Should().BeSameAs(metadataRegistry);
    }

    [Fact]
    public void InboxConfiguration_SetsHealthCheckOptions()
    {
        var config = TestConfigurationFactory.CreateConfiguration("test");

        config.HealthCheckOptions.Should().NotBeNull();
    }

    #endregion

    #region InboxType Tests

    [Theory]
    [InlineData(InboxType.Default)]
    [InlineData(InboxType.Batched)]
    [InlineData(InboxType.Fifo)]
    [InlineData(InboxType.FifoBatched)]
    public void InboxConfiguration_InboxTypeCanBeSet(InboxType expectedType)
    {
        var config = TestConfigurationFactory.CreateConfiguration("test", expectedType);

        config.InboxType.Should().Be(expectedType);
    }

    #endregion

    #region SerializerFactoryFunc Tests

    [Fact]
    public void SerializerFactoryFunc_CanBeSet()
    {
        var expectedFactory = Substitute.For<IInboxSerializerFactory>();
        Func<IServiceProvider, IInboxSerializerFactory> factoryFunc = _ => expectedFactory;

        var config = new InboxConfiguration
        {
            InboxName = "test",
            InboxType = InboxType.Default,
            Options = TestConfigurationFactory.CreateOptions("test"),
            MetadataRegistry = Substitute.For<IInboxMessageMetadataRegistry>(),
            StorageProviderFactoryFunc = _ => Substitute.For<IInboxStorageProviderFactory>(),
            SerializerFactoryFunc = factoryFunc,
            HealthCheckOptions = new InboxHealthCheckOptions(),
            DateTimeProvider = Substitute.For<IDateTimeProvider>()
        };

        config.SerializerFactoryFunc.Should().NotBeNull();
        config.SerializerFactoryFunc(Substitute.For<IServiceProvider>()).Should().BeSameAs(expectedFactory);
    }

    #endregion

    #region StorageProviderFactoryFunc Tests

    [Fact]
    public void StorageProviderFactoryFunc_CanBeSet()
    {
        var expectedFactory = Substitute.For<IInboxStorageProviderFactory>();
        Func<IServiceProvider, IInboxStorageProviderFactory> factoryFunc = _ => expectedFactory;

        var config = new InboxConfiguration
        {
            InboxName = "test",
            InboxType = InboxType.Default,
            Options = TestConfigurationFactory.CreateOptions("test"),
            MetadataRegistry = Substitute.For<IInboxMessageMetadataRegistry>(),
            StorageProviderFactoryFunc = factoryFunc,
            SerializerFactoryFunc = _ => Substitute.For<IInboxSerializerFactory>(),
            HealthCheckOptions = new InboxHealthCheckOptions(),
            DateTimeProvider = Substitute.For<IDateTimeProvider>()
        };

        config.StorageProviderFactoryFunc.Should().NotBeNull();
        config.StorageProviderFactoryFunc(Substitute.For<IServiceProvider>()).Should().BeSameAs(expectedFactory);
    }

    #endregion
}
