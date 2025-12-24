using NSubstitute;
using Rh.Inbox.Abstractions.Configuration;
using Rh.Inbox.Abstractions.Providers;
using Rh.Inbox.Abstractions.Serialization;
using Rh.Inbox.Abstractions.Storage;
using Rh.Inbox.Configuration;
using Rh.Inbox.Health;

namespace Rh.Inbox.Tests.Unit.TestHelpers;

/// <summary>
/// Factory for creating test configurations with default values.
/// </summary>
internal static class TestConfigurationFactory
{
    public static InboxOptions CreateOptions(
        string? inboxName = null,
        int? readBatchSize = null,
        int? writeBatchSize = null,
        TimeSpan? maxProcessingTime = null,
        TimeSpan? pollingInterval = null,
        TimeSpan? readDelay = null,
        TimeSpan? shutdownTimeout = null,
        int? maxAttempts = null,
        bool? enableDeadLetter = null,
        int? maxProcessingThreads = null,
        int? maxWriteThreads = null,
        TimeSpan? deduplicationInterval = null,
        bool? enableLockExtension = null,
        double? lockExtensionThreshold = null,
        TimeSpan? deadLetterMaxMessageLifetime = null,
        bool? enableDeduplication = null)
    {
        return new InboxOptions
        {
            InboxName = inboxName ?? "test-inbox",
            ReadBatchSize = readBatchSize ?? 100,
            WriteBatchSize = writeBatchSize ?? 100,
            MaxProcessingTime = maxProcessingTime ?? TimeSpan.FromMinutes(5),
            PollingInterval = pollingInterval ?? TimeSpan.FromSeconds(5),
            ReadDelay = readDelay ?? TimeSpan.Zero,
            ShutdownTimeout = shutdownTimeout ?? TimeSpan.FromSeconds(30),
            MaxAttempts = maxAttempts ?? 3,
            EnableDeadLetter = enableDeadLetter ?? true,
            MaxProcessingThreads = maxProcessingThreads ?? 1,
            MaxWriteThreads = maxWriteThreads ?? 1,
            DeduplicationInterval = deduplicationInterval ?? TimeSpan.Zero,
            EnableLockExtension = enableLockExtension ?? false,
            LockExtensionThreshold = lockExtensionThreshold ?? 0.5,
            DeadLetterMaxMessageLifetime = deadLetterMaxMessageLifetime ?? TimeSpan.Zero,
            EnableDeduplication = enableDeduplication ?? false,
            DateTimeProvider = Substitute.For<IDateTimeProvider>()
        };
    }

    public static InboxConfiguration CreateConfiguration(
        string? inboxName = null,
        InboxType inboxType = InboxType.Default,
        InboxOptions? options = null,
        IInboxMessageMetadataRegistry? metadataRegistry = null,
        Func<IServiceProvider, IInboxStorageProviderFactory>? storageProviderFactoryFunc = null,
        Func<IServiceProvider, IInboxSerializerFactory>? serializerFactoryFunc = null,
        IInboxHealthCheckOptions? healthCheckOptions = null,
        IDateTimeProvider? dateTimeProvider = null)
    {
        var name = inboxName ?? "test-inbox";

        return new InboxConfiguration
        {
            InboxName = name,
            InboxType = inboxType,
            Options = options ?? CreateOptions(name),
            MetadataRegistry = metadataRegistry ?? Substitute.For<IInboxMessageMetadataRegistry>(),
            StorageProviderFactoryFunc = storageProviderFactoryFunc ?? (_ => Substitute.For<IInboxStorageProviderFactory>()),
            SerializerFactoryFunc = serializerFactoryFunc ?? (_ => Substitute.For<IInboxSerializerFactory>()),
            HealthCheckOptions = healthCheckOptions ?? new InboxHealthCheckOptions(),
            DateTimeProvider = dateTimeProvider ?? Substitute.For<IDateTimeProvider>()
        };
    }

    public static InboxConfiguration CreateValidConfiguration(
        string? inboxName = null,
        InboxType inboxType = InboxType.Default,
        InboxOptions? options = null)
    {
        var name = inboxName ?? "test-inbox";
        var serializerFactory = Substitute.For<IInboxSerializerFactory>();
        var serializer = Substitute.For<IInboxMessagePayloadSerializer>();
        serializerFactory.Create(Arg.Any<string>()).Returns(serializer);

        var storageProviderFactory = Substitute.For<IInboxStorageProviderFactory>();
        var storageProvider = Substitute.For<IInboxStorageProvider>();
        storageProviderFactory.Create(Arg.Any<IInboxConfiguration>()).Returns(storageProvider);

        return new InboxConfiguration
        {
            InboxName = name,
            InboxType = inboxType,
            Options = options ?? CreateOptions(name),
            MetadataRegistry = Substitute.For<IInboxMessageMetadataRegistry>(),
            StorageProviderFactoryFunc = _ => storageProviderFactory,
            SerializerFactoryFunc = _ => serializerFactory,
            HealthCheckOptions = new InboxHealthCheckOptions(),
            DateTimeProvider = Substitute.For<IDateTimeProvider>()
        };
    }
}