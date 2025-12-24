using System.Diagnostics;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Extensions;
using Rh.Inbox.Postgres;
using Rh.Inbox.Tests.Integration.Common;
using Rh.Inbox.Tests.Integration.Common.Handlers;
using Rh.Inbox.Tests.Integration.Common.TestMessages;
using Xunit;
using Xunit.Abstractions;

namespace Rh.Inbox.Tests.Integration.Postgres;

[Collection("Postgres")]
public class PostgresLargePayloadTests(PostgresContainerFixture container, ITestOutputHelper output) : IAsyncLifetime
{
    private ServiceProvider? _serviceProvider;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (_serviceProvider is not null)
        {
            await _serviceProvider.DisposeAsync();
        }
    }

    [Theory]
    [InlineData(PayloadSizes.Small, 100, "1KB")]
    [InlineData(PayloadSizes.MediumLow, 50, "50KB")]
    [InlineData(PayloadSizes.MediumHigh, 20, "256KB")]
    [InlineData(PayloadSizes.Large, 5, "1MB")]
    public async Task WriteAndProcess_VariousPayloadSizes_MeasuresPerformance(int payloadSize, int messageCount, string sizeLabel)
    {
        var inboxName = $"payload-{sizeLabel.ToLower().Replace(" ", "")}";
        var handler = new LargePayloadTrackingHandler();
        _serviceProvider = CreateServiceProvider(inboxName, handler);

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        var messages = TestMessageFactory.CreateLargePayloadMessages(messageCount, payloadSize);

        var writeSw = Stopwatch.StartNew();
        foreach (var msg in messages)
        {
            await writer.WriteAsync(msg, inboxName);
        }
        writeSw.Stop();

        var elapsed = await TestWaitHelper.WaitForCountAsync(
            () => handler.ProcessedCount, messageCount, TestConstants.LongProcessingTimeout);

        handler.ProcessedCount.Should().Be(messageCount);
        handler.PayloadSizesValid.Should().BeTrue("all payloads should maintain their size");

        var totalDataMB = (payloadSize * messageCount) / (1024.0 * 1024.0);
        output.WriteLine($"{sizeLabel}: {messageCount} msgs, {totalDataMB:F2}MB total");
        output.WriteLine($"Write: {writeSw.ElapsedMilliseconds}ms ({totalDataMB / writeSw.Elapsed.TotalSeconds:F2} MB/s)");
        output.WriteLine($"Process: {elapsed.TotalMilliseconds:F0}ms ({totalDataMB / elapsed.TotalSeconds:F2} MB/s)");
    }

    [Fact]
    public async Task BatchWrite_LargePayloads_MeasuresBatchEfficiency()
    {
        var handler = new LargePayloadTrackingHandler();
        _serviceProvider = CreateServiceProvider("batch-large-payload", handler);

        var writer = _serviceProvider.GetRequiredService<IInboxWriter>();
        await _serviceProvider.GetRequiredService<IInboxMigrationService>().MigrateAsync();
        await _serviceProvider.GetRequiredService<IInboxManager>().StartAsync(CancellationToken.None);

        const int messageCount = 50;
        var messages = TestMessageFactory.CreateLargePayloadMessages(messageCount, PayloadSizes.MediumHigh);

        var batchSw = Stopwatch.StartNew();
        await writer.WriteBatchAsync(messages, "batch-large-payload");
        batchSw.Stop();

        var elapsed = await TestWaitHelper.WaitForCountAsync(
            () => handler.ProcessedCount, messageCount, TestConstants.LongProcessingTimeout);

        handler.ProcessedCount.Should().Be(messageCount);

        var totalDataMB = (PayloadSizes.MediumHigh * messageCount) / (1024.0 * 1024.0);
        output.WriteLine($"Batch write {messageCount} × 256KB = {totalDataMB:F2}MB in {batchSw.ElapsedMilliseconds}ms ({totalDataMB / batchSw.Elapsed.TotalSeconds:F2} MB/s)");
        output.WriteLine($"Process: {elapsed.TotalMilliseconds:F0}ms");
    }

    private ServiceProvider CreateServiceProvider(string inboxName, LargePayloadTrackingHandler handler)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox(inboxName, builder =>
        {
            builder.AsDefault()
                .UsePostgres(container.ConnectionString)
                .ConfigureOptions(o =>
                {
                    o.ReadBatchSize = 10;
                    o.PollingInterval = TimeSpan.FromMilliseconds(50);
                })
                .RegisterHandler(handler);
        });
        return services.BuildServiceProvider();
    }
}
