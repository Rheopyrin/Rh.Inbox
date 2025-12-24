using System.Diagnostics;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Rh.Inbox.Abstractions;
using Rh.Inbox.Extensions;
using Rh.Inbox.Redis;
using Rh.Inbox.Tests.Integration.Common;
using Rh.Inbox.Tests.Integration.Common.Handlers;
using Rh.Inbox.Tests.Integration.Common.TestMessages;
using Xunit;
using Xunit.Abstractions;

namespace Rh.Inbox.Tests.Integration.Redis;

[Collection("Redis")]
public class RedisLargePayloadTests(RedisContainerFixture container, ITestOutputHelper output) : IAsyncLifetime
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
    [InlineData(PayloadSizes.Small, 200, "1KB")]
    [InlineData(PayloadSizes.MediumLow, 100, "50KB")]
    [InlineData(PayloadSizes.MediumHigh, 50, "256KB")]
    [InlineData(PayloadSizes.Large, 10, "1MB")]
    public async Task WriteAndProcess_VariousPayloadSizes_MeasuresPerformance(int payloadSize, int messageCount, string sizeLabel)
    {
        var inboxName = $"redis-payload-{sizeLabel.ToLower().Replace(" ", "")}";
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

        var totalDataMB = (payloadSize * messageCount) / (1024.0 * 1024.0);
        output.WriteLine($"{sizeLabel}: {messageCount} msgs, {totalDataMB:F2}MB total");
        output.WriteLine($"Write: {writeSw.ElapsedMilliseconds}ms ({totalDataMB / writeSw.Elapsed.TotalSeconds:F2} MB/s)");
        output.WriteLine($"Process: {elapsed.TotalMilliseconds:F0}ms ({totalDataMB / elapsed.TotalSeconds:F2} MB/s)");
    }

    private ServiceProvider CreateServiceProvider(string inboxName, LargePayloadTrackingHandler handler)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInbox(inboxName, builder =>
        {
            builder.AsDefault()
                .UseRedis(container.ConnectionString)
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
