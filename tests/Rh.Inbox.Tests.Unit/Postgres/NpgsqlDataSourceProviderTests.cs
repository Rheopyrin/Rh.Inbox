using FluentAssertions;
using Rh.Inbox.Postgres.Connection;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Postgres;

public class NpgsqlDataSourceProviderTests
{
    #region Dispose Tests

    [Fact]
    public void Dispose_WithNoCreatedConnections_CompletesSuccessfully()
    {
        var provider = new NpgsqlDataSourceProvider();

        var act = () => provider.Dispose();

        act.Should().NotThrow();
    }

    [Fact]
    public void Dispose_CalledTwice_NoOp()
    {
        var provider = new NpgsqlDataSourceProvider();

        provider.Dispose();
        var act = () => provider.Dispose();

        act.Should().NotThrow();
    }

    [Fact]
    public async Task DisposeAsync_WithNoCreatedConnections_CompletesSuccessfully()
    {
        var provider = new NpgsqlDataSourceProvider();

        var act = async () => await provider.DisposeAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_NoOp()
    {
        var provider = new NpgsqlDataSourceProvider();

        await provider.DisposeAsync();
        var act = async () => await provider.DisposeAsync();

        await act.Should().NotThrowAsync();
    }

    #endregion

    #region GetDataSource Tests

    [Fact]
    public void GetDataSource_AfterDispose_ThrowsObjectDisposedException()
    {
        var provider = new NpgsqlDataSourceProvider();
        provider.Dispose();

        var act = () => provider.GetDataSource("Host=localhost;Database=test");

        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public async Task GetDataSource_AfterDisposeAsync_ThrowsObjectDisposedException()
    {
        var provider = new NpgsqlDataSourceProvider();
        await provider.DisposeAsync();

        var act = () => provider.GetDataSource("Host=localhost;Database=test");

        act.Should().Throw<ObjectDisposedException>();
    }

    #endregion

    #region Interface Implementation Tests

    [Fact]
    public void Provider_ImplementsINpgsqlDataSourceProvider()
    {
        var provider = new NpgsqlDataSourceProvider();

        provider.Should().BeAssignableTo<INpgsqlDataSourceProvider>();
        provider.Dispose();
    }

    [Fact]
    public void Provider_ImplementsIAsyncDisposable()
    {
        var provider = new NpgsqlDataSourceProvider();

        provider.Should().BeAssignableTo<IAsyncDisposable>();
        provider.Dispose();
    }

    [Fact]
    public void Provider_ImplementsIDisposable()
    {
        var provider = new NpgsqlDataSourceProvider();

        provider.Should().BeAssignableTo<IDisposable>();
        provider.Dispose();
    }

    #endregion

    #region Lifecycle Tests

    [Fact]
    public void NewProvider_IsNotDisposed()
    {
        var provider = new NpgsqlDataSourceProvider();

        // Provider should be usable after construction
        var act = () => provider.Dispose();
        act.Should().NotThrow();
    }

    [Fact]
    public void Provider_CanBeUsedUntilDisposed()
    {
        var provider = new NpgsqlDataSourceProvider();

        // Dispose the provider
        provider.Dispose();

        // Subsequent data source access should throw
        var act = () => provider.GetDataSource("Host=localhost;Database=test");
        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void Dispose_ThenDispose_IsIdempotent()
    {
        var provider = new NpgsqlDataSourceProvider();

        provider.Dispose();
        provider.Dispose();

        // No exception should be thrown from multiple disposes
        var act = () => provider.GetDataSource("Host=localhost;Database=test");
        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public async Task DisposeAsync_ThenDisposeAsync_IsIdempotent()
    {
        var provider = new NpgsqlDataSourceProvider();

        await provider.DisposeAsync();
        await provider.DisposeAsync();

        // No exception should be thrown
        var act = () => provider.GetDataSource("Host=localhost;Database=test");
        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public async Task Dispose_ThenDisposeAsync_IsIdempotent()
    {
        var provider = new NpgsqlDataSourceProvider();

        provider.Dispose();
        await provider.DisposeAsync();

        // Both dispose calls succeed without exception
        var act = () => provider.GetDataSource("Host=localhost;Database=test");
        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public async Task DisposeAsync_ThenDispose_IsIdempotent()
    {
        var provider = new NpgsqlDataSourceProvider();

        await provider.DisposeAsync();
        provider.Dispose();

        // Both dispose calls succeed without exception
        var act = () => provider.GetDataSource("Host=localhost;Database=test");
        act.Should().Throw<ObjectDisposedException>();
    }

    #endregion
}
