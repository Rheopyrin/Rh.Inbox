using FluentAssertions;
using Rh.Inbox.Redis.Connection;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Redis;

public class RedisConnectionProviderTests
{
    #region Dispose Tests

    [Fact]
    public void Dispose_WithNoCreatedConnections_CompletesSuccessfully()
    {
        var provider = new RedisConnectionProvider();

        var act = () => provider.Dispose();

        act.Should().NotThrow();
    }

    [Fact]
    public void Dispose_CalledTwice_NoOp()
    {
        var provider = new RedisConnectionProvider();

        provider.Dispose();
        var act = () => provider.Dispose();

        act.Should().NotThrow();
    }

    [Fact]
    public async Task DisposeAsync_WithNoCreatedConnections_CompletesSuccessfully()
    {
        var provider = new RedisConnectionProvider();

        var act = async () => await provider.DisposeAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_NoOp()
    {
        var provider = new RedisConnectionProvider();

        await provider.DisposeAsync();
        var act = async () => await provider.DisposeAsync();

        await act.Should().NotThrowAsync();
    }

    #endregion

    #region GetConnectionAsync Tests

    [Fact]
    public async Task GetConnectionAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var provider = new RedisConnectionProvider();
        provider.Dispose();

        var act = async () => await provider.GetConnectionAsync("localhost:6379");

        await act.Should().ThrowAsync<ObjectDisposedException>();
    }

    [Fact]
    public async Task GetConnectionAsync_AfterDisposeAsync_ThrowsObjectDisposedException()
    {
        var provider = new RedisConnectionProvider();
        await provider.DisposeAsync();

        var act = async () => await provider.GetConnectionAsync("localhost:6379");

        await act.Should().ThrowAsync<ObjectDisposedException>();
    }

    #endregion

    #region Interface Implementation Tests

    [Fact]
    public void Provider_ImplementsIRedisConnectionProvider()
    {
        var provider = new RedisConnectionProvider();

        provider.Should().BeAssignableTo<IRedisConnectionProvider>();
        provider.Dispose();
    }

    [Fact]
    public void Provider_ImplementsIAsyncDisposable()
    {
        var provider = new RedisConnectionProvider();

        provider.Should().BeAssignableTo<IAsyncDisposable>();
        provider.Dispose();
    }

    [Fact]
    public void Provider_ImplementsIDisposable()
    {
        var provider = new RedisConnectionProvider();

        provider.Should().BeAssignableTo<IDisposable>();
        provider.Dispose();
    }

    #endregion

    #region Lifecycle Tests

    [Fact]
    public void NewProvider_IsNotDisposed()
    {
        var provider = new RedisConnectionProvider();

        // Provider should be usable after construction
        var act = () => provider.Dispose();
        act.Should().NotThrow();
    }

    [Fact]
    public async Task Provider_CanBeUsedUntilDisposed()
    {
        var provider = new RedisConnectionProvider();

        // Dispose the provider
        await provider.DisposeAsync();

        // Subsequent connection attempts should throw
        var act = async () => await provider.GetConnectionAsync("localhost:6379");
        await act.Should().ThrowAsync<ObjectDisposedException>();
    }

    [Fact]
    public void Dispose_ThenDispose_IsIdempotent()
    {
        var provider = new RedisConnectionProvider();

        provider.Dispose();
        provider.Dispose();

        // No exception should be thrown
        var act = async () => await provider.GetConnectionAsync("localhost:6379");
        act.Should().ThrowAsync<ObjectDisposedException>();
    }

    [Fact]
    public async Task DisposeAsync_ThenDisposeAsync_IsIdempotent()
    {
        var provider = new RedisConnectionProvider();

        await provider.DisposeAsync();
        await provider.DisposeAsync();

        // No exception should be thrown
        var act = async () => await provider.GetConnectionAsync("localhost:6379");
        await act.Should().ThrowAsync<ObjectDisposedException>();
    }

    [Fact]
    public async Task Dispose_ThenDisposeAsync_IsIdempotent()
    {
        var provider = new RedisConnectionProvider();

        provider.Dispose();
        await provider.DisposeAsync();

        // Both dispose calls succeed without exception
        var act = async () => await provider.GetConnectionAsync("localhost:6379");
        await act.Should().ThrowAsync<ObjectDisposedException>();
    }

    #endregion
}
