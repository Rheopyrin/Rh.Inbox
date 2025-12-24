using FluentAssertions;
using Rh.Inbox.Providers;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Providers;

public class DateTimeProviderTests
{
    [Fact]
    public void GetUtcNow_ReturnsCurrentUtcTime()
    {
        var provider = new DateTimeProvider();
        var before = DateTime.UtcNow;

        var result = provider.GetUtcNow();

        var after = DateTime.UtcNow;
        result.Should().BeOnOrAfter(before);
        result.Should().BeOnOrBefore(after);
    }

    [Fact]
    public void GetUtcNow_ReturnsUtcKind()
    {
        var provider = new DateTimeProvider();

        var result = provider.GetUtcNow();

        result.Kind.Should().Be(DateTimeKind.Utc);
    }

    [Fact]
    public void GetUtcNow_MultipleCalls_ReturnsIncreasingTimes()
    {
        var provider = new DateTimeProvider();

        var first = provider.GetUtcNow();
        Thread.Sleep(1);
        var second = provider.GetUtcNow();

        second.Should().BeOnOrAfter(first);
    }
}
