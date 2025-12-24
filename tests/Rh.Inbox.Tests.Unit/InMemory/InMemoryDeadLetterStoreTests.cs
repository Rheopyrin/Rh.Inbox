using FluentAssertions;
using Rh.Inbox.Abstractions.Messages;
using Rh.Inbox.InMemory;
using Xunit;

namespace Rh.Inbox.Tests.Unit.InMemory;

public class InMemoryDeadLetterStoreTests
{
    private readonly InMemoryDeadLetterStore _store;

    public InMemoryDeadLetterStoreTests()
    {
        _store = new InMemoryDeadLetterStore();
    }

    #region Add Tests

    [Fact]
    public void Add_SingleMessage_IncreasesCount()
    {
        var message = CreateDeadLetterMessage();

        _store.Add(message);

        _store.Count.Should().Be(1);
    }

    [Fact]
    public void Add_MultipleMessages_IncreasesCount()
    {
        _store.Add(CreateDeadLetterMessage());
        _store.Add(CreateDeadLetterMessage());
        _store.Add(CreateDeadLetterMessage());

        _store.Count.Should().Be(3);
    }

    #endregion

    #region Read Tests

    [Fact]
    public void Read_EmptyStore_ReturnsEmptyList()
    {
        var result = _store.Read(10);

        result.Should().BeEmpty();
    }

    [Fact]
    public void Read_WithMessages_ReturnsMessagesSortedByMovedAt()
    {
        var msg1 = CreateDeadLetterMessage(movedAt: DateTime.UtcNow.AddMinutes(-10));
        var msg2 = CreateDeadLetterMessage(movedAt: DateTime.UtcNow.AddMinutes(-5));
        var msg3 = CreateDeadLetterMessage(movedAt: DateTime.UtcNow);

        // Add in non-sorted order
        _store.Add(msg2);
        _store.Add(msg3);
        _store.Add(msg1);

        var result = _store.Read(10);

        result.Should().HaveCount(3);
        result[0].Id.Should().Be(msg1.Id);
        result[1].Id.Should().Be(msg2.Id);
        result[2].Id.Should().Be(msg3.Id);
    }

    [Fact]
    public void Read_WithLimitLessThanCount_ReturnsLimitedMessages()
    {
        for (var i = 0; i < 5; i++)
        {
            _store.Add(CreateDeadLetterMessage());
        }

        var result = _store.Read(3);

        result.Should().HaveCount(3);
    }

    #endregion

    #region CleanupExpiredAsync Tests

    [Fact]
    public async Task CleanupExpiredAsync_NoExpiredMessages_ReturnsZero()
    {
        var now = DateTime.UtcNow;
        _store.Add(CreateDeadLetterMessage(movedAt: now));
        _store.Add(CreateDeadLetterMessage(movedAt: now.AddMinutes(-1)));

        var deleted = await _store.CleanupExpiredAsync(now.AddMinutes(-5), CancellationToken.None);

        deleted.Should().Be(0);
        _store.Count.Should().Be(2);
    }

    [Fact]
    public async Task CleanupExpiredAsync_WithExpiredMessages_RemovesExpiredOnly()
    {
        var now = DateTime.UtcNow;
        var expiredMsg1 = CreateDeadLetterMessage(movedAt: now.AddDays(-10));
        var expiredMsg2 = CreateDeadLetterMessage(movedAt: now.AddDays(-8));
        var validMsg = CreateDeadLetterMessage(movedAt: now.AddDays(-1));

        _store.Add(expiredMsg1);
        _store.Add(expiredMsg2);
        _store.Add(validMsg);

        var expirationTime = now.AddDays(-7);
        var deleted = await _store.CleanupExpiredAsync(expirationTime, CancellationToken.None);

        deleted.Should().Be(2);
        _store.Count.Should().Be(1);
    }

    [Fact]
    public async Task CleanupExpiredAsync_AllExpired_RemovesAll()
    {
        var now = DateTime.UtcNow;
        _store.Add(CreateDeadLetterMessage(movedAt: now.AddDays(-10)));
        _store.Add(CreateDeadLetterMessage(movedAt: now.AddDays(-8)));
        _store.Add(CreateDeadLetterMessage(movedAt: now.AddDays(-5)));

        var deleted = await _store.CleanupExpiredAsync(now, CancellationToken.None);

        deleted.Should().Be(3);
        _store.Count.Should().Be(0);
    }

    [Fact]
    public async Task CleanupExpiredAsync_EmptyStore_ReturnsZero()
    {
        var deleted = await _store.CleanupExpiredAsync(DateTime.UtcNow, CancellationToken.None);

        deleted.Should().Be(0);
    }

    [Fact]
    public async Task CleanupExpiredAsync_ExactExpirationTime_RemovesMessage()
    {
        var expirationTime = DateTime.UtcNow.AddDays(-7);
        _store.Add(CreateDeadLetterMessage(movedAt: expirationTime));

        var deleted = await _store.CleanupExpiredAsync(expirationTime, CancellationToken.None);

        deleted.Should().Be(1);
        _store.Count.Should().Be(0);
    }

    #endregion

    #region Count Tests

    [Fact]
    public void Count_EmptyStore_ReturnsZero()
    {
        _store.Count.Should().Be(0);
    }

    #endregion

    #region Helper Methods

    private static DeadLetterMessage CreateDeadLetterMessage(DateTime? movedAt = null)
    {
        return new DeadLetterMessage
        {
            Id = Guid.NewGuid(),
            InboxName = "test-inbox",
            MessageType = "test.message",
            Payload = "test payload",
            GroupId = null,
            CollapseKey = null,
            AttemptsCount = 3,
            ReceivedAt = DateTime.UtcNow.AddMinutes(-30),
            FailureReason = "Test failure",
            MovedAt = movedAt ?? DateTime.UtcNow
        };
    }

    #endregion
}
