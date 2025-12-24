using FluentAssertions;
using Rh.Inbox.Redis.Utility;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Redis;

public class RedisKeyHelperTests
{
    #region IsValidKeyPrefix Tests

    [Theory]
    [InlineData("valid_name")]
    [InlineData("valid-name")]
    [InlineData("valid:name")]
    [InlineData("ValidName")]
    [InlineData("abc123")]
    [InlineData("inbox:orders")]
    [InlineData("my-app:inbox:orders")]
    public void IsValidKeyPrefix_ValidPrefix_ReturnsTrue(string keyPrefix)
    {
        RedisKeyHelper.IsValidKeyPrefix(keyPrefix).Should().BeTrue();
    }

    [Fact]
    public void IsValidKeyPrefix_TooLong_ReturnsFalse()
    {
        var longKeyPrefix = new string('a', 101);
        RedisKeyHelper.IsValidKeyPrefix(longKeyPrefix).Should().BeFalse();
    }

    [Fact]
    public void IsValidKeyPrefix_ExactMaxLength_ReturnsTrue()
    {
        var maxLengthKeyPrefix = new string('a', 100);
        RedisKeyHelper.IsValidKeyPrefix(maxLengthKeyPrefix).Should().BeTrue();
    }

    [Theory]
    [InlineData("has space")]
    [InlineData("has.dot")]
    [InlineData("has@symbol")]
    [InlineData("has$dollar")]
    [InlineData("has*star")]
    [InlineData("has[bracket")]
    public void IsValidKeyPrefix_InvalidChars_ReturnsFalse(string keyPrefix)
    {
        RedisKeyHelper.IsValidKeyPrefix(keyPrefix).Should().BeFalse();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void IsValidKeyPrefix_EmptyOrNull_ReturnsFalse(string? keyPrefix)
    {
        RedisKeyHelper.IsValidKeyPrefix(keyPrefix!).Should().BeFalse();
    }

    #endregion

    #region BuildKeyPrefix Tests

    [Fact]
    public void BuildKeyPrefix_ValidInput_ReturnsFormattedKey()
    {
        var result = RedisKeyHelper.BuildKeyPrefix("inbox", "orders");

        result.Should().Be("inbox:orders");
    }

    [Fact]
    public void BuildKeyPrefix_WithUpperCase_ConvertedToLowerCase()
    {
        var result = RedisKeyHelper.BuildKeyPrefix("INBOX", "ORDERS");

        // Default prefix is used as-is, inbox name is sanitized to lowercase
        result.Should().Be("INBOX:orders");
    }

    [Fact]
    public void BuildKeyPrefix_LongName_Truncates()
    {
        var longInboxName = new string('a', 150);
        var result = RedisKeyHelper.BuildKeyPrefix("prefix", longInboxName);

        result.Length.Should().BeLessOrEqualTo(100);
    }

    [Fact]
    public void BuildKeyPrefix_ExactMaxLength_NotTruncated()
    {
        // prefix: = 7 chars, so inbox name can be 93 chars for total of 100
        var inboxName = new string('a', 93);
        var result = RedisKeyHelper.BuildKeyPrefix("prefix", inboxName);

        result.Length.Should().Be(100);
        result.Should().Be($"prefix:{inboxName}");
    }

    [Fact]
    public void BuildKeyPrefix_WithInvalidChars_SanitizesName()
    {
        var result = RedisKeyHelper.BuildKeyPrefix("inbox", "my.order.service");

        result.Should().Be("inbox:my_order_service");
    }

    #endregion

    #region SanitizeKeyPart Tests

    [Theory]
    [InlineData("has.dot", "has_dot")]
    [InlineData("has space", "has_space")]
    [InlineData("has@symbol", "has_symbol")]
    [InlineData("multiple..dots", "multiple__dots")]
    public void SanitizeKeyPart_InvalidChars_ReplacesWithUnderscore(string input, string expected)
    {
        var result = RedisKeyHelper.SanitizeKeyPart(input);

        result.Should().Be(expected);
    }

    [Theory]
    [InlineData("1starts", "_1starts")]
    [InlineData("9number", "_9number")]
    [InlineData("123", "_123")]
    public void SanitizeKeyPart_StartsWithDigit_PrependsUnderscore(string input, string expected)
    {
        var result = RedisKeyHelper.SanitizeKeyPart(input);

        result.Should().Be(expected);
    }

    [Fact]
    public void SanitizeKeyPart_UpperCase_ConvertsToLowerCase()
    {
        var result = RedisKeyHelper.SanitizeKeyPart("MyKeyPart");

        result.Should().Be("mykeypart");
    }

    [Fact]
    public void SanitizeKeyPart_ValidKeyPart_ReturnsLowerCase()
    {
        var result = RedisKeyHelper.SanitizeKeyPart("valid_name");

        result.Should().Be("valid_name");
    }

    [Fact]
    public void SanitizeKeyPart_HyphenAllowed_ReturnsWithHyphen()
    {
        var result = RedisKeyHelper.SanitizeKeyPart("valid-name");

        result.Should().Be("valid-name");
    }

    [Fact]
    public void SanitizeKeyPart_EmptyString_ReturnsEmpty()
    {
        var result = RedisKeyHelper.SanitizeKeyPart("");

        result.Should().BeEmpty();
    }

    #endregion

    #region MaxKeyPrefixLength Tests

    [Fact]
    public void MaxKeyPrefixLength_Is100()
    {
        RedisKeyHelper.MaxKeyPrefixLength.Should().Be(100);
    }

    #endregion
}
