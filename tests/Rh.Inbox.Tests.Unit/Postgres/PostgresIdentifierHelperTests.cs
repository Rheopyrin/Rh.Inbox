using FluentAssertions;
using Rh.Inbox.Postgres.Utility;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Postgres;

public class PostgresIdentifierHelperTests
{
    #region IsValidIdentifier Tests

    [Theory]
    [InlineData("valid_name")]
    [InlineData("ValidName")]
    [InlineData("_underscore_start")]
    [InlineData("a")]
    [InlineData("abc123")]
    [InlineData("test_table_name")]
    public void IsValidIdentifier_ValidName_ReturnsTrue(string identifier)
    {
        PostgresIdentifierHelper.IsValidIdentifier(identifier).Should().BeTrue();
    }

    [Theory]
    [InlineData("1starts_with_digit")]
    [InlineData("9number")]
    public void IsValidIdentifier_StartsWithDigit_ReturnsFalse(string identifier)
    {
        PostgresIdentifierHelper.IsValidIdentifier(identifier).Should().BeFalse();
    }

    [Fact]
    public void IsValidIdentifier_TooLong_ReturnsFalse()
    {
        var longIdentifier = new string('a', 64);
        PostgresIdentifierHelper.IsValidIdentifier(longIdentifier).Should().BeFalse();
    }

    [Fact]
    public void IsValidIdentifier_ExactMaxLength_ReturnsTrue()
    {
        var maxLengthIdentifier = new string('a', 63);
        PostgresIdentifierHelper.IsValidIdentifier(maxLengthIdentifier).Should().BeTrue();
    }

    [Theory]
    [InlineData("has-dash")]
    [InlineData("has space")]
    [InlineData("has.dot")]
    [InlineData("has@symbol")]
    [InlineData("has$dollar")]
    public void IsValidIdentifier_InvalidChars_ReturnsFalse(string identifier)
    {
        PostgresIdentifierHelper.IsValidIdentifier(identifier).Should().BeFalse();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void IsValidIdentifier_EmptyOrNull_ReturnsFalse(string? identifier)
    {
        PostgresIdentifierHelper.IsValidIdentifier(identifier!).Should().BeFalse();
    }

    #endregion

    #region BuildTableName Tests

    [Fact]
    public void BuildTableName_ValidInput_ReturnsFormattedName()
    {
        var result = PostgresIdentifierHelper.BuildTableName("inbox", "orders");

        result.Should().Be("inbox_orders");
    }

    [Fact]
    public void BuildTableName_WithUpperCase_ConvertedToLowerCase()
    {
        var result = PostgresIdentifierHelper.BuildTableName("INBOX", "ORDERS");

        // Prefix is used as-is, inbox name is sanitized to lowercase
        result.Should().Be("INBOX_orders");
    }

    [Fact]
    public void BuildTableName_LongName_Truncates()
    {
        var longInboxName = new string('a', 100);
        var result = PostgresIdentifierHelper.BuildTableName("prefix", longInboxName);

        result.Length.Should().BeLessOrEqualTo(63);
    }

    [Fact]
    public void BuildTableName_ExactMaxLength_NotTruncated()
    {
        // prefix_ = 7 chars, so inbox name can be 56 chars for total of 63
        var inboxName = new string('a', 56);
        var result = PostgresIdentifierHelper.BuildTableName("prefix", inboxName);

        result.Length.Should().Be(63);
        result.Should().Be($"prefix_{inboxName}");
    }

    [Fact]
    public void BuildTableName_WithInvalidChars_SanitizesName()
    {
        var result = PostgresIdentifierHelper.BuildTableName("inbox", "my-order.service");

        result.Should().Be("inbox_my_order_service");
    }

    #endregion

    #region SanitizeIdentifier Tests

    [Theory]
    [InlineData("has-dash", "has_dash")]
    [InlineData("has.dot", "has_dot")]
    [InlineData("has space", "has_space")]
    [InlineData("has@symbol", "has_symbol")]
    [InlineData("multiple--dashes", "multiple__dashes")]
    public void SanitizeIdentifier_InvalidChars_ReplacesWithUnderscore(string input, string expected)
    {
        var result = PostgresIdentifierHelper.SanitizeIdentifier(input);

        result.Should().Be(expected);
    }

    [Theory]
    [InlineData("1starts", "_1starts")]
    [InlineData("9number", "_9number")]
    [InlineData("123", "_123")]
    public void SanitizeIdentifier_StartsWithDigit_PrependsUnderscore(string input, string expected)
    {
        var result = PostgresIdentifierHelper.SanitizeIdentifier(input);

        result.Should().Be(expected);
    }

    [Fact]
    public void SanitizeIdentifier_UpperCase_ConvertsToLowerCase()
    {
        var result = PostgresIdentifierHelper.SanitizeIdentifier("MyTableName");

        result.Should().Be("mytablename");
    }

    [Fact]
    public void SanitizeIdentifier_ValidIdentifier_ReturnsUnchanged()
    {
        var result = PostgresIdentifierHelper.SanitizeIdentifier("valid_name");

        result.Should().Be("valid_name");
    }

    [Fact]
    public void SanitizeIdentifier_EmptyString_ReturnsEmpty()
    {
        var result = PostgresIdentifierHelper.SanitizeIdentifier("");

        result.Should().BeEmpty();
    }

    #endregion

    #region MaxPostgresIdentifierLength Tests

    [Fact]
    public void MaxPostgresIdentifierLength_Is63()
    {
        PostgresIdentifierHelper.MaxPostgresIdentifierLength.Should().Be(63);
    }

    #endregion
}
