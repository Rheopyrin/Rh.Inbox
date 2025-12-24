using FluentAssertions;
using Rh.Inbox.Exceptions;
using Xunit;

namespace Rh.Inbox.Tests.Unit.Exceptions;

public class ExceptionTests
{
    #region InboxBaseException Tests

    [Fact]
    public void InboxBaseException_WithMessage_SetsMessage()
    {
        var exception = new InboxBaseException("Test error message");

        exception.Message.Should().Be("Test error message");
        exception.InnerException.Should().BeNull();
    }

    [Fact]
    public void InboxBaseException_WithMessageAndInner_SetsBoth()
    {
        var inner = new InvalidOperationException("Inner error");
        var exception = new InboxBaseException("Test error", inner);

        exception.Message.Should().Be("Test error");
        exception.InnerException.Should().BeSameAs(inner);
    }

    #endregion

    #region InboxNotFoundException Tests

    [Fact]
    public void InboxNotFoundException_SetsInboxName()
    {
        var exception = new InboxNotFoundException("my-inbox");

        exception.InboxName.Should().Be("my-inbox");
        exception.Message.Should().Contain("my-inbox");
        exception.Message.Should().Contain("not found");
    }

    [Fact]
    public void InboxNotFoundException_MessageFormat_IsCorrect()
    {
        var exception = new InboxNotFoundException("test-inbox");

        exception.Message.Should().Be("Rh.Inbox 'test-inbox' not found.");
    }

    #endregion

    #region InboxNotStartedException Tests

    [Fact]
    public void InboxNotStartedException_WithMessage_SetsMessage()
    {
        var exception = new InboxNotStartedException("Inbox not started");

        exception.Message.Should().Be("Inbox not started");
        exception.InnerException.Should().BeNull();
    }

    [Fact]
    public void InboxNotStartedException_WithMessageAndInner_SetsBoth()
    {
        var inner = new InvalidOperationException("Cause");
        var exception = new InboxNotStartedException("Inbox not started", inner);

        exception.Message.Should().Be("Inbox not started");
        exception.InnerException.Should().BeSameAs(inner);
    }

    [Fact]
    public void InboxNotStartedException_IsInboxBaseException()
    {
        var exception = new InboxNotStartedException("Test");

        exception.Should().BeAssignableTo<InboxBaseException>();
    }

    #endregion

    #region InboxException Tests

    [Fact]
    public void InboxException_SetsInboxNameAndMessage()
    {
        var exception = new InboxException("my-inbox", "Something went wrong");

        exception.InboxName.Should().Be("my-inbox");
        exception.Message.Should().Be("Something went wrong");
    }

    [Fact]
    public void InboxException_WithInner_SetsInnerException()
    {
        var inner = new InvalidOperationException("Cause");
        var exception = new InboxException("inbox", "Error", inner);

        exception.InnerException.Should().BeSameAs(inner);
    }

    [Fact]
    public void InboxException_IsInboxBaseException()
    {
        var exception = new InboxException("inbox", "Error");

        exception.Should().BeAssignableTo<InboxBaseException>();
    }

    #endregion

    #region InvalidInboxMessageException Tests

    [Fact]
    public void InvalidInboxMessageException_SetsInboxNameAndMessage()
    {
        var exception = new InvalidInboxMessageException("my-inbox", "Invalid message format");

        exception.InboxName.Should().Be("my-inbox");
        exception.Message.Should().Be("Invalid message format");
    }

    [Fact]
    public void InvalidInboxMessageException_WithInner_SetsInnerException()
    {
        var inner = new FormatException("Bad format");
        var exception = new InvalidInboxMessageException("inbox", "Error", inner);

        exception.InnerException.Should().BeSameAs(inner);
    }

    [Fact]
    public void InvalidInboxMessageException_IsInboxException()
    {
        var exception = new InvalidInboxMessageException("inbox", "Error");

        exception.Should().BeAssignableTo<InboxException>();
    }

    #endregion

    #region InvalidInboxConfigurationException Tests

    [Fact]
    public void InvalidInboxConfigurationException_WithMessage_SetsMessage()
    {
        var exception = new InvalidInboxConfigurationException("Invalid config");

        exception.Message.Should().Be("Invalid config");
        exception.Errors.Should().BeEmpty();
    }

    [Fact]
    public void InvalidInboxConfigurationException_WithMessageAndInner_SetsBoth()
    {
        var inner = new ArgumentException("Bad arg");
        var exception = new InvalidInboxConfigurationException("Invalid config", inner);

        exception.Message.Should().Be("Invalid config");
        exception.InnerException.Should().BeSameAs(inner);
    }

    [Fact]
    public void InvalidInboxConfigurationException_WithErrors_SetsErrors()
    {
        var errors = new[]
        {
            new InboxOptionError("BatchSize", "Must be positive"),
            new InboxOptionError("Timeout", "Must be greater than zero")
        };

        var exception = new InvalidInboxConfigurationException("Validation failed", errors);

        exception.Errors.Should().HaveCount(2);
        exception.Errors[0].OptionName.Should().Be("BatchSize");
        exception.Errors[0].ErrorMessage.Should().Be("Must be positive");
        exception.Errors[1].OptionName.Should().Be("Timeout");
        exception.Errors[1].ErrorMessage.Should().Be("Must be greater than zero");
    }

    [Fact]
    public void InvalidInboxConfigurationException_IsInboxBaseException()
    {
        var exception = new InvalidInboxConfigurationException("Error");

        exception.Should().BeAssignableTo<InboxBaseException>();
    }

    #endregion

    #region InboxOptionError Tests

    [Fact]
    public void InboxOptionError_RecordEquality_Works()
    {
        var error1 = new InboxOptionError("Option", "Error");
        var error2 = new InboxOptionError("Option", "Error");

        error1.Should().Be(error2);
    }

    [Fact]
    public void InboxOptionError_RecordInequality_Works()
    {
        var error1 = new InboxOptionError("Option1", "Error");
        var error2 = new InboxOptionError("Option2", "Error");

        error1.Should().NotBe(error2);
    }

    #endregion
}
