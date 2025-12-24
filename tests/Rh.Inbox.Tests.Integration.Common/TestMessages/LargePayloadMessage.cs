namespace Rh.Inbox.Tests.Integration.Common.TestMessages;

public record LargePayloadMessage(string Id, byte[] Payload);

public static class PayloadSizes
{
    public const int Small = 1 * 1024;           // 1 KB
    public const int MediumLow = 50 * 1024;      // 50 KB
    public const int MediumHigh = 256 * 1024;    // 256 KB
    public const int Large = 1024 * 1024;        // 1 MB

    public static byte[] Generate(int size)
    {
        var payload = new byte[size];
        Random.Shared.NextBytes(payload);
        return payload;
    }
}
