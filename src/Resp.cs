namespace codecrafters_redis;

public abstract record RespMessage;
public record Ping: RespMessage;

public static class Resp
{
    public static bool TryParse(ReadOnlySpan<byte> buffer, out RespMessage? message, out int consumed)
    {
        message = null;
        consumed = 0;
        
        if (buffer.StartsWith("*1\r\n$4\r\nPING\r\n"u8))
        {
            Console.WriteLine("Got ping");
            message = new Ping();
            consumed = 14;
            return true;
        }

        return false; // not enough bytes or unknown message
    }
}