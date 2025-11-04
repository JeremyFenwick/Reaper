using System.Text;

namespace codecrafters_redis.resp;

public abstract record Response()
{
    public abstract byte[] ToBytes();
    protected static readonly byte[] NullBulkString = "$-1\r\n"u8.ToArray();
    protected static readonly byte[] NullArray = "*-1\r\n"u8.ToArray();

    protected static byte[] BulkString(string msg)
    {
        return Encoding.UTF8.GetBytes($"${msg.Length}\r\n{msg}\r\n");
    }
}

public record Pong() : Response()
{
    public string Msg = "PONG";

    public override byte[] ToBytes()
    {
        return "+PONG\r\n"u8.ToArray();
    }
}

public record Ok() : Response()
{
    public string Msg = "OK";

    public override byte[] ToBytes()
    {
        return "+OK\r\n"u8.ToArray();
    }
}

public record BulkString(string Msg) : Response()
{
    public override byte[] ToBytes()
    {
        return BulkString(Msg);
    }
}

public record NullBulkString() : Response()
{
    public override byte[] ToBytes()
    {
        return NullBulkString;
    }
}

public record Integer(int Number) : Response()
{
    public override byte[] ToBytes()
    {
        return Encoding.UTF8.GetBytes($":{Number}\r\n");
    }
}

public record Array(List<string> List) : Response()
{
    public override byte[] ToBytes()
    {
        var data = new List<byte>();
        data.AddRange(Encoding.UTF8.GetBytes($"*{List.Count}\r\n"));
        foreach (var item in List) data.AddRange(BulkString(item));
        return data.ToArray();
    }
}

public record NullArray() : Response()
{
    public override byte[] ToBytes()
    {
        return NullArray;
    }
}