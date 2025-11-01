namespace codecrafters_redis.resp;

public abstract record Response()
{
    public abstract byte[] ToBytes();
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
        return RespEncoder.BulkString(Msg);
    }
}

public record NullBulkString() : Response()
{
    public override byte[] ToBytes()
    {
        return "$-1\r\n"u8.ToArray();
    }
}