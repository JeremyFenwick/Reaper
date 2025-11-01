namespace codecrafters_redis.resp;

public abstract record Response()
{
    public abstract byte[] ToBytes();
}

public record PongResponse() : Response()
{
    public string Msg = "PONG";
    
    public override byte[] ToBytes()
    {
        return "+PONG\r\n"u8.ToArray();
    }
}

public record EchoResponse(string Msg) : Response()
{
    public override byte[] ToBytes()
    {
        return RespEncoder.BulkString(Msg);
    }
}