using System.Text;

namespace codecrafters_redis.resp;

public static class RespEncoder
{
    public static byte[] BulkString(string msg)
    {
        return Encoding.UTF8.GetBytes($"${msg.Length}\r\n{msg}\r\n");
    }
}