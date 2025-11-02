using System.Text;
using Microsoft.Extensions.Logging;

namespace codecrafters_redis.resp;

public static class Parser
{
    public static bool TryParse(ReadOnlySpan<byte> data, out int consumed, out Request request)
    {
        consumed = 0;
        request = new Void();
        if (data.Length < 1 || data[0] != '*') return false;
        // Get the length of the redis array
        consumed++;
        var result = new List<string>();
        if (!TryParseInt(data[consumed..], ref consumed, out var arrayLength)) return false;
        // If the array is empty we have a void request, else we extract the contents
        if (arrayLength == 0) return true;
        for (var i = 0; i < arrayLength; i++)
        {
            // Consume the $ before the integer
            var span = data[consumed..];
            if (span.Length == 0 || span[0] != (byte)'$')
                throw new FormatException("Expected bulk string type");
            consumed++; 
            // Get the data itself
            if (!TryParseInt(data[consumed..], ref consumed, out var length)) return false;
            if (!TryParseString(data[consumed..], length, ref consumed, out var str)) return false;
            result.Add(str);
        }
        // Return the result
        request = BuildRequest(result);
        return true;
    }

    private static bool TryParseString(ReadOnlySpan<byte> data, int length, ref int consumed, out string value)
    {
        value = "";
        
        if (data.Length < length + 2) return false;
        if (data[length] != '\r' || data[length + 1] != '\n') throw new FormatException("Invalid string format");
        value = Encoding.UTF8.GetString(data[..length]);
        
        consumed += length + 2;
        return true;
    }

    private static bool TryParseInt(ReadOnlySpan<byte> data, ref int consumed, out int value)
    {
        // find line ending
        var idx = data.IndexOf((byte)'\r');
        if (idx < 0 || idx + 1 >= data.Length) {
            value = 0;
            return false;   // incomplete
        }
        if (data[idx + 1] != (byte)'\n')
            throw new FormatException("Expected \\n after \\r in RESP integer.");

        // parse integer
        if (!System.Buffers.Text.Utf8Parser.TryParse(data[..idx], out value, out _))
            throw new FormatException("Invalid RESP integer.");

        consumed += idx + 2;
        return true;
    }
    
    // REQUEST BUILDER

    private static Request BuildRequest(List<string> requestData)
    {
        return requestData[0].ToLower() switch
        {
            "ping" => new Ping(),
            "echo" => BuildEcho(requestData),
            "set" => BuildSet(requestData),
            "get" => BuildGet(requestData),
            "rpush" => BuildRPush(requestData),
            _ => throw new FormatException("Invalid RESP format")
        };
    }

    private static Request BuildRPush(List<string> requestData)
    {
        if (requestData.Count != 3) throw new FormatException("Invalid RESP format");
        return new RPush(requestData[1], requestData[2]);
    }

    private static Get BuildGet(List<string> requestData)
    {
        if (requestData.Count != 2) throw new FormatException("Invalid RESP format");
        return new Get(requestData[1]);
    }

    private static Set BuildSet(List<string> requestData)
    {
        return requestData.Count switch
        {
            < 3 => throw new FormatException("Invalid RESP format"),
            // Check for EX setting 
            > 3 when requestData[3].Equals("ex", StringComparison.CurrentCultureIgnoreCase) => 
                new Set(requestData[1], requestData[2], int.Parse(requestData[4]) * 1000),
            > 3 when requestData[3].Equals("px", StringComparison.CurrentCultureIgnoreCase) => 
                new Set(requestData[1], requestData[2], int.Parse(requestData[4])),
            _ => new Set(requestData[1], requestData[2])
        };
    }

    private static EchoRequest BuildEcho(List<string> requestData)
    {
        if (requestData.Count < 2)
            throw new FormatException("Invalid echo request. The request must contain at least two values");
        return new EchoRequest(requestData[1]);
    }
}