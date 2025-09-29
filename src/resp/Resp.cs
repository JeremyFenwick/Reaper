using System.Text;

namespace codecrafters_redis.resp;

public static class Resp
{
    public static bool TryParse(ReadOnlySpan<byte> buffer, out RespMessage? message, out int consumed)
    {
        message = null;
        consumed = 0;
        var offset = 0;

        // Check we have enough data and correct start
        if (buffer.Length < 1 || buffer[offset] != '*') return false;
        offset++;

        // Parse array length
        if (!TryParseInteger(buffer, ref offset, out var arrayLength)) return false;
        if (!TrySkipCrlf(buffer, ref offset)) return false;

        var items = new List<string>(arrayLength);
        for (var i = 0; i < arrayLength; i++)
        {
            if (!TryParseBulkString(buffer, ref offset, out var item)) return false;
            items.Add(item!);
        }

        message = CreateMessage(items);
        consumed = offset;
        return true;
    }

    private static bool TryParseBulkString(ReadOnlySpan<byte> buffer, ref int offset, out string? item)
    {
        item = null;
        var tempOffset = offset;

        // Check we have enough data and correct start
        if (tempOffset >= buffer.Length || buffer[tempOffset] != '$') return false;
        tempOffset++;

        // Parse length
        if (!TryParseInteger(buffer, ref tempOffset, out var length)) return false;
        if (!TrySkipCrlf(buffer, ref tempOffset)) return false;

        // Check for null
        if (length == -1)
        {
            item = null;
            offset = tempOffset;
            return true;
        }

        // If the length is negative otherwise it is invalid
        if (length < 0) return false;

        // Parse string
        if (tempOffset + length > buffer.Length) return false; // Check we have enough bytes
        item = Encoding.UTF8.GetString(buffer.Slice(tempOffset, length));
        tempOffset += length;

        // Skip the final crlf
        if (!TrySkipCrlf(buffer, ref tempOffset)) return false;
        offset = tempOffset;
        return true;
    }

    private static bool TrySkipCrlf(ReadOnlySpan<byte> buffer, ref int offset)
    {
        if (offset + 1 >= buffer.Length || buffer[offset] != '\r' || buffer[offset + 1] != '\n') return false;
        offset += 2;
        return true;
    }

    private static bool TryParseInteger(ReadOnlySpan<byte> buffer, ref int offset, out int value)
    {
        value = 0;
        var tempOffset = offset;
        var hasDigits = false;
        var isNegative = false;

        // Handle negatives
        if (buffer[tempOffset] == '-')
        {
            isNegative = true;
            tempOffset++;
        }

        // Get the value of the integer
        while (tempOffset < buffer.Length && buffer[tempOffset] != '\r')
        {
            if (buffer[tempOffset] < '0' || buffer[tempOffset] > '9') return false;
            value = value * 10 + (buffer[tempOffset] - '0');
            hasDigits = true;
            tempOffset++;
        }

        // Edge cases 
        if (tempOffset >= buffer.Length) return false; // No newline
        if (!hasDigits) return false; // No digits (e.g. "-\r\n")

        // Return the value
        offset = tempOffset;
        if (isNegative) value = -value;
        return true;
    }

    private static RespMessage CreateMessage(List<string> items)
    {
        var command = items[0].ToUpperInvariant(); // normalize the command

        return command switch // We are case insensitive
        {
            "PING" => new Ping(),
            "ECHO" => new Echo(items[1]),
            "SET" => SetMessage(items),
            "GET" => new Get(items[1]),
            "RPUSH" => new RPush(items[1], items[2..]),
            "LPUSH" => new LPush(items[1], items[2..]),
            "LRANGE" => new LRange(items[1], int.Parse(items[2]), int.Parse(items[3])),
            "LLEN" => new LLen(items[1]),
            "LPOP" => new LPop(items[1], items.Count > 2 ? int.Parse(items[2]) : 1),
            "BLPOP" => new BlPop(items[1], float.Parse(items[2])),
            "TYPE" => new Type(items[1]),
            "XADD" => GenerateXAdd(items),
            _ => new Unknown()
        };
    }

    private static RespMessage SetMessage(List<string> items)
    {
        if (items.Count < 5) return new Set(items[1], items[2], null);

        var modifier = items[3].ToUpperInvariant();
        return modifier switch
        {
            "EX" => new Set(items[1], items[2], DateTime.UtcNow.AddSeconds(int.Parse(items[4]))),
            "PX" => new Set(items[1], items[2], DateTime.UtcNow.AddMilliseconds(int.Parse(items[4]))),
            _ => new Set(items[1], items[2], null)
        };
    }

    private static RespMessage GenerateXAdd(List<string> items)
    {
        if (items.Count < 4)
            throw new ArgumentException("XADD requires at least a key, an ID, and one field/value pair.");

        var key = items[1];
        var id = items[2].Split("-");

        // Use a list so duplicate fields are preserved (Redis allows this)
        var fields = new List<KeyValuePair<string, string>>();

        for (var i = 3; i < items.Count; i += 2)
        {
            if (i + 1 >= items.Count)
                throw new ArgumentException("XADD fields must be specified as field/value pairs.");

            fields.Add(new KeyValuePair<string, string>(items[i], items[i + 1]));
        }

        // Get the timestamp
        long? ts = id[0] == "*" ? null : long.Parse(id[0]);
        // Get the sequence
        int? seq = id.Length == 0 || id[1] == "*" ? null : int.Parse(id[1]);

        return new XAdd(key, ts, seq, fields);
    }

    // HELPER METHODS FOR WRITING RESPONSES

    public static async Task WriteSimpleStringAsync(StreamWriter writer, string value)
    {
        await writer.WriteAsync($"+{value}\r\n");
    }

    public static async Task WriteBulkStringAsync(StreamWriter writer, string value)
    {
        await writer.WriteAsync($"${value.Length}\r\n{value}\r\n");
    }

    public static async Task WriteNullBulkStringAsync(StreamWriter writer)
    {
        await writer.WriteAsync("$-1\r\n");
    }

    public static async Task WriteIntegerAsync(StreamWriter writer, int value)
    {
        await writer.WriteAsync($":{value}\r\n");
    }

    public static async Task WriteRespArrayAsync(StreamWriter writer, List<string> elements)
    {
        await writer.WriteAsync($"*{elements.Count}\r\n");
        foreach (var element in elements) await WriteBulkStringAsync(writer, element);
    }

    public static async Task WriteNullArrayAsync(StreamWriter writer)
    {
        await writer.WriteAsync($"*-1\r\n");
    }

    public static async Task WriteSimpleErrorAsync(StreamWriter writer, string message)
    {
        await writer.WriteAsync($"-ERR {message}\r\n");
    }
}