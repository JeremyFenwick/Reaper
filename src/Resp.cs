using System.Text;

namespace codecrafters_redis;

public abstract record RespMessage;
public record Ping: RespMessage;
public record Echo(string Message): RespMessage;

public record Set(string Key, string Value, DateTime? Expiry) : RespMessage;

public record Get(string Key): RespMessage;
public record Unknown : RespMessage;

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
        if (!TryParseInteger(buffer, ref offset, out int arrayLength)) return false;
        if (!TrySkipCrlf(buffer, ref offset)) return false;
    
        var items = new List<string>(arrayLength);
        for (int i = 0; i < arrayLength; i++)
        {
            if (!TryParseBulkString(buffer, ref offset, out string? item)) return false;
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
        if (!TryParseInteger(buffer, ref tempOffset, out int length)) return false;
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
        var command = items[0].ToUpperInvariant(); // just normalize the command
        
        return command switch // We are case insensitive
        {
            "PING" => new Ping(),
            "ECHO" => new Echo(items[1]),
            "SET" => SetMessage(items),
            "GET" => new Get(items[1]),
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
            _    => new Set(items[1], items[2], null)
        };

    }
}