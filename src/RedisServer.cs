using System.Collections.Concurrent;
using System.Net.Sockets;

namespace codecrafters_redis;

public class RedisServer(int port)
{
    private record DbEntry(string Value, DateTime? Expiry);
    
    private readonly TcpListener _listener = TcpListener.Create(port);
    private readonly ConcurrentDictionary<string, DbEntry> _kvDb = new();
    private readonly ConcurrentDictionary<string, List<string>> _listDb = new();

    public async Task Start()
    {
        _listener.Start();
        // Create the cancellation token source
        var cancellationTokenSource = new CancellationTokenSource();
        // Run the db cleaner
        _ = Task.Run(() => DbCleanUp(cancellationTokenSource.Token));
        
        while (true)
        {
            var client = await _listener.AcceptTcpClientAsync();
            _ = Task.Run(() => HandleClient(client));
        }
    }

    private async Task DbCleanUp(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            // Wait 10 seconds
            await Task.Delay(10000, cancellationToken);
            // Get the expired keys
            var expiredKeys = _kvDb
                .Where(kvp => kvp.Value.Expiry.HasValue && kvp.Value.Expiry.Value < DateTime.UtcNow)
                .Select(kvp => kvp.Key)
                .ToList();

            foreach (var expiredEntry in expiredKeys)
            {
                _kvDb.TryRemove(expiredEntry, out _);
            }
        }
    }

    private async Task HandleClient(TcpClient client)
    {
        await using var stream = client.GetStream();
        await using var writer = new StreamWriter(stream);
        var buffer = new byte[4096];
        var bufferLength = 0;
        
        while (true)
        {
            var message = new byte[1024];
            var bytesRead = await stream.ReadAsync(message);
            Array.Copy(message, 0, buffer, bufferLength, bytesRead);
            bufferLength += bytesRead;
            
            // Process messages
            var offset = 0;
            while (offset < bufferLength && Resp.TryParse(buffer.AsSpan(offset, bufferLength - offset), out var respMsg,
                       out var consumed))
            {
                await HandleRequest(writer, respMsg);
                await writer.FlushAsync();
                offset += consumed;
            }
            
            // Move buffer length 
            if (offset == 0) continue;
            Array.Copy(buffer, offset, buffer, 0, bufferLength - offset);
            bufferLength -= offset;
        }
    }

    private async Task HandleRequest(StreamWriter writer, RespMessage? message)
    {
        switch (message)
        {
            case Ping _:
                await WriteSimpleString(writer, "PONG");
                break;
            case Echo echo:
                await WriteBulkString(writer, echo.Message);
                break;
            case Set set:
                _kvDb[set.Key] = new DbEntry(set.Value, set.Expiry);
                await WriteSimpleString(writer, "OK");
                break;
            case Get get:
                // Check if the key exists. If the expiry exists, check to see that we are within its limits
                if (!_kvDb.TryGetValue(get.Key, out var dbEntry) || dbEntry.Expiry is not null && dbEntry.Expiry < DateTime.UtcNow)
                {
                    await WriteNullBulkString(writer);
                    return;
                }
                await WriteBulkString(writer, dbEntry.Value);
                break;
            case RPush rPush:
                if (!_listDb.ContainsKey(rPush.ListName)) _listDb[rPush.ListName] = [];
                rPush.Elements.ForEach(e => _listDb[rPush.ListName].Add(e));
                await WriteInteger(writer, _listDb[rPush.ListName].Count);
                break;
            case LPush lPush:
                if (!_listDb.ContainsKey(lPush.ListName)) _listDb[lPush.ListName] = [];
                lPush.Elements.ForEach(e => _listDb[lPush.ListName].Insert(0, e));
                await WriteInteger(writer, _listDb[lPush.ListName].Count);
                break;
            case LRange lRange:
                await HandleLRange(writer, lRange);
                break;
            case LLen lLen:
                if (!_listDb.TryGetValue(lLen.ListName, out var list))
                {
                    await WriteInteger(writer, 0);
                    return;
                }
                await WriteInteger(writer, list.Count);
        }
    }

    private async Task HandleLRange(StreamWriter writer, LRange lRange) 
    {
        if (!_listDb.TryGetValue(lRange.ListName, out var list))
        {
            await WriteRespArray(writer, []);
            return;
        }

        var count = list.Count;

        var start = lRange.Start < 0 ? count + lRange.Start : lRange.Start;
        var end = lRange.End < 0 ? count + lRange.End : lRange.End;

        // Clamp start and end
        start = Math.Max(0, start);
        end = Math.Min(count - 1, end);

        // If start is after end, return empty
        if (start > end)
        {
            await WriteRespArray(writer, []);
            return;
        }

        var range = list.GetRange(start, end - start + 1);
        await WriteRespArray(writer, range);
    }
    
    // HELPER METHODS FOR WRITING
    
    private static async Task WriteSimpleString(StreamWriter writer, string value)
    {
        await writer.WriteAsync($"+{value}\r\n");
    }

    private static async Task WriteBulkString(StreamWriter writer, string value)
    {
        await writer.WriteAsync($"${value.Length}\r\n{value}\r\n");
    }

    private static async Task WriteNullBulkString(StreamWriter writer)
    {
        await writer.WriteAsync("$-1\r\n");
    }
    
    private static async Task WriteInteger(StreamWriter writer, int value)
    {
        await writer.WriteAsync($":{value}\r\n");
    }

    private static async Task WriteRespArray(StreamWriter writer, List<string> elements)
    {
        await writer.WriteAsync($"*{elements.Count}\r\n");
        foreach (var element in elements)
        {
            await WriteBulkString(writer, element);
        }
    }
}