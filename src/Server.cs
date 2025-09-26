using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Runtime.InteropServices;

namespace codecrafters_redis;

public class Server(int port)
{
    private record DbEntry(string Value, DateTime? Expiry);
    
    private readonly TcpListener _listener = TcpListener.Create(port);
    private readonly ConcurrentDictionary<string, DbEntry> _db = new();

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
            var expiredKeys = _db
                .Where(kvp => kvp.Value.Expiry.HasValue && kvp.Value.Expiry.Value < DateTime.UtcNow)
                .Select(kvp => kvp.Key)
                .ToList();

            foreach (var expiredEntry in expiredKeys)
            {
                _db.TryRemove(expiredEntry, out _);
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
                _db[set.Key] = new DbEntry(set.Value, set.Expiry);
                await writer.WriteAsync("+OK\r\n");
                break;
            case Get get:
                if (!_db.TryGetValue(get.Key, out var dbEntry) || dbEntry.Expiry is not null && dbEntry.Expiry < DateTime.UtcNow)
                {
                    await WriteNullBulkString(writer);
                    return;
                }

                await WriteBulkString(writer, dbEntry.Value);
                break;
        }
    }
    
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
}