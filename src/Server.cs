using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Runtime.InteropServices;

namespace codecrafters_redis;

public class Server(int port)
{
    private readonly TcpListener _listener = TcpListener.Create(port);
    private readonly ConcurrentDictionary<string, string> _db = new();

    public async Task Start()
    {
        _listener.Start();
        while (true)
        {
            var client = await _listener.AcceptTcpClientAsync();
            _ = Task.Run(() => HandleClient(client));
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
                await writer.WriteAsync("+PONG\r\n");
                break;
            case Echo echo:
                await writer.WriteAsync($"${echo.Message.Length}\r\n{echo.Message}\r\n");
                break;
            case Set set:
                _db[set.Key] = set.Value;
                await writer.WriteAsync("+OK\r\n");
                break;
            case Get get:
                if (_db.TryGetValue(get.Key, out var value))
                {
                    await writer.WriteAsync($"${value.Length}\r\n{value}\r\n");
                }
                else
                {
                    await writer.WriteAsync("$-1\r\n");
                }
                break;
        }
    }
}