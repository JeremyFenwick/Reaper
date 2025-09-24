using System.Net.Sockets;
using System.Runtime.InteropServices;

namespace codecrafters_redis;

public class Server(int port)
{
    private readonly TcpListener _listener = TcpListener.Create(port);

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
            // Console.WriteLine($"Read {bytesRead} bytes:");
            // for (int i = 0; i < bytesRead; i++)
            // {
            //     Console.WriteLine($"Byte {i}: {message[i]} ('{(char)message[i]}')");
            // }      
            // Copy the message data in
            Array.Copy(message, 0, buffer, bufferLength, bytesRead);
            bufferLength += bytesRead;
            
            // Process messages
            var offset = 0;
            while (offset < bufferLength && Resp.TryParse(buffer.AsSpan(offset, bufferLength - offset), out var respMsg,
                       out var consumed))
            {
                await writer.WriteAsync("+PONG\r\n");
                await writer.FlushAsync();
                offset += consumed;
            }
            
            // Move buffer length 
            if (offset > 0)
            {
                Array.Copy(buffer, offset, buffer, 0, bufferLength - offset);
                bufferLength -= offset;
            }
        }
    }
}