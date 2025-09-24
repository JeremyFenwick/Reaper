using System.Net.Sockets;

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
        
        while (true)
        {
            var buffer = new byte[1024];
            var bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
            await writer.WriteAsync("+PONG\r\n");
        }
    }
}