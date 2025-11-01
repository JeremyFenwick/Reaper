using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.Extensions.Logging;

namespace codecrafters_redis.redis_server;

public class Server(ILogger<Server> logger, Context ctx)
{
    public void Run()
    {
        var listener = new TcpListener(IPAddress.Any, ctx.Port);
        listener.Start();
        logger.LogInformation($"Redis server started. Context: {ctx}");
        
        // Begin accepting connections
        while (true)
        {
            var client = listener.AcceptTcpClient();
            logger.LogInformation($"Client connected: {client.Client.RemoteEndPoint}");
            Task.Run(() => RespondAsync(client));
        }
    }

    private async Task RespondAsync(TcpClient client)
    {
        var stream = client.GetStream();
        var buffer = new byte[1024];

        try
        {
            while (true)
            {
                var bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
                if (bytesRead == 0) break;
                logger.LogInformation($"Received bytes: {bytesRead}");
                await stream.WriteAsync("+PONG\r\n"u8.ToArray());
            }
        }
        catch (Exception e)
        {
            logger.LogError(e.Message);
        }
        finally
        {
            client.Close();
        }
    }
}