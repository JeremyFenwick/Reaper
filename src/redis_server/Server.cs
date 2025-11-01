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
        logger.LogInformation($"Redis server started. Listening on port {ctx.Port}");
        
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
        using (client)
        {
            var stream = client.GetStream();
            logger.LogInformation($"Sending pong response to client: {client.Client.RemoteEndPoint}");
            var msg = "+PONG\r\n"u8.ToArray();
            await stream.WriteAsync(msg);
            client.Close();
        }
    }
}