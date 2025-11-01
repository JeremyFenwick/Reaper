using System.Net;
using System.Net.Sockets;
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
        var _ = listener.AcceptTcpClient();
    }
}