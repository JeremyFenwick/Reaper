using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using codecrafters_redis.resp;
using Microsoft.Extensions.Logging;
using Encoder = System.Text.Encoder;

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
        var requestData = new List<byte>();
        try
        {
            while (true)
            {
                var bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
                if (bytesRead == 0) break;
                requestData.AddRange(buffer.Take(bytesRead));
                if (!Parser.TryParse(CollectionsMarshal.AsSpan(requestData), out var consumed, out var request)) continue;
                logger.LogInformation($"Received request {request} from client {client.Client.RemoteEndPoint}");
                // Remove the consumed data
                requestData.RemoveRange(0, consumed);
                var response = GenerateResponse(request);
                logger.LogInformation($"Sending response of length {response.Length} to client {client.Client.RemoteEndPoint}");
                await stream.WriteAsync(response);
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

    private byte[] GenerateResponse(Request request)
    {
        return request switch
        {
            Ping _ => RespEncoder.Pong(),
            Echo echo => RespEncoder.BulkString(echo.Msg),
            _ => throw new ArgumentOutOfRangeException()
        };
    }
}