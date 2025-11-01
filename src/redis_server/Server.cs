using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using codecrafters_redis.data_structures;
using codecrafters_redis.resp;
using Microsoft.Extensions.Logging;
using Encoder = System.Text.Encoder;

namespace codecrafters_redis.redis_server;

public class Server(ILogger<Server> logger, Context ctx)
{
    private KeyValueStore store = new KeyValueStore(RedisLogger.CreateLogger<KeyValueStore>());
    public void Run()
    {
        var listener = new TcpListener(IPAddress.Any, ctx.Port);
        listener.Start();
        logger.LogInformation($"Redis server started. {ctx}");
        
        // Begin accepting connections
        while (true)
        {
            var client = listener.AcceptTcpClient();
            logger.LogInformation($"Client connected: {client.Client.RemoteEndPoint}");
            Task.Run(() => RespondAsync(client));
        }
    }
    
    // CLIENT HANDLING

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
                var response = await GenerateResponse(request);
                logger.LogInformation($"Sending response {response} to client {client.Client.RemoteEndPoint}");
                await stream.WriteAsync(response.ToBytes());
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

    private async Task<Response> GenerateResponse(Request request)
    {
        return request switch
        {
            Ping _ => new Pong(),
            EchoRequest echo => new BulkString(echo.Msg),
            Get get => await HandleGet(get),
            Set set => await HandleSet(set),
            _ => throw new ArgumentOutOfRangeException()
        };
    }
    
    // KV STORE OPERATIONS
    private async Task<Response> HandleGet(Get get)
    {
        var result = await store.Get(get);
        if (result == null) return new NullBulkString();
        return new BulkString(result);
    }

    private async Task<Response> HandleSet(Set set)
    {
        var result = await store.Set(set);
        if (result) return new Ok();
        throw new ArgumentOutOfRangeException();
    }
}