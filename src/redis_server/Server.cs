using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using codecrafters_redis.data_structures;
using codecrafters_redis.resp;
using Microsoft.Extensions.Logging;
using Array = codecrafters_redis.resp.Array;

namespace codecrafters_redis.redis_server;

public class Server(ILogger<Server> logger, Context ctx)
{
    private readonly KeyValueStore _store = new(RedisLogger.CreateLogger<KeyValueStore>());
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
                // Read in the bytes. If none are found the connection is closed
                var bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
                if (bytesRead == 0) break;
                // Parse the request
                requestData.AddRange(buffer.Take(bytesRead));
                if (!Parser.TryParse(CollectionsMarshal.AsSpan(requestData), out var consumed, out var request)) continue;
                logger.LogInformation($"Received request {request} from client {client.Client.RemoteEndPoint}");
                // Remove the consumed data
                requestData.RemoveRange(0, consumed);
                // Send the response
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
            RPush rPush => await HandleRPush(rPush),
            LRange lRange => await HandleLRange(lRange),
            LPush lPush => await HandleLPush(lPush),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    // KV STORE OPERATIONS
    private async Task<Response> HandleLRange(LRange lRange)
    {
        var result = await _store.LRange(lRange);
        return new Array(result);
    }
    private async Task<Response> HandleGet(Get get)
    {
        var result = await _store.Get(get);
        if (result == null) return new NullBulkString();
        return new BulkString(result);
    }

    private async Task<Ok> HandleSet(Set set)
    {
        var result = await _store.Set(set);
        if (result) return new Ok();
        throw new ArgumentOutOfRangeException();
    }
    
    private async Task<Integer> HandleRPush(RPush rPush)
    {
        var result = await _store.RPush(rPush);
        return new Integer(result);
    }
    
    private async Task<Integer> HandleLPush(LPush lPush)
    {
        var result = await _store.LPush(lPush);
        return new Integer(result);
    }
}