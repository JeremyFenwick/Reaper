using System.Threading.Channels;
using codecrafters_redis.resp;
using Microsoft.Extensions.Logging;

namespace codecrafters_redis.data_structures;

public class KeyValueStore
{
    private readonly Channel<Request> _requestQueue = Channel.CreateUnbounded<Request>();
    private readonly Dictionary<string, string> _dataStore = new();
    private readonly ILogger<KeyValueStore> _logger;

    public KeyValueStore(ILogger<KeyValueStore> logger)
    {
        _logger = logger;
        Task.Run(ProcessRequests);
    }

    public Task<bool> Set(Set set)
    {
        if (_requestQueue.Writer.TryWrite(set)) return set.Task;
        _logger.LogCritical("Failed to add SET request to queue {set}", set);
        throw new  Exception("Failed to add SET request to queue");

    }

    public Task<string?> Get(Get get)
    {
        if (_requestQueue.Writer.TryWrite(get)) return get.Task;
        _logger.LogCritical("Failed to add GET request to queue {get}", get);
        throw new Exception("Failed to get response from queue");
    }

    private async Task ProcessRequests()
    {
        await foreach (var request in _requestQueue.Reader.ReadAllAsync())
        {
            try
            {
                switch (request)
                {
                    case Set set:
                        _dataStore[set.Key] = set.Value;
                        set.Tcs.SetResult(true);
                        break; 
                    case Get get:
                        _dataStore.TryGetValue(get.Key, out var value);
                        get.Tcs.SetResult(value);
                        break;
                    default:
                        _logger.LogError("Received unknown: {request}", request);
                        break;
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Failed to process request");
                switch (request)
                {
                    case Get get:
                        get.Tcs.SetException(e);
                        break;
                    case Set set:
                        set.Tcs.SetException(e);
                        break;
                }
            }
 
        }
    }
}