using System.Threading.Channels;
using codecrafters_redis.resp;
using Microsoft.Extensions.Logging;

namespace codecrafters_redis.data_structures;

public class KeyValueStore
{
    private readonly Channel<Request> _requestQueue = Channel.CreateUnbounded<Request>();
    private readonly Dictionary<string, RedisObject> _dataStore = new();
    private readonly ILogger<KeyValueStore> _logger;

    public KeyValueStore(ILogger<KeyValueStore> logger)
    {
        _logger = logger;
        Task.Run(ProcessRequests);
    }

    public Task<bool> Set(Set set)
    {
        if (_requestQueue.Writer.TryWrite(set)) return set.Task;
        throw new Exception("Failed to add SET request to queue");
    }

    public Task<string?> Get(Get get)
    {
        if (_requestQueue.Writer.TryWrite(get)) return get.Task;
        throw new Exception("Failed to get response from queue");
    }

    public Task<int> RPush(RPush rpush)
    {
        if (_requestQueue.Writer.TryWrite(rpush)) return rpush.Task;
        throw new Exception("Failed to add RPush request to queue");
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
                        HandleSet(set);
                        break; 
                    case Get get:
                        HandleGet(get);
                        break;
                    case RPush rpush:
                        HandleRPush(rpush);
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

    private void HandleRPush(RPush rpush)
    {
        if (_dataStore.TryGetValue(rpush.Key, out var value) && value is RedisList list)
        {
            list.Values.AddRange(rpush.Elements);
            _logger.LogInformation("Added {element} to existing list {key}", rpush.Elements, rpush.Key);
            rpush.Tcs.SetResult(list.Values.Count);
        }
        else
        {
            _dataStore[rpush.Key] = new RedisList(rpush.Elements);
            _logger.LogInformation("New new list with {element} under {key}", rpush.Elements, rpush.Key);
            rpush.Tcs.SetResult(rpush.Elements.Count);
        }
    }

    private void HandleSet(Set set)
    {
        var expiryTime = set.ExpiryMs == 0
            ? 0
            : set.ExpiryMs + DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var newEntry = new RedisBasicEntry(set.Value, expiryTime);
        _logger.LogInformation("Creating new entry {newEntry}", newEntry);
        _dataStore[set.Key] = newEntry;
        set.Tcs.SetResult(true);
    }

    private void HandleGet(Get get)
    {
        _dataStore.TryGetValue(get.Key, out var entry);
        // Case where we have a valid entry
        if (entry is RedisBasicEntry kvEntry && LiveEntry(get.Key, kvEntry))
        {
            _logger.LogInformation("Found {entry} from {get}", kvEntry, get);
            get.Tcs.SetResult(kvEntry.Value);
        }
        else
        {
            _logger.LogInformation("Failed to find entry for {get}", get);
            get.Tcs.SetResult(null);
        }
    }

    private bool LiveEntry(string key, RedisBasicEntry entry)
    {
        if (entry.ExpiryMs == 0 || entry.ExpiryMs > DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()) return true;
        // Remove the dead value
        _logger.LogInformation("Found dead {entry}. Removing.", entry);
        _dataStore.Remove(key);
        return false;
    }
}