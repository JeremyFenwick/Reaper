using System.Threading.Channels;
using codecrafters_redis.resp;
using Microsoft.Extensions.Logging;

namespace codecrafters_redis.data_structures;

public class KeyValueStore
{
    private readonly Channel<Request> _requestQueue = Channel.CreateUnbounded<Request>();
    private readonly Dictionary<string, KvEntry> _dataStore = new();
    private record KvEntry(string Value, long ExpiryTime = 0);
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
                        HandleSet(set);
                        break; 
                    case Get get:
                        HandleGet(get);
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

    private void HandleSet(Set set)
    {
        var expiryTime = set.ExpiryMs == 0
            ? 0
            : set.ExpiryMs + DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var newEntry = new KvEntry(set.Value, expiryTime);
        _logger.LogInformation("Creating new entry {newEntry}", newEntry);
        _dataStore[set.Key] = newEntry;
        set.Tcs.SetResult(true);
    }

    private void HandleGet(Get get)
    {
        _dataStore.TryGetValue(get.Key, out var entry);
        // Case where we have a valid entry
        if (entry != null && LiveEntry(get.Key, entry))
        {
            _logger.LogInformation("Found {entry} from {get}", entry, get);
            get.Tcs.SetResult(entry.Value);
        }
        else
        {
            _logger.LogInformation("Failed to find entry for {get}", get);
            get.Tcs.SetResult(null);
        }
    }

    private bool LiveEntry(string key, KvEntry entry)
    {
        if (entry.ExpiryTime == 0 || entry.ExpiryTime > DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()) return true;
        // Remove the dead value
        _logger.LogInformation("Found dead {entry}. Removing.", entry);
        _dataStore.Remove(key);
        return false;
    }
}