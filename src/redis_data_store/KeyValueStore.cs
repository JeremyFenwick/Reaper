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

    // PUBLIC API
    public Task<bool> Set(Set set)
    {
        if (_requestQueue.Writer.TryWrite(set)) return set.TaskSource.Task;
        throw new Exception("Failed to add SET request to queue");
    }

    public Task<string?> Get(Get get)
    {
        if (_requestQueue.Writer.TryWrite(get)) return get.TaskSource.Task;
        throw new Exception("Failed to get response from queue");
    }

    public Task<int> RPush(RPush rPush)
    {
        if (_requestQueue.Writer.TryWrite(rPush)) return rPush.TaskSource.Task;
        throw new Exception("Failed to add RPush request to queue");
    }

    public Task<List<string>> LRange(LRange range)
    {
        if (_requestQueue.Writer.TryWrite(range)) return range.TaskSource.Task;
        throw new Exception("Failed to add RPush request to queue");
    }

    public Task<int> LPush(LPush lPush)
    {
        if (_requestQueue.Writer.TryWrite(lPush)) return lPush.TaskSource.Task;
        throw new Exception("Failed to add RPush request to queue");
    }

    public Task<int> LLen(LLen len)
    {
        if (_requestQueue.Writer.TryWrite(len)) return len.TaskSource.Task;
        throw new Exception("Failed to add RPush request to queue");
    }

    // INTERNAL REQUEST PROCESSING
    private async Task ProcessRequests()
    {
        await foreach (var request in _requestQueue.Reader.ReadAllAsync())
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
                    case RPush rPush:
                        HandleRPush(rPush);
                        break;
                    case LRange lRange:
                        HandleLRange(lRange);
                        break;
                    case LPush lPush:
                        HandleLPush(lPush);
                        break;
                    case LLen len:
                        HandleLLen(len);
                        break;
                    default:
                        _logger.LogError("Received unknown: {request}", request);
                        break;
                }
            }
            catch (Exception e)
            {
                switch (request)
                {
                    case IWithTaskSource withTaskSource:
                        withTaskSource.SetException(e);
                        break;
                    default:
                        _logger.LogError(e, "Failed to process request");
                        break;
                }
            }
    }

    private void HandleLLen(LLen len)
    {
        if (_dataStore.TryGetValue(len.Key, out var value) && value is RedisList list)
            len.TaskSource.SetResult(list.Values.Count);
        else
            len.TaskSource.SetResult(0);
    }

    private void HandleLPush(LPush lPush)
    {
        if (_dataStore.TryGetValue(lPush.Key, out var value) && value is RedisList list)
        {
            foreach (var element in lPush.Elements) list.Values.Insert(0, element);
            lPush.TaskSource.SetResult(list.Values.Count);
        }
        else
        {
            lPush.Elements.Reverse();
            _dataStore[lPush.Key] = new RedisList(lPush.Elements);
            _logger.LogInformation("New list with {elements} under {key}", lPush.Elements, lPush.Key);
            lPush.TaskSource.SetResult(lPush.Elements.Count);
        }
    }

    private void HandleLRange(LRange lRange)
    {
        if (!_dataStore.TryGetValue(lRange.Key, out var value) || value is not RedisList redistList)
            lRange.TaskSource.SetResult([]);
        else
            lRange.TaskSource.SetResult(SafeSlice(redistList.Values, lRange.Start, lRange.End));
        return;

        List<string> SafeSlice(List<string> list, int start, int end)
        {
            if (start < 0) start = list.Count + start;
            if (end < 0) end = list.Count + end;

            // Clamp both start and end
            start = Math.Max(0, start);
            end = Math.Min(list.Count - 1, end);

            // If the start is greater than the end return nothing
            return start > end ? [] : list.GetRange(start, end - start + 1);
        }
    }

    private void HandleRPush(RPush rPush)
    {
        if (_dataStore.TryGetValue(rPush.Key, out var value) && value is RedisList list)
        {
            list.Values.AddRange(rPush.Elements);
            _logger.LogInformation("Added {elements} to existing list {key}", rPush.Elements, rPush.Key);
            rPush.TaskSource.SetResult(list.Values.Count);
        }
        else
        {
            _dataStore[rPush.Key] = new RedisList(rPush.Elements);
            _logger.LogInformation("New list with {elements} under {key}", rPush.Elements, rPush.Key);
            rPush.TaskSource.SetResult(rPush.Elements.Count);
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
        set.TaskSource.SetResult(true);
    }

    private void HandleGet(Get get)
    {
        _dataStore.TryGetValue(get.Key, out var entry);
        // Case where we have a valid entry
        if (entry is RedisBasicEntry kvEntry && LiveEntry(get.Key, kvEntry))
        {
            _logger.LogInformation("Found {entry} from {get}", kvEntry, get);
            get.TaskSource.SetResult(kvEntry.Value);
        }
        else
        {
            _logger.LogInformation("Failed to find entry for {get}", get);
            get.TaskSource.SetResult(null);
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