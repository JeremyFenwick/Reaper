using System.Threading.Channels;
using codecrafters_redis.redis_data_store;
using codecrafters_redis.resp;
using Microsoft.Extensions.Logging;

namespace codecrafters_redis.data_structures;

public class KeyValueStore
{
    private readonly Channel<Request> _requestQueue = Channel.CreateUnbounded<Request>();
    private readonly Dictionary<string, LinkedList<Request>> _blockedRequests = new();
    private readonly Dictionary<string, RedisObject> _dataStore = new();
    private readonly ILogger<KeyValueStore> _logger;
    private int _counter = 0;

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
        throw new Exception("Failed to add LRange request to queue");
    }

    public Task<int> LPush(LPush lPush)
    {
        if (_requestQueue.Writer.TryWrite(lPush)) return lPush.TaskSource.Task;
        throw new Exception("Failed to add LPush request to queue");
    }

    public Task<int> LLen(LLen len)
    {
        if (_requestQueue.Writer.TryWrite(len)) return len.TaskSource.Task;
        throw new Exception("Failed to add LLen request to queue");
    }

    public Task<List<string>> LPop(LPop pop)
    {
        if (_requestQueue.Writer.TryWrite(pop)) return pop.TaskSource.Task;
        throw new Exception("Failed to add LPop request to queue");
    }

    public Task<string?> BlPop(BlPop blPop)
    {
        if (_requestQueue.Writer.TryWrite(blPop))
        {
            // Set the timer if it exists
            if (blPop.TimeoutMs > 0)
                _ = Task.Delay(TimeSpan.FromMilliseconds(blPop.TimeoutMs))
                    .ContinueWith(_ => blPop.TaskSource.TrySetResult(null));
            return blPop.TaskSource.Task;
        }

        throw new Exception("Failed to add BlPop request to queue");
    }

    // INTERNAL REQUEST PROCESSING
    private async Task ProcessRequests()
    {
        await foreach (var request in _requestQueue.Reader.ReadAllAsync())
            try
            {
                _counter++;
                _logger.LogInformation($"Processing request: {request}");
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
                    case LPop pop:
                        HandleLPop(pop);
                        break;
                    case BlPop blPop:
                        if (!_blockedRequests.ContainsKey(blPop.Key))
                            _blockedRequests.Add(blPop.Key, new LinkedList<Request>());
                        _blockedRequests[blPop.Key].AddLast(request);
                        break;
                    default:
                        _logger.LogError("Received unknown: {request}", request);
                        break;
                }

                // If we added a value a blocked request,
                // we need to check if we can clear r
                if (request is IHasKey hasKeyRequest and (resp.Set or resp.RPush or resp.LPush or resp.BlPop))
                    CheckBlockedRequests(hasKeyRequest);

                if (_counter >= 10_000)
                {
                    _logger.LogInformation("Cleaning blocked requests");
                    CleanBlockedRequests();
                    _counter = 0;
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

    private void CheckBlockedRequests(IHasKey request)
    {
        // Guard clause, technically this should never trigger
        if (!_dataStore.TryGetValue(request.Key, out var value) || value is not RedisList list) return;

        // Check we actually have blocked requests
        if (!_blockedRequests.TryGetValue(request.Key, out var blockedRequests)) return;

        // Clear as many blocked requests as we can
        _logger.LogInformation("Attempting to unblock requests for {key}", request.Key);
        var completed = new List<Request>();
        foreach (var blockedRequest in blockedRequests)
        {
            if (list.Values.Count == 0) break;
            if (blockedRequest is BlPop pop) HandleBlPop(pop);
            completed.Add(blockedRequest);
        }

        // Clear the completed requests from the blocked queue
        _logger.LogInformation("Completed {count} requests for {key}", completed.Count, request.Key);
        foreach (var item in completed) blockedRequests.Remove(item);
        if (blockedRequests.Count == 0) _blockedRequests.Remove(request.Key);
    }

    private void CleanBlockedRequests()
    {
        _logger.LogInformation("Cleaning blocked requests");
        var emptyKeys = new List<string>();
        var deadRequestCount = 0;

        foreach (var (key, list) in _blockedRequests)
        {
            var toRemove = new List<Request>();
            foreach (var request in list)
                if (request is BlPop { TaskSource.Task.IsCompleted: true })
                {
                    toRemove.Add(request);
                    deadRequestCount++;
                }

            foreach (var request in toRemove) list.Remove(request);
            if (list.Count == 0) emptyKeys.Add(key);
        }

        _logger.LogInformation("Removed {count} requests dead blocked reqquests", deadRequestCount);
        foreach (var key in emptyKeys) _blockedRequests.Remove(key);
    }

    private void HandleBlPop(BlPop blPop)
    {
        // Note this should never fail as we check for the entry when we clear the blocked list
        if (!_dataStore.TryGetValue(blPop.Key, out var value) || value is not RedisList list)
        {
            _logger.LogCritical(
                "Data structure is in an impossible situation where HandleBlPop was called without {blPop.Key}",
                blPop.Key);
            return;
        }

        var popped = list.Values.First();
        list.Values.RemoveFirst();
        blPop.TaskSource.TrySetResult(popped);
    }

    private void HandleLPop(LPop pop)
    {
        if (_dataStore.TryGetValue(pop.Key, out var value) && value is RedisList list)
        {
            var result = new List<string>();
            while (list.Values.Count > 0 && result.Count < pop.Number)
            {
                var popped = list.Values.First();
                list.Values.RemoveFirst();
                result.Add(popped);
            }

            pop.TaskSource.TrySetResult(result);
        }
        else
        {
            pop.TaskSource.TrySetResult([]);
        }
    }

    private void HandleLLen(LLen len)
    {
        if (_dataStore.TryGetValue(len.Key, out var value) && value is RedisList list)
            len.TaskSource.TrySetResult(list.Values.Count);
        else
            len.TaskSource.TrySetResult(0);
    }

    private void HandleLPush(LPush lPush)
    {
        if (_dataStore.TryGetValue(lPush.Key, out var value) && value is RedisList list)
        {
            foreach (var element in lPush.Elements) list.Values.AddFirst(element);
            lPush.TaskSource.TrySetResult(list.Values.Count);
        }
        else
        {
            lPush.Elements.Reverse();
            _dataStore[lPush.Key] = new RedisList(new LinkedList<string>(lPush.Elements));
            lPush.TaskSource.TrySetResult(lPush.Elements.Count);
        }
    }

    private void HandleLRange(LRange lRange)
    {
        if (!_dataStore.TryGetValue(lRange.Key, out var value) || value is not RedisList redistList)
            lRange.TaskSource.TrySetResult([]);
        else
            lRange.TaskSource.TrySetResult(SafeSlice(redistList.Values, lRange.Start, lRange.End));
        return;

        List<string> SafeSlice(LinkedList<string> list, int start, int end)
        {
            if (start < 0) start = list.Count + start;
            if (end < 0) end = list.Count + end;

            // Clamp both start and end
            start = Math.Max(0, start);
            end = Math.Min(list.Count - 1, end);

            if (start > end) return [];

            var result = new List<string>(end - start + 1);
            var index = 0;

            // iterate linked list until we reach `start`
            for (var node = list.First; node != null && index <= end; node = node.Next, index++)
                if (index >= start)
                    result.Add(node.Value);

            return result;
        }
    }

    private void HandleRPush(RPush rPush)
    {
        if (_dataStore.TryGetValue(rPush.Key, out var value) && value is RedisList list)
        {
            foreach (var element in rPush.Elements) list.Values.AddLast(element);
            rPush.TaskSource.TrySetResult(list.Values.Count);
        }
        else
        {
            _dataStore[rPush.Key] = new RedisList(new LinkedList<string>(rPush.Elements));
            rPush.TaskSource.TrySetResult(rPush.Elements.Count);
        }
    }


    private void HandleSet(Set set)
    {
        var expiryTime = set.ExpiryMs == 0
            ? 0
            : set.ExpiryMs + DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var newEntry = new RedisBasicEntry(set.Value, expiryTime);
        _dataStore[set.Key] = newEntry;
        set.TaskSource.TrySetResult(true);
    }

    private void HandleGet(Get get)
    {
        _dataStore.TryGetValue(get.Key, out var entry);
        // Case where we have a valid entry
        if (entry is RedisBasicEntry kvEntry && LiveEntry(get.Key, kvEntry))
            get.TaskSource.TrySetResult(kvEntry.Value);
        else
            get.TaskSource.TrySetResult(null);
    }

    private bool LiveEntry(string key, RedisBasicEntry entry)
    {
        if (entry.ExpiryMs == 0 || entry.ExpiryMs > DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()) return true;
        // Remove the dead value
        _dataStore.Remove(key);
        return false;
    }
}