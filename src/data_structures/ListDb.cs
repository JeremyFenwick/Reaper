using System.Collections.Concurrent;
using System.Reflection.Metadata.Ecma335;
using System.Threading.Channels;

namespace codecrafters_redis.data_structures;

public class ListDb
{
    // HEADERS - USES THE ACTOR PATTERN + COMMAND PATTERN. DICT IS SINGLE THREADED
    public record DbEntry(bool Exists, List<string> Entries);

    private readonly Dictionary<string, DbEntry> _kvDb = new();
    private readonly Channel<ICommand> _commandChannel = Channel.CreateUnbounded<ICommand>();

    private interface ICommand
    {
        void Execute(Dictionary<string, DbEntry> db);
    }

    public ListDb()
    {
        _ = Task.Run(RunAsync);
    }

    // API FUNCTIONS

    public Task<DbEntry> GetAsync(string key)
    {
        var tcs = new TaskCompletionSource<DbEntry>();
        _commandChannel.Writer.TryWrite(new GetCommand(key, tcs));
        return tcs.Task;
    }

    public Task<int> AppendAsync(string key, List<string> values)
    {
        var tcs = new TaskCompletionSource<int>();
        _commandChannel.Writer.TryWrite(new AppendCommand(key, values, tcs));
        return tcs.Task;
    }

    public Task<int> LenAsync(string key)
    {
        var tcs = new TaskCompletionSource<int>();
        _commandChannel.Writer.TryWrite(new LenCommand(key, tcs));
        return tcs.Task;
    }

    public Task<List<string>> PopAsync(string key, int count)
    {
        var tcs = new TaskCompletionSource<List<string>>();
        _commandChannel.Writer.TryWrite(new PopCommand(key, count, tcs));
        return tcs.Task;
    }

    public Task<DbEntry> GetRangeAsync(string key, int start, int end)
    {
        var tcs = new TaskCompletionSource<DbEntry>();
        _commandChannel.Writer.TryWrite(new GetRangeCommand(key, start, end, tcs));
        return tcs.Task;
    }

    public Task<int> AppendToFrontAsync(string key, List<string> values)
    {
        var tcs = new TaskCompletionSource<int>();
        _commandChannel.Writer.TryWrite(new AppendToFrontCommand(key, values, tcs));
        return tcs.Task;
    }

    public Task<string?> BlPopAsync(string key, int? timeOut = null)
    {
        var tcs = new TaskCompletionSource<string?>();
        var timeOutDate = timeOut == null ? (DateTime?)null : DateTime.UtcNow.AddMilliseconds(timeOut.Value);
        _commandChannel.Writer.TryWrite(new BlPopCommand(key, timeOutDate, tcs));
        return tcs.Task;
    }

    public void Dispose()
    {
        _commandChannel.Writer.Complete();
    }

    // HELPER FUNCTIONS

    private static DbEntry EmptyEntry()
    {
        return new DbEntry(false, []);
    }

    private async Task RunAsync()
    {
        var blockedOperations = new Dictionary<string, Queue<BlPopCommand>>();
        await foreach (var command in _commandChannel.Reader.ReadAllAsync())
            switch (command)
            {
                case AppendCommand append:
                {
                    append.Execute(_kvDb);
                    RunBlockingOp(append.Key, blockedOperations);
                    break;
                }
                case AppendToFrontCommand appendToFront:
                {
                    appendToFront.Execute(_kvDb);
                    RunBlockingOp(appendToFront.Key, blockedOperations);
                    break;
                }
                case BlPopCommand blPop when blockedOperations.ContainsKey(blPop.Key):
                    blockedOperations[blPop.Key].Enqueue(blPop);
                    break;
                case BlPopCommand blPop when !_kvDb.ContainsKey(blPop.Key) || _kvDb[blPop.Key].Entries.Count == 0:
                    blockedOperations[blPop.Key] = new Queue<BlPopCommand>();
                    blockedOperations[blPop.Key].Enqueue(blPop);
                    break;
                default:
                    command.Execute(_kvDb);
                    break;
            }
    }

    private void RunBlockingOp(string key, Dictionary<string, Queue<BlPopCommand>> blockedOps)
    {
        if (!blockedOps.TryGetValue(key, out var queue)) return;
        while (queue.Count > 0 && _kvDb.ContainsKey(key) && _kvDb[key].Entries.Count > 0)
        {
            var pop = queue.Dequeue();
            pop.Execute(_kvDb);
        }

        if (queue.Count == 0) blockedOps.Remove(key);
    }

    // COMMANDS

    private record GetCommand(string Key, TaskCompletionSource<DbEntry> Tcs) : ICommand
    {
        public void Execute(Dictionary<string, DbEntry> db)
        {
            Tcs.SetResult(!db.TryGetValue(Key, out var entry) ? EmptyEntry() : entry);
        }
    }

    private record AppendCommand(
        string Key,
        List<string> Values,
        TaskCompletionSource<int> Tcs) : ICommand
    {
        public void Execute(Dictionary<string, DbEntry> db)
        {
            if (!db.ContainsKey(Key)) db[Key] = new DbEntry(true, []);
            db[Key].Entries.AddRange(Values);
            Tcs.SetResult(db[Key].Entries.Count);
        }
    }

    private record LenCommand(string Key, TaskCompletionSource<int> Tcs) : ICommand
    {
        public void Execute(Dictionary<string, DbEntry> db)
        {
            Tcs.SetResult(db.TryGetValue(Key, out var entry) ? entry.Entries.Count : 0);
        }
    }

    private record PopCommand(string Key, int Count, TaskCompletionSource<List<string>> Tcs) : ICommand
    {
        public void Execute(Dictionary<string, DbEntry> db)
        {
            var result = new List<string>();
            for (var i = 0; i < Count; i++)
            {
                if (db[Key].Entries.Count == 0) break;
                result.Add(db[Key].Entries[0]);
                db[Key].Entries.RemoveAt(0);
            }

            Tcs.SetResult(result);
        }
    }

    private record GetRangeCommand(string Key, int Start, int End, TaskCompletionSource<DbEntry> Tcs) : ICommand
    {
        public void Execute(Dictionary<string, DbEntry> db)
        {
            if (!db.TryGetValue(Key, out var entry))
            {
                Tcs.SetResult(EmptyEntry());
                return;
            }

            var count = entry.Entries.Count;

            var start = Start < 0 ? count + Start : Start;
            var end = End < 0 ? count + End : End;

            // Clamp start and end
            start = Math.Max(0, start);
            end = Math.Min(count - 1, end);

            // If start is after end, return empty
            if (start > end)
            {
                Tcs.SetResult(EmptyEntry());
                return;
            }

            var rangeLookup = entry.Entries.GetRange(start, end - start + 1);
            Tcs.SetResult(new DbEntry(true, rangeLookup));
        }
    }

    private record AppendToFrontCommand(
        string Key,
        List<string> Values,
        TaskCompletionSource<int> Tcs) : ICommand
    {
        public void Execute(Dictionary<string, DbEntry> db)
        {
            if (!db.ContainsKey(Key)) db[Key] = new DbEntry(true, []);
            Values.ForEach(v => db[Key].Entries.Insert(0, v));
            Tcs.SetResult(db[Key].Entries.Count);
        }
    }

    private record BlPopCommand(
        string Key,
        DateTime? TimeOut,
        TaskCompletionSource<string?> Tcs) : ICommand
    {
        public void Execute(Dictionary<string, DbEntry> db)
        {
            // Check timeout before blocking
            if (TimeOut != null && TimeOut < DateTime.UtcNow)
            {
                Tcs.SetResult(null);
                return;
            }

            // Try to pop from the list
            var entries = db[Key].Entries;

            // Actually pop the item when list is not empty
            var result = entries[0];
            entries.RemoveAt(0);
            Tcs.SetResult(result);
        }
    }
}