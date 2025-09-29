using System.Threading.Channels;

namespace codecrafters_redis.data_structures;

public class StreamDb
{
    public record Result(bool Error, string Message);

    private record DbEntry(List<StreamEntry> Entries);

    private record StreamEntry(string Id, List<KeyValuePair<string, string>> Fields);

    private readonly Channel<ICommand> _commandChannel = Channel.CreateUnbounded<ICommand>();
    private readonly Dictionary<string, DbEntry> _kvDb = new();

    private interface ICommand
    {
        void Execute(Dictionary<string, DbEntry> db);
    }

    public StreamDb()
    {
        _ = Task.Run(RunAsync);
    }

    public Task<Result> AddAsync(string key, string? id, List<KeyValuePair<string, string>> fields)
    {
        var tcs = new TaskCompletionSource<Result>();
        _commandChannel.Writer.TryWrite(new XAddCommand(key, id ?? Guid.NewGuid().ToString(), fields, tcs));
        return tcs.Task;
    }

    // HELPER FUNCTIONS

    private async Task RunAsync()
    {
        await foreach (var command in _commandChannel.Reader.ReadAllAsync()) command.Execute(_kvDb);
    }

    // COMMANDS

    private record XAddCommand(
        string Key,
        string Id,
        List<KeyValuePair<string, string>> Pairs,
        TaskCompletionSource<Result> Tcs) : ICommand
    {
        public void Execute(Dictionary<string, DbEntry> db)
        {
            if (!db.ContainsKey(Key)) db[Key] = new DbEntry([]);
            var entries = db[Key].Entries;
            if (entries.Count > 0)
            {
                var last = entries[^1];
                if (!ValidId(last.Id, Id))
                {
                    Tcs.SetResult(new Result(true, Id));
                    return;
                }
            }

            entries.Add(new StreamEntry(Id, Pairs));
            Tcs.SetResult(new Result(false, Id));
        }
    }

    private static bool ValidId(string lastId, string newId)
    {
        var (lastTimestamp, lastSequence) = ParseId(lastId);
        var (newTimestamp, newSequence) = ParseId(newId);
        if (newTimestamp < lastTimestamp) return false;
        return newTimestamp != lastTimestamp || newSequence > lastSequence;
    }

    private static (long timestamp, int sequence) ParseId(string id)
    {
        return id.Split('-') is var parts ? (long.Parse(parts[0]), int.Parse(parts[1])) : (0L, 0);
    }
}