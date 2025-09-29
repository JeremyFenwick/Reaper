using System.Threading.Channels;
using codecrafters_redis.resp;

namespace codecrafters_redis.data_structures;

public class StreamDb
{
    public record Result(bool Error, string Message);

    private record DbEntry(List<StreamEntry> Entries);

    private record StreamEntry(string Id, long Timestamp, int Sequence, List<KeyValuePair<string, string>> Fields);

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

    public Task<Result> AddAsync(XAdd xAdd)
    {
        var tcs = new TaskCompletionSource<Result>();
        _commandChannel.Writer.TryWrite(new XAddCommand(xAdd.Key, xAdd.TimeStamp, xAdd.Sequence, xAdd.Pairs, tcs));
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
        long? Timestamp,
        int? Sequence,
        List<KeyValuePair<string, string>> Pairs,
        TaskCompletionSource<Result> Tcs) : ICommand
    {
        public void Execute(Dictionary<string, DbEntry> db)
        {
            // Check if the ID is valid
            if (Timestamp == 0 && Sequence == 0)
            {
                Tcs.SetResult(new Result(true, "The ID specified in XADD must be greater than 0-0"));
                return;
            }

            if (!db.ContainsKey(Key)) db[Key] = new DbEntry([]);
            var entries = db[Key].Entries;
            // We may not have a prior entry to compare to
            var last = entries.Count > 0 ? entries[^1] : null;
            var (valid, newTs, newSeq) = GenerateId(Timestamp, Sequence, last?.Timestamp, last?.Sequence);
            if (!valid)
            {
                Tcs.SetResult(new Result(true,
                    "The ID specified in XADD is equal or smaller than the target stream top item"));
                return;
            }

            entries.Add(new StreamEntry($"{newTs}-{newSeq}", newTs, newSeq, Pairs));
            Tcs.SetResult(new Result(false, $"{newTs}-{newSeq}"));
        }
    }

    private static (bool valid, long Timestamp, int Sequence) GenerateId(long? timestamp, int? sequence,
        long? lastTimestamp,
        int? lastSequence)
    {
        // Check if the ID is valid
        if (timestamp < lastTimestamp || (timestamp == lastTimestamp && sequence < lastSequence))
            return (false, 0, 0);
        // Generate the values if requested
        timestamp ??= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        sequence ??= 0;
        // If the timestamp is 0 the sequence must be greater than 0
        if (timestamp == 0 && sequence == 0) return (true, 0, 1);
        // If we have no id to compare to we just return
        if (lastTimestamp == null || lastSequence == null) return (true, timestamp.Value, sequence.Value);
        // If we have an existing id check to see if we need to set the sequence
        if (timestamp == lastTimestamp) sequence = lastSequence + 1;

        return (true, timestamp.Value, sequence.Value);
    }
}