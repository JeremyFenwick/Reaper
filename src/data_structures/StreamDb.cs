using System.Threading.Channels;

namespace codecrafters_redis.data_structures;

public class StreamDb
{
    public record DbEntry(bool Exists, List<StreamEntry> Entries);

    public record StreamEntry(string Id, List<KeyValuePair<string, string>> Fields);

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

    public Task<string> AddAsync(string key, string? id, List<KeyValuePair<string, string>> fields)
    {
        var tcs = new TaskCompletionSource<string>();
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
        TaskCompletionSource<string> Tcs) : ICommand
    {
        public void Execute(Dictionary<string, DbEntry> db)
        {
            if (!db.ContainsKey(Key)) db[Key] = new DbEntry(true, []);
            db[Key].Entries.Add(new StreamEntry(Id, Pairs));
            Tcs.SetResult(Id);
        }
    }
}