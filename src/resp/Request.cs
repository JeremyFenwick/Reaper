namespace codecrafters_redis.resp;

public abstract record Request();

public interface IWithTaskSource {
    public void SetException(Exception exception);
}

public record Void(): Request();
public record Ping(): Request();
public record EchoRequest(string Msg): Request();

public record Set(string Key, string Value, int ExpiryMs = 0) : Request(), IWithTaskSource
{
    public TaskCompletionSource<bool> TaskSource { get; } = new();
    public void SetException(Exception exception) => TaskSource.TrySetException(exception);
}

public record Get(string Key) : Request(), IWithTaskSource
{
    public TaskCompletionSource<string?> TaskSource { get; } = new();
    public void SetException(Exception exception) => TaskSource.TrySetException(exception);
}

public record RPush(string Key, List<string> Elements) : Request(),  IWithTaskSource
{
    public TaskCompletionSource<int> TaskSource { get; } = new();
    public void SetException(Exception exception) => TaskSource.TrySetException(exception);

}

public record LRange(string Key, int Start, int End) : Request(),  IWithTaskSource
{
    public TaskCompletionSource<List<string>> TaskSource { get; } = new();
    public void SetException(Exception exception) => TaskSource.TrySetException(exception);

}