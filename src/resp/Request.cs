namespace codecrafters_redis.resp;

// REQUEST DEFINITIONS
public abstract record Request();

public interface IWithTaskSource
{
    public void SetException(Exception exception);
}

public interface IHasKey
{
    string Key { get; }
}

// REQUEST VARIATIONS
public record Void() : Request();

public record Ping() : Request();

public record EchoRequest(string Msg) : Request();

public record Set(string Key, string Value, int ExpiryMs = 0) : Request(), IWithTaskSource, IHasKey
{
    public TaskCompletionSource<bool> TaskSource { get; } = new();

    public void SetException(Exception exception)
    {
        TaskSource.TrySetException(exception);
    }
}

public record Get(string Key) : Request(), IWithTaskSource, IHasKey
{
    public TaskCompletionSource<string?> TaskSource { get; } = new();

    public void SetException(Exception exception)
    {
        TaskSource.TrySetException(exception);
    }
}

public record RPush(string Key, List<string> Elements) : Request(), IWithTaskSource, IHasKey
{
    public TaskCompletionSource<int> TaskSource { get; } = new();

    public void SetException(Exception exception)
    {
        TaskSource.TrySetException(exception);
    }
}

public record LRange(string Key, int Start, int End) : Request(), IWithTaskSource, IHasKey
{
    public TaskCompletionSource<List<string>> TaskSource { get; } = new();

    public void SetException(Exception exception)
    {
        TaskSource.TrySetException(exception);
    }
}

public record LPush(string Key, List<string> Elements) : Request(), IWithTaskSource, IHasKey
{
    public TaskCompletionSource<int> TaskSource { get; } = new();

    public void SetException(Exception exception)
    {
        TaskSource.TrySetException(exception);
    }
}

public record LLen(string Key) : Request(), IWithTaskSource, IHasKey
{
    public TaskCompletionSource<int> TaskSource { get; } = new();

    public void SetException(Exception exception)
    {
        TaskSource.TrySetException(exception);
    }
}

public record LPop(string Key, int Number = 1) : Request(), IWithTaskSource, IHasKey
{
    public TaskCompletionSource<List<string>> TaskSource { get; } = new();

    public void SetException(Exception exception)
    {
        TaskSource.TrySetException(exception);
    }
}

public record BlPop(string Key, int TimeoutMs = 0) : Request(), IWithTaskSource, IHasKey
{
    public TaskCompletionSource<string?> TaskSource { get; } = new();

    public void SetException(Exception exception)
    {
        TaskSource.TrySetException(exception);
    }
}

public record GetType(string Key) : Request(), IWithTaskSource, IHasKey
{
    public TaskCompletionSource<string> TaskSource { get; } = new();

    public void SetException(Exception exception)
    {
        TaskSource.TrySetException(exception);
    }
}