namespace codecrafters_redis.resp;

public abstract record Request();

public record Void(): Request();
public record Ping(): Request();
public record EchoRequest(string Msg): Request();

public record Set(string Key, string Value, int ExpiryMs = 0) : Request()
{
    public readonly TaskCompletionSource<bool> Tcs = new();
    public Task<bool> Task => Tcs.Task;
}

public record Get(string Key) : Request()
{
    public readonly TaskCompletionSource<string?> Tcs = new();
    public Task<string?> Task => Tcs.Task;
}