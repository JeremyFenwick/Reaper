namespace codecrafters_redis.resp;

public abstract record Request();

public record Void(): Request();
public record Ping(): Request();
public record Echo(string Msg): Request();