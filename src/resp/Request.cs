namespace codecrafters_redis.resp;

public abstract record Request();

public record VoidRequest(): Request();
public record PingRequest(): Request();
public record EchoRequest(string Msg): Request();