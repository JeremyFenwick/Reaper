namespace codecrafters_redis;

public abstract record RespMessage;
public record Ping: RespMessage;
public record Echo(string Message): RespMessage;
public record Set(string Key, string Value, DateTime? Expiry) : RespMessage;
public record Get(string Key): RespMessage;
public record RPush(string ListName, List<string> Elements): RespMessage;
public record LPush(string ListName, List<string> Elements): RespMessage;
public record LRange(string ListName, int Start, int End): RespMessage;
public record LLen(string ListName): RespMessage;
public record Unknown : RespMessage;