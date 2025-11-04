namespace codecrafters_redis.redis_data_store;

public abstract record RedisObject()
{
    public abstract string Type { get; }
    public static string NoneType => "none";
}

public record RedisString(string Value, long ExpiryMs) : RedisObject()
{
    public override string Type => "string";
}

public record RedisList(LinkedList<string> Values) : RedisObject()
{
    public override string Type => "list";
}