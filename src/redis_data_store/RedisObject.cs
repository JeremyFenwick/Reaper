namespace codecrafters_redis.data_structures;

public record RedisObject();

public record RedisBasicEntry(string Value, long ExpiryMs) : RedisObject();

public record RedisList(LinkedList<string> Values) : RedisObject();