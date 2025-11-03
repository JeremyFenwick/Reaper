namespace codecrafters_redis.redis_data_store;

public record RedisObject();

public record RedisBasicEntry(string Value, long ExpiryMs) : RedisObject();

public record RedisList(LinkedList<string> Values) : RedisObject();