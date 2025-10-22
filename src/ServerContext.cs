namespace codecrafters_redis;

public record ServerContext(bool IsMaster, string? Host, int? Port, string? MasterHost, int? MasterPort);