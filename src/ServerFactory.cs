namespace codecrafters_redis;

public static class ServerFactory
{
    public static RedisServer CreateFromArgs(string[] args)
    {
        var argDict = ParseArgs(args);

        var port = argDict.TryGetValue("--port", out var portStr)
            ? int.Parse(portStr)
            : 6379;

        if (!argDict.TryGetValue("--replicaof", out var replicaOf)) return new RedisServer(port);
        var parts = replicaOf.Split(' ');
        var masterHost = parts[0];
        var masterPort = int.Parse(parts[1]);
        return new RedisServer(port, false, masterHost, masterPort);
    }

    private static Dictionary<string, string> ParseArgs(string[] args)
    {
        var dict = new Dictionary<string, string>();
        for (var i = 0; i < args.Length - 1; i++)
            if (args[i].StartsWith("--"))
                dict[args[i]] = args[i + 1];
        return dict;
    }
}