using codecrafters_redis;

var port = 6379;
if (args.Length > 0 && args[0] == "--port")
{
    var newPort = int.Parse(args[1]);
    port = newPort;
}

var server = new RedisServer(port);
await server.StartAsync();