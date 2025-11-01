using codecrafters_redis;
using codecrafters_redis.redis_server;

// Setup the server
var logger = RedisLoggerFactory.CreateLogger<Server>();
var ctx = new Context(6379);
var server = new Server(logger, ctx);
// Run the server
server.Run();

