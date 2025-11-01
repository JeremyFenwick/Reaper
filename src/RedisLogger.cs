using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;

namespace codecrafters_redis;

public static class RedisLogger
{
    private static readonly ILoggerFactory Factory;
    
    static RedisLogger()
    {
        Factory = LoggerFactory.Create(builder =>
        {
            builder
                // Log to console
                .AddSimpleConsole(opts =>
                    {
                        opts.IncludeScopes = false;
                        opts.TimestampFormat = "[HH:mm:ss] ";
                        opts.ColorBehavior = LoggerColorBehavior.Enabled;
                        opts.SingleLine = true;
                    }
                
                ) 
                // Set minimum log level
                .SetMinimumLevel(LogLevel.Debug); 
        });
    }
    
    // Generic logger
    public static ILogger<T> CreateLogger<T>() => Factory.CreateLogger<T>();

    // Non-generic logger with string-based category
    public static ILogger CreateLogger(string categoryName) => Factory.CreateLogger(categoryName);

}