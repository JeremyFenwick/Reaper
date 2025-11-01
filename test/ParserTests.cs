using System.Text;
using codecrafters_redis.resp;
using NUnit.Framework;

namespace codecrafters_redis.test;

public class ParserTests
{
    [Test]
    public void ParsePing()
    {
        var data = "*1\r\n$4\r\nPING\r\n"u8.ToArray();
        var success = Parser.TryParse(data, out var consumed, out var request);
        Assert.That(success, Is.True);
        Assert.That(request, Is.TypeOf<Ping>());
        Assert.That(consumed, Is.EqualTo(14));
    }

    [Test]
    public void IncompleteMessage()
    {
        var data = "*1\r\n$4\r\nPI"u8.ToArray();
        var success = Parser.TryParse(data, out var consumed, out var request);
        Assert.That(success, Is.False);
    }

    [Test]
    public void ParseEcho()
    {
        var data = "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"u8.ToArray();
        var success = Parser.TryParse(data, out var consumed, out var request);
        Assert.That(success, Is.True);
        Assert.That(request, Is.TypeOf<Echo>());
        Assert.That(((Echo)request).Msg, Is.EqualTo("hey"));
        Assert.That(consumed, Is.EqualTo(23));
    }
}