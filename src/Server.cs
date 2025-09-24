using System.Net.Sockets;

namespace codecrafters_redis;

public class Server(int port)
{
    private readonly TcpListener _listener = TcpListener.Create(port);

    public void Start()
    {
        _listener.Start();
        var client = _listener.AcceptTcpClient();
    }
}