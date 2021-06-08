import Server.AtomixServer.ServerSkeleton;

public class BankServer {
    public static void main(String[] args)
    {
        new ServerSkeleton(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
    }
}
