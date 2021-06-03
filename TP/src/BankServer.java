import Server.ServerSkeleton;

public class BankServer {
    public static void main(String args[]) throws InterruptedException{

        ServerSkeleton server = new ServerSkeleton(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
    }
}
