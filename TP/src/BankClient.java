import Bank.include.BankInterface;
import Client.ClientStub;

public class BankClient {

    public static void main(String[] args)
    {
        new BankClientGUI(Integer.parseInt(args[0]));
    }
}
