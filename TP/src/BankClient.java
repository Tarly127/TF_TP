import Bank.include.BankInterface;
import Client.ClientStub;

import java.io.IOException;

public class BankClient {

    public static void main(String[] args) throws IOException
    {
        new BankClientGUI(Integer.parseInt(args[0]));
    }
}
