import Bank.include.BankInterface;
import Client.ClientStub;
import Other.Transaction;

import java.util.List;
import java.util.Random;

import static java.lang.System.exit;

public class BankClient {

    //private static final int MAX_ITER = 10000000;

    public static void main(String args[])
    {
        BankInterface bank = new ClientStub(Integer.parseInt(args[0]));

        for(int i = 0; i < 10; i++)
        {
            try
            {
                bank.movement(1,1);

                System.out.println("Ended request " + i);
            }
            catch (NullPointerException e)
            {
                System.out.println("Timed out on " + i + ";");
            }
        }


        try
        {
            float balance = bank.balance(1);

            System.out.println("Balance in the end: " + balance + "$");
        }
        catch (NullPointerException e)
        {
            System.out.println("Timed out on BALANCE request!");
        }


        exit(0);
    }
}
