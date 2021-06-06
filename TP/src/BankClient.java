import Bank.include.BankInterface;
import Client.ClientStub;
import Other.Transaction;

import java.util.List;
import java.util.Random;

import static java.lang.System.exit;

public class BankClient {

    private static final int MAX_ITER = 10000000;

    public static void main(String args[]) throws InterruptedException{
        BankInterface bank = new ClientStub(Integer.parseInt(args[0]));

        //for(int i = 0; i < 10; i++){
        //    try {
        //        bank.movement(1,100);
        //    }
        //    catch (NullPointerException e){
        //        System.out.println("Timed out on " + i + ";");
        //    }
        //}
//
        //bank.transfer(1,2,50);
//
        //bank.interest();
//
        //List<Transaction> lt = bank.history(1);
//
        //for(Transaction t : lt)
        //    System.out.println(t.toString());
//

        try{
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
