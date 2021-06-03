import Bank.include.BankInterface;
import Client.ClientStub;

import java.util.Random;

import static java.lang.System.exit;

public class BankClient {

    private static final int MAX_ITER = 10000000;

    public static void main(String args[]) throws InterruptedException{
        BankInterface bank = new ClientStub(Integer.parseInt(args[0]));

        Random rand = new Random();
        float local_bal = 0F;

        //bank.movement(1, 100);
        //bank.transfer(1,2,50);

        for(int i = 0; i < MAX_ITER; i++){
            try {
                int x = rand.nextInt(15) - 10;
                if( bank.movement(1, x) ) System.out.println("Finished " + i + ";");
            }
            catch (NullPointerException e){
                System.out.println("Timed out on " + i + ";");
            }
        }

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
