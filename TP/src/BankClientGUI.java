import Bank.include.BankInterface;
import Client.ClientStub;
import Common.Input;
import Common.Transaction;

import java.util.List;

import static java.lang.System.exit;

public class BankClientGUI
{

    private BankInterface cs;

    private void menu()
    {
        System.out.println("--- BANK ---");
        System.out.println();
        System.out.println("1 - Balance");
        System.out.println("2 - Movement");
        System.out.println("3 - Transfer");
        System.out.println("4 - Interest");
        System.out.println("5 - History");
        System.out.println("6 - Leave");
        System.out.println();
        System.out.print("Choose what you want to do:");

        int option = Input.lerInt();
        switch (option)
        {
            case 1 : {
                System.out.print("AccountID: ");
                int accountID = Input.lerInt();
                float x = cs.balance(accountID);
                System.out.println("\n--- BALANCE: " + x + " ---");
                break;
            }
            case 2 : {
                System.out.print("AccountID: ");
                int accountID = Input.lerInt();
                System.out.print("Amount: ");
                float amount = Input.lerFloat();
                boolean x = cs.movement(accountID, amount);
                System.out.println("\n--- MOVEMENT result: " + x + " ---");
                break;
            }
            case 3 : {
                System.out.print("From: ");
                int from = Input.lerInt();
                System.out.print("To: ");
                int to = Input.lerInt();
                System.out.print("Amount: ");
                int amount = Input.lerInt();
                boolean x = cs.transfer(from, to, amount);
                System.out.println("\n--- TRANSFER result: " + x + " ---");
                break;
            }
            case 4 : {
                cs.interest();
                break;
            }
            case 5 : {
                System.out.print("AccountID: ");
                int accountID = Input.lerInt();
                List<Transaction> x = cs.history(accountID);
                System.out.println("--- HISTORY ---");
                for(Transaction t : x)
                    System.out.println(t.toString());
                break;
            }
            case 6 : {
                System.out.println("\n--- Leaving bank ---");
                exit(0);
                break;
            }
            default:
                menu();
        }
        System.out.println();
        menu();
    }

    public BankClientGUI(int port)
    {
        cs = new ClientStub(port);

        menu();
    }

}
