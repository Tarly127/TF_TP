package Bank.include;

import Other.Transaction;

import java.util.List;

public interface BankInterface
{

    float   balance           (int accountID);

    boolean movement          (int accountID, float amount);

    boolean transfer          (int from, int to, float amount);

    void    interest          ();

    void    set               (int accountID, float amount);

    List<Transaction> history (int accountID);

}
