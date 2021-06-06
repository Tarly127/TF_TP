package Server;

import Bank.src.Account;
import Bank.src.Bank;
import Other.Transaction;

import java.util.LinkedList;
import java.util.Queue;

public class LockFreeAccount
{
    private int account_id;
    private Queue<Transaction> history;
    private float balance;

    public LockFreeAccount(Account a)
    {
        this.account_id = a.getAccount_id();
        this.balance = a.balance();
        this.history = new LinkedList<>(a.history());
    }

    public float getBalance ()
    {
        return balance;
    }

    public Queue<Transaction> getHistory() {
        return history;
    }

    public int getAccount_id() {
        return account_id;
    }

}
