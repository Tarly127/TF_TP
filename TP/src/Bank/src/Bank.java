package Bank.src;

import Bank.include.BankInterface;
import Other.Transaction;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class Account implements Serializable
{
    private static final short MAX_HISTORY_SIZE = 10;


    private ReentrantLock lock;
    private Queue<Transaction> history;
    private float balance;

    Account(float balance)
    {
        this.history = new LinkedList<>();
        this.balance = balance;
        this.lock    = new ReentrantLock();
    }

    float getBalance ()
    {

        return balance;
    }

    void setBalance  (float balance)
    {

        this.balance = balance;
    }

    void push        (Transaction t)
    {
        if (this.history.size() == MAX_HISTORY_SIZE) {
            this.history.remove();
        }

        this.history.add(t);
    }

    boolean movement (float a, short type)
    {
        boolean flag = true;

        lock.lock();

        if( a < 0 && -1 * a > balance)
        {
            flag = false;
        }
        else
        {
            balance += a;
        }

        push(new Transaction(type, LocalDateTime.now(), a, balance));

        lock.unlock();

        return flag;
    }

    float   balance  ()
    {
        lock.lock();

        float b = this.balance;

        lock.unlock();

        return b;
    }

    void    interest (float interest)
    {
        this.lock.lock();

        this.balance += this.balance * interest;

        this.push(new Transaction(Transaction.INTEREST, LocalDateTime.now(), -1, balance));

        this.lock.unlock();
    }

    List<Transaction> history()
    {
        this.lock.lock();

        List<Transaction> r = new ArrayList<>(this.history);

        this.lock.unlock();

        return r;
    }

}

public class Bank implements BankInterface, Serializable
{
    private Float         interest;
    Map<Integer, Account> accounts;

    public Bank(float interest)
    {
        this.interest = interest;

        this.accounts = new HashMap<>();

        this.accounts.put(1, new Account(0));
        this.accounts.put(2, new Account(0));
        this.accounts.put(3, new Account(0));
    }

    public float   balance(int accounID)
    {
        float amount = -1;

        if( this.accounts.containsKey((accounID)) )
        {
            amount = this.accounts.get(accounID).balance();
        }

        return amount;
    }

    public boolean movement(int accountID, float ammount)
    {
        boolean flag = false;

        if( this.accounts.containsKey(accountID) )
        {
            flag = this.accounts.get(accountID).movement(ammount, Transaction.MOVEMENT);
        }

        return flag;
    }

    public void    set(int accountID, float amount)
    {

        this.accounts.get(accountID).setBalance(amount);

    }

    public boolean transfer(int from, int to, float amount)
    {
        if ( amount > 0 )
            return     this.accounts.containsKey(from)
                    && this.accounts.containsKey(to)
                    && this.accounts.get(from).movement(-amount, Transaction.TRANSFER)
                    && this.accounts.get(to).movement( amount, Transaction.TRANSFER);
        else
            return     this.accounts.containsKey(from)
                    && this.accounts.containsKey(to)
                    && this.accounts.get(to).movement(-amount, Transaction.TRANSFER)
                    && this.accounts.get(from).movement( amount, Transaction.TRANSFER);

    }

    public void    interest()
    {
        for ( Map.Entry<?,Account> e : this.accounts.entrySet() )
        {
            e.getValue().interest(this.interest);
        }
    }

    public List<Transaction> history(int accountId)
    {
        if( this.accounts.containsKey(accountId) )
        {
            return this.accounts.get(accountId).history();
        }
        else
            return null;
    }



}
