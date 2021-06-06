package Bank.src;

import Other.Transaction;
import Server.LockFreeAccount;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantLock;

public class Account implements Serializable
{
    private static final short MAX_HISTORY_SIZE = 10;


    private int account_id;
    private ReentrantLock lock;
    private Queue<Transaction> history;
    private float balance;

    public Account(int id, float balance)
    {
        this.account_id = id;
        this.history    = new LinkedList<>();
        this.balance    = balance;
        this.lock       = new ReentrantLock();
    }

    public Account(LockFreeAccount a)
    {
        this.balance = a.getBalance();
        this.account_id = a.getAccount_id();
        this.history = a.getHistory();
    }

    public float getBalance ()
    {

        return balance;
    }

    public void setBalance  (float balance)
    {

        lock.lock();
        this.balance = balance;
        lock.unlock();
    }

    public void push        (Transaction t)
    {
        if (this.history.size() == MAX_HISTORY_SIZE) {
            this.history.remove();
        }

        this.history.add(t);
    }

    public boolean movement (float a, short type)
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

        push(new Transaction(LocalDateTime.now(), this.account_id, a, this.balance, -1, -1));

        lock.unlock();

        return flag;
    }

    public float   balance  ()
    {
        lock.lock();

        float b = this.balance;

        lock.unlock();

        return b;
    }

    public void    interest (float interest)
    {
        this.lock.lock();

        this.balance += this.balance * interest;

        this.push(new Transaction(LocalDateTime.now(), -1, -1));

        this.lock.unlock();
    }

    public List<Transaction> history()
    {
        this.lock.lock();

        List<Transaction> r = new ArrayList<>(this.history);

        this.lock.unlock();

        return r;
    }

    public int getAccount_id()
    {
        return account_id;
    }

    public void setHistory(Queue<Transaction> history) {
        lock.lock();
        this.history = history;
        lock.unlock();
    }
}
