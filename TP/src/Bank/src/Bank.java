package Bank.src;

import Bank.include.BankInterface;
import Other.Transaction;
import Server.LockFreeAccount;
import Server.LockFreeBank;

import java.io.Serializable;
import java.util.*;


public class Bank implements BankInterface, Serializable
{
    private Float         interest;
    Map<Integer, Account> accounts;

    public Bank(float interest)
    {
        this.interest = interest;

        this.accounts = new HashMap<>();

        for(int i = 0; i < 20; i++)
        {
            this.accounts.put(i, new Account(0, 0f));
        }
    }

    public void update(LockFreeBank lfb)
    {
        this.interest = lfb.getInterest();
        for(Map.Entry<Integer, LockFreeAccount> e : lfb.getAccounts().entrySet())
        {
            this.accounts.get(e.getKey()).setBalance(e.getValue().getBalance());
            this.accounts.get(e.getKey()).setHistory(e.getValue().getHistory());
        }
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

    public LockFreeBank makeLockFree()
    {
        LockFreeBank lfb = new LockFreeBank(interest);

        lfb.addAllAccounts(accounts);

        return lfb;
    }



}
