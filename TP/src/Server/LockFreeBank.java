package Server;

import Bank.src.Account;
import Bank.src.Bank;

import java.util.HashMap;
import java.util.Map;

public class LockFreeBank {

    private Float         interest;
    Map<Integer, LockFreeAccount> accounts;

    public LockFreeBank(float interest)
    {
        this.accounts = new HashMap<>();
        this.interest = interest;
    }

    public void addAllAccounts(Map<Integer, Account> in)
    {
        for(Map.Entry<Integer, Account> e : in.entrySet())
        {
            this.accounts.put(e.getKey(), new LockFreeAccount(e.getValue()));
        }

    }

    public Float getInterest() {
        return interest;
    }

    public Map<Integer, LockFreeAccount> getAccounts() {
        return accounts;
    }
}
