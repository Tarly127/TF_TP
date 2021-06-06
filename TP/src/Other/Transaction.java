package Other;

import java.io.Serializable;
import java.time.LocalDateTime;

public class Transaction implements Serializable
{

    public static final short BALANCE  = 0;
    public static final short MOVEMENT = 1;
    public static final short TRANSFER = 6;
    public static final short INTEREST = 7;
    public static final short HISTORY  = 8;

    private int account_from;
    private int account_to;

    private float amount;

    private float amount_after_from;
    private float amount_after_to;

    private long          req_id;      // External Client Side ID
    private long          internal_id; // Internal Server Group ID
    private short         type;
    private LocalDateTime time_created;



    // MOVEMENT constructor
    public Transaction(LocalDateTime time_created, int account_to, float amount, float amount_after, long req_id,
                       long internal_id)
    {

        this.account_to = account_to;
        this.account_from = -1;

        this.amount = amount;

        this.amount_after_to = amount_after;
        this.amount_after_from = -1;

        this.type = MOVEMENT;
        this.req_id = req_id;
        this.internal_id = internal_id;

        this.time_created = time_created;

    }

    // TRANSFER constructor
    public Transaction(LocalDateTime time_created, int account_to, int account_from, float amount,
                       float amount_after_to, float amount_after_from, long req_id, long internal_id)
    {

        this.account_to = account_to;
        this.account_from = account_from;

        this.amount = amount;

        this.amount_after_to   = amount_after_to;
        this.amount_after_from = amount_after_from;

        this.type = TRANSFER;
        this.req_id = req_id;
        this.internal_id = internal_id;

        this.time_created = time_created;

    }

    // INTEREST constructor
    public Transaction(LocalDateTime time_created, long req_id, long internal_id)
    {

        this.account_to   = -1;
        this.account_from = -1;

        this.amount = -1.f;

        this.amount_after_to   = -1;
        this.amount_after_from = -1;

        this.type = INTEREST;
        this.req_id = req_id;
        this.internal_id = internal_id;

        this.time_created = time_created;

    }

    public int getAccount_from() {
        return account_from;
    }

    public int getAccount_to() {
        return account_to;
    }

    public float getAmount() {
        return amount;
    }

    public float getAmount_after_from() {
        return amount_after_from;
    }

    public float getAmount_after_to() {
        return amount_after_to;
    }

    public long getReq_id() {
        return req_id;
    }

    public short getType() {
        return type;
    }

    public LocalDateTime getTime_created() {
        return time_created;
    }

    public long getInternal_id() {
        return internal_id;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        switch (this.getType())
        {
            case MOVEMENT:
            {
                sb.append("MOVEMENT (").append(this.req_id).append("): ").append(this.account_to).append(" (").append(this.amount).append("$)");
                break;
            }
            case TRANSFER:
            {
                sb.append("TRANSFER (").append(this.req_id).append("): ").append(this.account_to).append(" (").append(this.amount).append("$)");
                break;
            }
            case INTEREST:
            {
                sb.append("INTEREST (").append(this.req_id).append(")");
                break;
            }
        }

        return sb.toString();
    }
}
