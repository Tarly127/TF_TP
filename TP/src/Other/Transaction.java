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

    private short         type;
    private LocalDateTime time_created;
    private float         amount;
    private float         amount_after;

    public Transaction(short type, LocalDateTime time_created, float amount, float amount_after) {
        this.type = type;
        this.time_created = time_created;
        this.amount = amount;
        this.amount_after = amount_after;
    }

    public float getAmount() {
        return amount;
    }

    public float getAmount_after() {
        return amount_after;
    }

    public LocalDateTime getTime_created() {
        return time_created;
    }

    public short getType() {
        return type;
    }
}
