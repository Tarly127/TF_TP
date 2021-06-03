package Messages;

public class ReqMessage {
    private int accountId;
    private int toAccountId;
    private int type; // 0 -> balance; 1 -> movement; 2 -> transfer
    private int reqId;
    private float amount; //if applicable

    public ReqMessage(int accountId, int type, int reqId, float amount){
        this.accountId = accountId;
        this.toAccountId = -1;
        this.type = type;
        this.reqId = reqId;
        this.amount = amount;
    }

    public ReqMessage(int accountId, int toAccountId, int type, int reqId, float amount){
        this.accountId = accountId;
        this.toAccountId = toAccountId;
        this.type = type;
        this.reqId = reqId;
        this.amount = amount;
    }

    public int getAccountId() {
        return accountId;
    }

    public void setAccountId(int accountId) {
        this.accountId = accountId;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getReqId() {
        return reqId;
    }

    public void setReqId(int reqId) {
        this.reqId = reqId;
    }

    public float getAmount() {
        return amount;
    }

    public void setAmount(float amount) {
        this.amount = amount;
    }

    public int getToAccountId() {
        return toAccountId;
    }

    public void setToAccountId(int toAccountId) {
        this.toAccountId = toAccountId;
    }
}
