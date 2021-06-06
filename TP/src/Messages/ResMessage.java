package Messages;

public class ResMessage<T> {
    private int reqId;
    private T response;

    public ResMessage(int reqId, T response) {
        this.reqId = reqId;
        this.response = response;
    }

    public ResMessage(int reqId)
    {
        this.reqId = reqId;
    }

    public int getReqId() {
        return reqId;
    }

    public T getResponse() {
        return response;
    }
}
