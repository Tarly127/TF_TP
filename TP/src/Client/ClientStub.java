package Client;

import Bank.include.BankInterface;
import Common.Input;
import Common.Messages.ReqMessage;
import Common.Messages.ResMessage;
import Common.Transaction;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;

import static java.lang.System.exit;


public class ClientStub implements BankInterface
{

    private static final int TIMEOUT_LIMIT = 10;
    private static final int REQ_PORT = 10000;

    private NettyMessagingService ms;
    private ScheduledExecutorService es;
    private Address address;
    private Address toAddress;
    private Serializer s;

    private int lastReq;


    private Map<Integer, CompletableFuture<Float>>             balance_requests;
    private Map<Integer, CompletableFuture<Boolean>>           movement_requests;
    private Map<Integer, CompletableFuture<Boolean>>           transfer_requests;
    private Map<Integer, CompletableFuture<Long>>              interest_requests;
    private Map<Integer, CompletableFuture<List<Transaction>>> history_requests;

    private Map<Integer, Integer> server_responses; // may be useless? only truly useful if client doesn't block
                                                    // after sending requests

    public ClientStub(int port)
    {
        this.address = Address.from("localhost",port);

        // This address will be fixed and of my choosing
        this.toAddress = Address.from("localhost", REQ_PORT);

        this.es = Executors.newScheduledThreadPool(1);
        this.ms = new NettyMessagingService("ClientStub", this.address, new MessagingConfig());
        this.s  = Serializer.builder().withTypes( ReqMessage.class,
                                                  ResMessage.class,
                                                  Transaction.class,
                                                  LocalDateTime.class
                                                ).build();

        this.balance_requests  = new HashMap<>();
        this.movement_requests = new HashMap<>();
        this.transfer_requests = new HashMap<>();
        this.history_requests  = new HashMap<>();
        this.interest_requests = new HashMap<>();
        this.server_responses  = new HashMap<>();

        this.lastReq = 0;

        //  BALANCE RESPONSE
        this.ms.registerHandler("balance-res",  (a, m) -> {
            ResMessage<Float> rmsg = this.s.decode(m);

            CompletableFuture<Float> req = this.balance_requests.get(rmsg.getReqId());

            if (this.balance_requests.containsKey(rmsg.getReqId())) {

                int s_port = a.port();

                if (server_responses.containsKey(s_port)) {
                    int i = server_responses.get(s_port);
                    i++;
                    server_responses.replace(s_port, i);
                } else {
                    server_responses.put(a.port(), 1);
                }
                req.complete(rmsg.getResponse());
                //this.balance_requests.remove(rmsg.getReqId());
            }
        }, this.es);

        // MOVEMENT RESPONSE
        this.ms.registerHandler("movement-res", (a, m) -> {
            ResMessage<Boolean> rmsg = this.s.decode(m);

            CompletableFuture<Boolean> req = this.movement_requests.get(rmsg.getReqId());

            if (this.movement_requests.containsKey(rmsg.getReqId())) {

                int s_port = a.port();

                if (server_responses.containsKey(s_port)) {
                    int i = server_responses.get(s_port);
                    i++;
                    server_responses.replace(s_port, i);
                } else {
                    server_responses.put(a.port(), 1);
                }
                req.complete(rmsg.getResponse());
                //this.movement_requests.remove(rmsg.getReqId());
            }
        }, this.es);

        // TRANSFER RESPONSE
        this.ms.registerHandler("transfer-res", (a, m) -> {
            ResMessage<Boolean> rmsg = this.s.decode(m);

            CompletableFuture<Boolean> req = this.transfer_requests.get(rmsg.getReqId());

            if (this.transfer_requests.containsKey(rmsg.getReqId())) {

                int s_port = a.port();

                if (server_responses.containsKey(s_port)) {
                    int i = server_responses.get(s_port);
                    i++;
                    server_responses.replace(s_port, i);
                } else {
                    server_responses.put(a.port(), 1);
                }
                req.complete(rmsg.getResponse());
                //this.movement_requests.remove(rmsg.getReqId());
            }
        }, this.es);

        // INTEREST RESPONSE
        this.ms.registerHandler("interest-res", (a, m) -> {

            ResMessage<Void> rmsg = this.s.decode(m);

            CompletableFuture<Long> req = this.interest_requests.get(rmsg.getReqId());

            if (this.interest_requests.containsKey(rmsg.getReqId())) {

                int s_port = a.port();

                if (server_responses.containsKey(s_port)) {
                    int i = server_responses.get(s_port);
                    i++;
                    server_responses.replace(s_port, i);
                } else {
                    server_responses.put(a.port(), 1);
                }
                req.complete(0L);
            }
        }, this.es);

        // HISTORY RESPONSE
        this.ms.registerHandler("history-res",  (a, m) -> {

            //System.out.println("Received a History Response");

            ResMessage<List<Transaction>> rmsg = this.s.decode(m);

            CompletableFuture<List<Transaction>> req = this.history_requests.get(rmsg.getReqId());

            if (this.history_requests.containsKey(rmsg.getReqId())) {

                int s_port = a.port();

                if (server_responses.containsKey(s_port)) {
                    int i = server_responses.get(s_port);
                    i++;
                    server_responses.replace(s_port, i);
                } else {
                    server_responses.put(a.port(), 1);
                }
                req.complete(rmsg.getResponse());
            }
        }, this.es);

        this.ms.start();
    }

    // BALANCE  (accountId) -> float
    public float   balance(int accountID)
    {
        try {
            CompletableFuture<Float> fut_balance = new CompletableFuture<>();
            fut_balance.completeOnTimeout(null, ClientStub.TIMEOUT_LIMIT, TimeUnit.SECONDS);

            int reqId = this.lastReq++;
            ReqMessage msg = new ReqMessage(accountID, Transaction.BALANCE, reqId, 0);


            this.balance_requests.put(reqId, fut_balance);

            // Send request
            this.ms.sendAndReceive(toAddress, "balance-req", this.s.encode(msg));

            float res = fut_balance.get();

            this.balance_requests.remove(reqId);

            return res;
        }
        catch(InterruptedException | ExecutionException | NullPointerException e)
        {
            System.err.println("Request Failed!");

            return -1;
        }
    }

    // MOVEMENT (accountId, amount) -> boolean
    public boolean movement(int accountID, float amount)
    {
        try
        {
            CompletableFuture<Boolean> fut_mov = new CompletableFuture<>();
            fut_mov.completeOnTimeout(null, ClientStub.TIMEOUT_LIMIT, TimeUnit.SECONDS);

            int reqId = this.lastReq++;
            ReqMessage msg = new ReqMessage(accountID, Transaction.MOVEMENT, reqId, amount);


            this.movement_requests.put(reqId, fut_mov);

            // Send requests
            this.ms.sendAsync(toAddress, "movement-req", this.s.encode(msg));


            // If a request is sent and there is no response, the client will stop
            // so we'll ignore the response for now
            boolean res = fut_mov.get();

            this.movement_requests.remove(reqId);

            return res;
        }
        catch (InterruptedException | ExecutionException | NullPointerException e)
        {
            System.err.println("MOVEMENT Request Failed!");
            return false;
        }
    }

    // SET      (accountId, amount) -> void
    public void    set(int accountID, float amount)
    {

        // This is only here because I set it as a member of the interface of the Bank, and since ClientSub is an
        // implementation of it, it needs to have an implementation of set(), but it goes unused

        return;
    }

    // TRANSFER (from, to, amount) -> boolean
    public boolean transfer(int from, int to, float amount)
    {
        try {
            CompletableFuture<Boolean> fut_transfer = new CompletableFuture<>();
            fut_transfer.completeOnTimeout(null, ClientStub.TIMEOUT_LIMIT, TimeUnit.SECONDS);

            int reqId = this.lastReq++;

            ReqMessage msg = new ReqMessage(from, to, Transaction.TRANSFER, reqId, amount);

            this.transfer_requests.put(reqId, fut_transfer);

            // Send CompletableFuture
            this.ms.sendAsync(toAddress, "transfer-req", this.s.encode(msg));

            boolean res = fut_transfer.get();

            this.transfer_requests.remove(reqId);

            return res;
        }
        catch (InterruptedException | ExecutionException | NullPointerException e)
        {
            System.err.println("TRANSFER Request Failed!");
            return false;
        }

    }

    // INTEREST () -> ()
    public void interest()
    {
        try
        {
            CompletableFuture<Long> fut_transfer = new CompletableFuture<>();
            fut_transfer.completeOnTimeout(null, ClientStub.TIMEOUT_LIMIT, TimeUnit.SECONDS);

            int reqId = this.lastReq++;

            ReqMessage msg = new ReqMessage(-1, -1, Transaction.INTEREST, reqId, -1);

            this.interest_requests.put(reqId, fut_transfer);

            // Send CompletableFuture
            this.ms.sendAsync(toAddress, "interest-req", this.s.encode(msg));

            fut_transfer.get();

            this.transfer_requests.remove(reqId);
        }
        catch (InterruptedException | ExecutionException | NullPointerException e)
        {
            System.err.println("INTEREST Request Failed");
        }
    }

    // HISTORY  (accountId) -> List<Transaction>
    public List<Transaction> history (int accountID)
    {
        try {
            CompletableFuture<List<Transaction>> fut_history = new CompletableFuture<>();
            fut_history.completeOnTimeout(null, ClientStub.TIMEOUT_LIMIT, TimeUnit.SECONDS);

            int reqId = this.lastReq++;

            ReqMessage msg = new ReqMessage(accountID, -1, Transaction.HISTORY, reqId, -1);

            this.history_requests.put(reqId, fut_history);

            // Send CompletableFuture
            this.ms.sendAsync(toAddress, "history-req", this.s.encode(msg));

            List<Transaction> res = fut_history.get();

            this.history_requests.remove(reqId);

            if( res  == null ) throw new NullPointerException();

            return res;
        }
        catch (InterruptedException | ExecutionException | NullPointerException e)
        {
            System.err.println("HISTORY Request Failed!");
            return null;
        }
    }

}