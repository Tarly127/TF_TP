package Server;


import Bank.include.BankInterface;
import Bank.src.Bank;
import GroupCommMiddleware.SpreadMiddleware;
import Messages.ReqMessage;
import Messages.ResMessage;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import spread.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

class QueuedRequest
{
    private SpreadGroup sg;
    private ReqMessage rm;

    QueuedRequest(SpreadGroup sg, ReqMessage rm) {
        this.sg = sg;
        this.rm = rm;
    }

    SpreadGroup getSender() {
        return sg;
    }

    ReqMessage getReqMessage() {
        return rm;
    }
}


class MyPair<U,V>
{
    private U x;
    private V y;

    MyPair(U x, V y) {
        this.x = x;
        this.y = y;
    }

    U getX() {
        return x;
    }

    V getY() {
        return y;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MyPair)) return false;
        MyPair<?, ?> myPair = (MyPair<?, ?>) o;
        return Objects.equals(x, myPair.x) &&
                Objects.equals(y, myPair.y);
    }
}

public class ServerSkeleton{

    private static final int   REQ_PORT               = 10000; // Guião 4



    // Guião 2
    private Serializer s;
    private ScheduledExecutorService es;

    // Guião 3
    private int port;

    // Guião 4
    private NettyMessagingService ms;

    // Guião 5
    //private boolean no_st;
    private int last_msg_seen;
    //private SpreadGroup sg;
    //private LinkedList<MyPair<Address, BankUpdate>> unanswered_reqs;

    // Guião 7
    private Map<Integer, Map<Integer, Float>> active_requests;
    //private int active_threads;
    //private int last_batch;
    //private ReentrantLock act_req_lock;
    //private ReentrantLock act_thr_lock;

    private BankInterface Bank;

    // TP
    private static final Float INTEREST = 0.05f;


    private SpreadMiddleware spread_gv;
    private boolean ready;


    public ServerSkeleton(int port, int connect_to){

        this.port = port;
        this.s    = Serializer.builder().withTypes(ReqMessage.class,
                                                   ResMessage.class,
                                                   Bank.class,
                                                   ArrayList.class,
                                                   QueuedRequest.class,
                                                   //FullBankTransfer.class,
                                                   MyPair.class,
                                                   Integer.class,
                                                   Address.class,
                                                   //BankUpdate.class

                                        ).build();
        this.es        = Executors.newScheduledThreadPool(1);

        this.Bank = new Bank();

        //this.history         = new LinkedList<>();
        //this.leader_queue    = new LinkedList<>();
        //this.unanswered_reqs = new LinkedList<>();
        this.active_requests = new HashMap<>();

        this.ms    = null;
        //this.no_st = true;

        //this.act_req_lock    = new ReentrantLock();
        //this.act_thr_lock    = new ReentrantLock();
        //this.active_threads  = 0;

        try
        {
            read_state();
        }
        catch (FileNotFoundException e)
        {
            this.Bank          = new Bank(ServerSkeleton.INTEREST);
            this.last_msg_seen = 0;
        }


        // As all Spread operations are asynchronous, everything they (eventually) return will need to be stored in a
        // CompletableFuture, and what actions would come after it is completed will need to be supplied right away
        // this will also apply to movements, interests and transfers!
        CompletableFuture<Boolean> fut_leader = new CompletableFuture<>()
                                                    .thenApply( is_leader -> {
                                                        if ( (Boolean) is_leader )
                                                        {
                                                            start_atomix();
                                                        }
                                                        else
                                                            ready = false;
                                                    });

        CompletableFuture<Void> updated = new CompletableFuture<>()
                                                    .thenApply( updated -> {
                                                       if ( spread_gv.is_ready() )
                                                           start_atomix();
                                                    });

        // Initialize Spread Connection
        this.spread_gv = new SpreadMiddleware(port, connect_to, last_msg_seen, fut_leader, updated);


        // So it writes state when killed
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                store_state();
            }
        });

        es.scheduleAtFixedRate(() -> {
            if()
                System.out.println("I'm the leader man!");
            else
                System.out.println("Someone else is the leader man");

            System.out.println("Account 1 balance: " + Bank.balance(1) + "$");
            System.out.println("Account 2 balance: " + Bank.balance(2) + "$");
        }, 0, 4, TimeUnit.SECONDS);
    }

    public void show(int accountID){
        System.out.println(accountID + " : " + this.Bank.balance(accountID));
    }

    // Atomix Handlers

    private void movement_handler(Address a, byte[] m)
    {
        last_msg_seen++;

        ReqMessage req_msg = this.s.decode(m);

        int accountId = req_msg.getAccountId();
        int req_id    = req_msg.getReqId();
        float amount  = req_msg.getAmount();

        boolean flag = this.Bank.movement(accountId,amount);

        ResMessage<Boolean> res_message = new ResMessage<>(req_id,flag);

        //System.out.println("Movement");

        if( flag )
        {

            CompletableFuture<Long> cf = new CompletableFuture<>()
                                            .thenApply((id) -> {
                                                System.out.println("cona");
                                            });

        }
        else {
            // If the operation is a failure, we don't need to notify everyone else,
            // so we can reply to the request right away
            this.ms.sendAsync(a, "movement-res", this.s.encode(res_message));
        }
    }

    private void balance_handler(Address a, byte[] m)
    {
        ReqMessage req_msg = this.s.decode(m);

        int accountId = req_msg.getAccountId();
        int req_id    = req_msg.getReqId();

        float cap = this.Bank.balance(accountId);

        ResMessage<Float> res_message = new ResMessage<>(req_id,cap);

        //System.out.println("Balance");


        // No point in warning everyone else about a BALANCE request, reply right away
        this.ms.sendAsync(a, "balance-res", this.s.encode(res_message));

    }

    private void transfer_handler(Address a, byte[] m)
    {
        ReqMessage req_msg = this.s.decode(m);

        int accountId    = req_msg.getAccountId();
        int to_accountId = req_msg.getToAccountId();
        int req_id       = req_msg.getReqId();
        float amount    = req_msg.getAmount();

        boolean flag = this.Bank.transfer(accountId, to_accountId ,amount);

        ResMessage<Boolean> res_message = new ResMessage<>(req_id,flag);

        //System.out.println("Transfer");

        if( flag ) {

            float bal_after_f = this.Bank.balance(accountId);
            float bal_after_t = this.Bank.balance(to_accountId);

            BankUpdate su  = new BankUpdate(accountId, to_accountId, bal_after_f, bal_after_t, req_id);
            MyPair<Address,BankUpdate> sump = new MyPair<>(a, su);


            // CRITICAL ZONE BEGIN
            act_req_lock.lock();

            last_msg_seen++;

            history.add(su);

            if( history.size() == MAX_HIST_SIZE ) history.removeFirst();

            unanswered_reqs.add(sump);

            act_req_lock.unlock();
            // CRITICAL ZONE END

            // Notify everyone else about the Movement before replying
            SpreadMessage su_msg = new SpreadMessage();

            su_msg.setData(s.encode(sump));
            su_msg.setType(state_update);
            su_msg.setReliable();
            su_msg.setSafe(); // VERY IMPORTANT
            su_msg.addGroup(sg);

            try {
                sconn.multicast(su_msg);
            } catch (SpreadException e) {
                e.printStackTrace();
            }
        }
        else {
            // If the operation is a failure, we don't need to notify everyone else,
            // so we can reply to the request right away
            this.ms.sendAsync(a, "transfer-res", this.s.encode(res_message));
        }
    }

    // State Persistence Shenanigans

    private void read_state() throws FileNotFoundException
    {
        FileInputStream fis = new FileInputStream("save/server_state_" + this.port + ".obj");

        try{
            ObjectInputStream in = new ObjectInputStream(fis);
            this.Bank = (Bank) in.readObject();
            this.last_msg_seen= (int) in.readObject();
            System.out.println("Last request received before death: " + last_msg_seen);
            in.close();
            System.out.println("Read previous state");
        }
        catch (IOException | ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    private void store_state()
    {
        try{
            System.out.println("Last update: " + history.size());
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("save/server_state_" + this.port + ".obj"));
            out.writeObject(this.Bank);
            out.writeObject(this.last_msg_seen);
            out.flush();
            out.close();
            System.out.println("Stored my own state before death");
        }
        catch(IOException e){
            e.printStackTrace();
        }


    }

    // Atomix Starting Shenanigans

    private void start_atomix()
    {
        if(ms == null)
        {
            ms = new NettyMessagingService("ServerSkeleton", Address.from(REQ_PORT), new MessagingConfig());
            ms.registerHandler("movement-req", (a,m) -> { movement_handler(a,m);}, es);
            ms.registerHandler("balance-req",  (a,m) -> { balance_handler(a,m); }, es);
            ms.registerHandler("transfer-req", (a,m) -> { transfer_handler(a,m);}, es);
            ms.start();
        }
    }

    private void stop_atomix()
    {
        if(ms != null)
        {
            ms.unregisterHandler("movement-req");
            ms.unregisterHandler("balance-req" );
            ms.unregisterHandler("transfer-req");
            ms.stop();
        }
    }

}