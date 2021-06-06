package Server;

import Bank.src.Bank;
import Messages.ReqMessage;
import Messages.ResMessage;
import Other.Transaction;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import java.io.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;


public class ServerSkeleton{

    private static final int   REQ_PORT   = 10000;
    private static final float INTEREST   = 0.05f;
    private static final int   NO_THREADS = 1;

    private int port;
    private long last_msg_seen;
    private CompletableFuture<Long> fut_leader;
    private CompletableFuture<Void>    fut_updated;
    private Serializer s;
    private ScheduledExecutorService ses;
    private ExecutorService es;
    private NettyMessagingService ms;
    private SpreadMiddleware spread_gv;
    private Bank Bank;


    public ServerSkeleton(int port, int connect_to){

        this.port = port;
        this.s    = Serializer.builder().withTypes(ReqMessage.class,
                                                   ResMessage.class,
                                                    Transaction.class,
                LocalDateTime.class
                                        ).build();
        this.es              = Executors.newFixedThreadPool(NO_THREADS);
        this.ses             = Executors.newScheduledThreadPool(1); // can take this out later, only here for debbuging

        this.Bank            = new Bank(ServerSkeleton.INTEREST);
        this.ms              = null;

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
        this.fut_leader  = new CompletableFuture<>();
        this.fut_updated = new CompletableFuture<>();


        // Deal with the Leader
        this.es.submit(() ->
        {
            try{
                last_msg_seen = fut_leader.get();
                if( spread_gv.is_ready() )
                    start_atomix();
            }
            catch (InterruptedException | ExecutionException e)
            {
                e.printStackTrace();
            }

        });

        // Deal with Group Join Update
        this.es.submit(() ->
        {
            try{
                fut_updated.get();

                if ( spread_gv.is_ready() )
                    start_atomix();
            }
            catch (InterruptedException | ExecutionException e)
            {
                e.printStackTrace();
            }

        });

        // Initialize Spread Connection
        this.spread_gv = new SpreadMiddleware(this.Bank, port, connect_to, last_msg_seen, fut_leader, fut_updated);


        // So it writes state when killed
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                store_state();
            }
        });

        ses.scheduleAtFixedRate(() -> {
            if(spread_gv.is_ready())
                System.out.println("I'm the leader!");
            else
                System.out.println("Someone else is the leader");
//
            System.out.println("Account 1 balance: " + Bank.balance(1) + "$");
            System.out.println("Account 2 balance: " + Bank.balance(2) + "$");
            System.out.println("I've seen " + last_msg_seen + " messages");
        }, 0, 4, TimeUnit.SECONDS);
    }

    public void show(int accountID){
        System.out.println(accountID + " : " + this.Bank.balance(accountID));
    }

    // Atomix Handlers

    private void movement_handler(Address a, byte[] m)
    {

        LocalDateTime now = LocalDateTime.now();

        last_msg_seen++;

        ReqMessage req_msg = this.s.decode(m);

        int accountId = req_msg.getAccountId();
        int req_id    = req_msg.getReqId();
        float amount  = req_msg.getAmount();

        boolean flag = this.Bank.movement(accountId,amount);

        float bal_after = this.Bank.balance(accountId);

        ResMessage<Boolean> res_message = new ResMessage<>(req_id,flag);

        //System.out.println("Movement");

        Transaction t = new Transaction(now, accountId, amount, bal_after, req_id, last_msg_seen);

        if( flag )
        {

            CompletableFuture<Long> cf = new CompletableFuture<>();

            this.es.submit( () -> {
               try{
                   cf.get();

                   ms.sendAsync(a, "movement-res", s.encode(res_message));
               }catch (InterruptedException | ExecutionException e)
               {
                   e.printStackTrace();
               }
            });


            this.spread_gv.update_backups(a, t, cf);

        }
        else {
            // If the operation is a failure, we don't need to notify everyone else,
            // so we can reply to the request right away
            this.ms.sendAsync(a, "movement-res", this.s.encode(res_message));
        }
    }

    private void balance_handler (Address a, byte[] m)
    {
        ReqMessage req_msg = this.s.decode(m);

        int accountId = req_msg.getAccountId();
        int req_id    = req_msg.getReqId();

        float cap = this.Bank.balance(accountId);

        ResMessage<Float> res_message = new ResMessage<>(req_id,cap);

        // No point in warning everyone else about a BALANCE request, reply right away
        this.ms.sendAsync(a, "balance-res", this.s.encode(res_message));

    }

    private void transfer_handler(Address a, byte[] m)
    {
        LocalDateTime now = LocalDateTime.now();

        last_msg_seen++;

        ReqMessage req_msg = this.s.decode(m);

        int accountId    = req_msg.getAccountId();
        int to_accountId = req_msg.getToAccountId();
        int req_id       = req_msg.getReqId();
        float amount    = req_msg.getAmount();

        boolean flag = this.Bank.transfer(accountId, to_accountId, amount);

        float bal_after_to = this.Bank.balance(to_accountId);
        float bal_after_from = this.Bank.balance(accountId);

        ResMessage<Boolean> res_message = new ResMessage<>(req_id,flag);

        //System.out.println("Transfer");

        Transaction t = new Transaction(now, to_accountId, accountId, amount, bal_after_to, bal_after_from, req_id,
                last_msg_seen);

        if( flag )
        {

            CompletableFuture<Long> cf = new CompletableFuture<>();

            this.es.submit( () -> {
                try{
                    cf.get();

                    this.ms.sendAsync(a, "transfer-res", this.s.encode(res_message));
                }catch (InterruptedException | ExecutionException e )
                {
                    e.printStackTrace();
                }
            });


            this.spread_gv.update_backups(a, t, cf);

        }
        else
        {
            // If the operation is a failure, we don't need to notify everyone else,
            // so we can reply to the request right away
            this.ms.sendAsync(a, "movement-res", this.s.encode(res_message));
        }
    }

    private void history_handler (Address a, byte[] m)
    {
        ReqMessage req_msg = this.s.decode(m);

        int accountId = req_msg.getAccountId();
        int req_id = req_msg.getReqId();

        List<Transaction> lt = this.Bank.history(accountId);

        ResMessage<List<Transaction>> res_message = new ResMessage<>(req_id, lt);

        // No point in warning everyone else about a HISTORY request, reply right away
        this.ms.sendAsync(a, "history-res", this.s.encode(res_message));
    }

    private void interest_handler(Address a, byte[] m)
    {

        LocalDateTime now = LocalDateTime.now();

        last_msg_seen++;

        ReqMessage req_msg = this.s.decode(m);

        int req_id       = req_msg.getReqId();

        this.Bank.interest();

        ResMessage<Void> res_message = new ResMessage<>(req_id);

        //System.out.println("Interest");

        Transaction t = new Transaction(now, req_id, last_msg_seen);

        CompletableFuture<Long> cf = new CompletableFuture<>();

        this.es.submit( () -> {
            try{
                cf.get();

                this.ms.sendAsync(a, "interest-res", this.s.encode(res_message));

            }
            catch (InterruptedException | ExecutionException e )
            {
                e.printStackTrace();
            }
        });

        this.spread_gv.update_backups(a, t, cf);
    }


    // Very, very Simple Data Persistence

    private void read_state() throws FileNotFoundException
    {
        FileInputStream fis = new FileInputStream("save/server_state_" + this.port + ".obj");

        try{
            ObjectInputStream in = new ObjectInputStream(fis);
            this.Bank = (Bank) in.readObject();
            this.last_msg_seen= (long) in.readObject();
            //System.out.println("Last request received before death: " + last_msg_seen);
            in.close();
            //System.out.println("Read previous state");
        }
        catch (IOException | ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    private void store_state()
    {
        try{
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("save/server_state_" + this.port + ".obj"));
            out.writeObject(this.Bank);
            out.writeObject(this.spread_gv.getLast_msg_seen());
            out.flush();
            out.close();
            System.out.println("Stored my own state before death");
        }
        catch(IOException e){
            e.printStackTrace();
        }


    }

    // Starting/Stoping Atomix

    private void start_atomix()
    {
        if(ms == null)
        {
            ms = new NettyMessagingService("ServerSkeleton", Address.from(REQ_PORT), new MessagingConfig());

            ms.registerHandler("movement-req", (a,m) -> { movement_handler(a,m); }, es);
            ms.registerHandler("balance-req",  (a,m) -> { balance_handler (a,m); }, es);
            ms.registerHandler("transfer-req", (a,m) -> { transfer_handler(a,m); }, es);
            ms.registerHandler("history-req",  (a,m) -> { history_handler (a,m); }, es);
            ms.registerHandler("interest-req", (a,m) -> { interest_handler(a,m); }, es);

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
            ms.unregisterHandler("history-req" );
            ms.unregisterHandler("interest-req");
            ms.stop();
        }
    }

}