package Server.AtomixServer;

import Bank.src.Bank;
import Messages.ReqMessage;
import Messages.ResMessage;
import Other.Transaction;
import Server.SpreadServer.SpreadMiddleware;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import java.io.*;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;


public class ServerSkeleton{

    private static final int   REQ_PORT   = 10000;
    private static final float INTEREST   = 0.05f;
    private static final int   NO_THREADS = 8;

    private int port;
    private long last_msg_seen;
    private CompletableFuture<Long> fut_leader;
    private CompletableFuture<Long> fut_updated;
    private Serializer s;
    private ScheduledExecutorService ses;
    private ExecutorService es;
    private NettyMessagingService ms;
    private SpreadMiddleware spread_gv;
    private Bank Bank;

    private AtomicLong reqs_completed;  // requests completed

    public ServerSkeleton(int port, int connect_to)
    {

        this.port = port;
        this.s    = Serializer.builder().withTypes(
                                            ReqMessage.class,
                                            ResMessage.class,
                                            Transaction.class,
                                            LocalDateTime.class
                                        ).build();
        this.es              = Executors.newFixedThreadPool(NO_THREADS);
        this.ses             = Executors.newScheduledThreadPool(1); // can take this out later, only here for debbuging

        this.Bank            = new Bank(ServerSkeleton.INTEREST);
        this.ms              = null;

        this.reqs_completed = new AtomicLong(0);

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
            listen_for_leader();
        });

        // Deal with Group Join Update
        this.es.submit(() ->
        {
            try
            {
                last_msg_seen = fut_updated.get();
                System.out.println("Updated: " + spread_gv.is_ready());


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
        // Very simple way to have persistence of state
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                store_state();
            }
        });
/*
       ses.scheduleAtFixedRate(()->{
            System.out.println("Débito: " + reqs_completed.longValue() + " pedidos por segundo");
        },0,1, TimeUnit.SECONDS);

        ses.scheduleAtFixedRate(() -> {
            //if(spread_gv.is_ready())
            //    System.out.println("I'm the leader!");
            //else
            //    System.out.println("Someone else is the leader");
            System.out.println("Account 1 balance: " + Bank.balance(1) + "$");
            //System.out.println("Account 2 balance: " + Bank.balance(2) + "$");
            //System.out.println("I've seen " + last_msg_seen + " messages");
        }, 0, 10, TimeUnit.SECONDS); */
    }

    public void show(int accountID){
        System.out.println(accountID + " : " + this.Bank.balance(accountID));
    }

    // Atomix Handlers

    private void movement_handler(Address a, byte[] m)
    {

        LocalDateTime start = LocalDateTime.now();

        ReqMessage req_msg = this.s.decode(m);

        int accountId = req_msg.getAccountId();
        int req_id    = req_msg.getReqId();
        float amount  = req_msg.getAmount();

        boolean flag = this.Bank.movement(accountId,amount);

        float bal_after = this.Bank.balance(accountId);

        ResMessage<Boolean> res_message = new ResMessage<>(req_id,flag);

        Transaction t = new Transaction(start, accountId, amount, bal_after, req_id, -1);

        if( flag )
        {

            CompletableFuture<Long> cf = new CompletableFuture<>();

            this.es.submit( () ->
            {
               try
               {
                   cf.get();

                   LocalDateTime end = LocalDateTime.now();
                   LocalDateTime difference = LocalDateTime.from(start);
                   System.out.println("--- Duration of movement(success): " + difference.until(end, ChronoUnit.MILLIS) + " seconds ---");

                   ms.sendAsync(a, "movement-res", s.encode(res_message));

                   reqs_completed.incrementAndGet();
               }
               catch (InterruptedException | ExecutionException e)
               {
                   e.printStackTrace();
               }
            });


            this.spread_gv.update_backups(a, t, cf);

        }
        else {

            LocalDateTime end = LocalDateTime.now();
            LocalDateTime difference = LocalDateTime.from(start);
            System.out.println("--- Duration of movement(failure): " + difference.until(end, ChronoUnit.MILLIS) + " seconds ---");

            // If the operation is a failure, we don't need to notify everyone else,
            // so we can reply to the request right away
            this.ms.sendAsync(a, "movement-res", this.s.encode(res_message));
        }
    }

    private void balance_handler (Address a, byte[] m)
    {
        LocalDateTime start = LocalDateTime.now();

        ReqMessage req_msg = this.s.decode(m);

        int accountId = req_msg.getAccountId();
        int req_id    = req_msg.getReqId();

        float cap = this.Bank.balance(accountId);

        ResMessage<Float> res_message = new ResMessage<>(req_id,cap);

        LocalDateTime end = LocalDateTime.now();
        LocalDateTime difference = LocalDateTime.from(start);
        System.out.println("--- Duration of balance: " + difference.until(end, ChronoUnit.MILLIS) + " seconds ---");

        // No point in warning everyone else about a BALANCE request, reply right away
        this.ms.sendAsync(a, "balance-res", this.s.encode(res_message));

        reqs_completed.incrementAndGet();

    }

    private void transfer_handler(Address a, byte[] m)
    {
        LocalDateTime start = LocalDateTime.now();

        ReqMessage req_msg = this.s.decode(m);

        int accountId    = req_msg.getAccountId();
        int to_accountId = req_msg.getToAccountId();
        int req_id       = req_msg.getReqId();
        float amount    = req_msg.getAmount();

        boolean flag = this.Bank.transfer(accountId, to_accountId, amount);

        float bal_after_to = this.Bank.balance(to_accountId);
        float bal_after_from = this.Bank.balance(accountId);

        ResMessage<Boolean> res_message = new ResMessage<>(req_id,flag);

        Transaction t = new Transaction(start, to_accountId, accountId, amount, bal_after_to, bal_after_from, req_id,
                -1);

        if( flag )
        {

            CompletableFuture<Long> cf = new CompletableFuture<>();

            this.es.submit( () -> {
                try{
                    cf.get();

                    LocalDateTime end = LocalDateTime.now();
                    LocalDateTime difference = LocalDateTime.from(start);
                    System.out.println("--- Duration of transfer(success): " + difference.until(end, ChronoUnit.MILLIS) + " seconds ---");

                    this.ms.sendAsync(a, "transfer-res", this.s.encode(res_message));

                    reqs_completed.incrementAndGet();

                }catch (InterruptedException | ExecutionException e )
                {
                    e.printStackTrace();
                }
            });


            this.spread_gv.update_backups(a, t, cf);

        }
        else
        {
            LocalDateTime end = LocalDateTime.now();
            LocalDateTime difference = LocalDateTime.from(start);
            System.out.println("--- Duration of transfer(failure): " + difference.until(end, ChronoUnit.MILLIS) + " seconds ---");

            // If the operation is a failure, we don't need to notify everyone else,
            // so we can reply to the request right away
            this.ms.sendAsync(a, "movement-res", this.s.encode(res_message));
        }
    }

    private void history_handler (Address a, byte[] m)
    {
        LocalDateTime start = LocalDateTime.now();
        ReqMessage req_msg = this.s.decode(m);

        int accountId = req_msg.getAccountId();
        int req_id = req_msg.getReqId();

        List<Transaction> lt = this.Bank.history(accountId);

        ResMessage<List<Transaction>> res_message = new ResMessage<>(req_id, lt);

        LocalDateTime end = LocalDateTime.now();
        LocalDateTime difference = LocalDateTime.from(start);
        System.out.println("--- Duration of history: " + difference.until(end, ChronoUnit.MILLIS) + " seconds ---");

        // No point in warning everyone else about a HISTORY request, reply right away
        this.ms.sendAsync(a, "history-res", this.s.encode(res_message));

        reqs_completed.incrementAndGet();

    }

    private void interest_handler(Address a, byte[] m)
    {
        LocalDateTime start = LocalDateTime.now();

        ReqMessage req_msg = this.s.decode(m);

        int req_id       = req_msg.getReqId();

        this.Bank.interest();

        ResMessage<Void> res_message = new ResMessage<>(req_id);

        Transaction t = new Transaction(start, req_id, -1);

        CompletableFuture<Long> cf = new CompletableFuture<>();

        this.es.submit( () -> {
            try{
                cf.get();

                LocalDateTime end = LocalDateTime.now();
                LocalDateTime difference = LocalDateTime.from(start);
                System.out.println("--- Duration of interest: " + difference.until(end, ChronoUnit.MILLIS) + " seconds ---");

                this.ms.sendAsync(a, "interest-res", this.s.encode(res_message));

                reqs_completed.incrementAndGet();

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

        try
        {
            ObjectInputStream in = new ObjectInputStream(fis);

            this.Bank          = (Bank) in.readObject();
            this.last_msg_seen = (long) in.readObject();

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
        try
        {
            // As soon as we get to this point, we shutdown atomix so we won't receive further requests
            stop_atomix();

            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("save/server_state_" + this.port + ".obj"));

            long tc = this.spread_gv.getTransactions_completed();

            out.writeObject(this.Bank);
            out.writeObject(tc);

            out.flush();
            out.close();

            System.out.println("Stored my own state before death (" + tc + ")");
        }
        catch(IOException e){
            e.printStackTrace();
        }


    }

    // Starting/Stoping Atomix

    private void start_atomix()
    {
        if(ms == null) {
            ms = new NettyMessagingService("ServerSkeleton", Address.from("localhost", REQ_PORT), new MessagingConfig());

            ms.registerHandler("movement-req", (a, m) -> {
                movement_handler(a, m);
            }, es);
            ms.registerHandler("balance-req", (a, m) -> {
                balance_handler(a, m);
            }, es);
            ms.registerHandler("transfer-req", (a, m) -> {
                transfer_handler(a, m);
            }, es);
            ms.registerHandler("history-req", (a, m) -> {
                history_handler(a, m);
            }, es);
            ms.registerHandler("interest-req", (a, m) -> {
                interest_handler(a, m);
            }, es);
        }

            ms.start();

            System.out.println("Server listening on port " + REQ_PORT + "...");
    }

    private void stop_atomix()
    {
        if(ms != null)
        {
            ms.stop();
        }
    }

    private void listen_for_leader()
    {
        try
        {
            last_msg_seen = fut_leader.get();
            System.out.println("Leader: " + spread_gv.is_ready());
            if (spread_gv.is_ready())
                start_atomix();

            else
                stop_atomix();

            this.fut_leader = new CompletableFuture<>();

            this.spread_gv.set_fut_leader(this.fut_leader);


        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        listen_for_leader();
    }

}