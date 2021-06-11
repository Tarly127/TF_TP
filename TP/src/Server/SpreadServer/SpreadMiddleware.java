package Server.SpreadServer;

import Bank.src.Account;
import Bank.src.Bank;
import Other.Election;
import Other.Transaction;
import Server.DataPackets.LockFreeAccount;
import Server.DataPackets.LockFreeBank;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import spread.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;


// "Box" Containing the Full Bank Data
class FullBankTransfer
{
    private LockFreeBank bank;
    private long last_msg_seen;

    FullBankTransfer(Bank bank, long last_msg_seen) {
        this.bank = bank.makeLockFree();
        this.last_msg_seen = last_msg_seen;
    }

    LockFreeBank getBank() {
        return bank;
    }

    long getLast_msg_seen() {
        return last_msg_seen;
    }
}

// Simple Generic Tuple with two elements
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

public class SpreadMiddleware {

    private static final short state_transfer_partial = 9001;
    private static final short state_transfer_full    = 9002;
    private static final short state_transfer_request = 9003;
    private static final short state_update           = 9004;
    private static final short leader_election        = 9005;
    private static final int   MAX_HIST_SIZE          = 100;
    private static final int   TOTAL_MEMBERS = 3;

    private SpreadConnection sconn;                                     // Spread connection to daemon
    private SpreadGroup sg;                                             // Communication Group I'm in
    private Serializer s;                                               // Atomix Serializer to serialize messages
    private Bank Bank;                                                  // I have to have a pointer to the same Bank
                                                                        // the Skeleton has so I can update it
    private Election e;                                                 // Leader election after partition

    // Using Atomic variables, and their associated atomic operations will decrease the number of calls to the kernel
    // for mutexes
    private AtomicLong                         last_msg_seen;           // last message received from client (or leader)
    private AtomicLong                         transactions_completed;  // last operation finished

    private boolean                            is_leader;               // whether I'm the leader or not
    private boolean                            is_utd;                  // whether I'm up to date or not
    private boolean                            no_st;                   // whether I've had a state transfer or not
    private boolean                            election_decided;        // whether election is decided or not

    private LinkedList<Transaction>            history;                 // history of the last MAX_HIST_SIZE requests
    private LinkedList<SpreadGroup>            leader_queue;            // queue of processes that may be the leader
    private LinkedList<String>                 partner_queue;           // queue of processes in group
    private LinkedList<SpreadMessage>          req_queue;               // queue of requests to be sent to backups

    private ReentrantLock                      queue_lock;              // lock associated with req_queue
    private ReentrantLock                      history_lock;            // lock associated with history
    private ReentrantLock                      unanswered_lock;         // lock associated with unanswered_reqs

    private Map<Long, CompletableFuture<Long>> unanswered_reqs;         // store the requests in transit between
                                                                        // primary and the backups, so the answer
                                                                        // may later be sent to the client

    private CompletableFuture<Long> leader;                             // will be completed when I become the leader
    private CompletableFuture<Long> updated;                            // will be completed when I'm up to date





    public SpreadMiddleware(Bank bank, int port, int connect_to, long lms,
                            CompletableFuture<Long> in_leader,
                            CompletableFuture<Long> in_updated)
    {
        try
        {
            // *** For debugging purposes only ***
            //ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
            //ses.scheduleAtFixedRate(() -> {
            //    if(is_leader)
            //        System.out.println("I'm the leader!");
            //
            //    System.out.println(last_msg_seen.longValue() + "::" + transactions_completed.longValue());
            //    System.out.println("Number of requests enqueued: " + unanswered_reqs.size());
            //
            //}, 0, 10, TimeUnit.SECONDS);


            this.history                = new LinkedList<>();
            this.leader_queue           = new LinkedList<>();
            this.partner_queue          = new LinkedList<>();
            this.req_queue              = new LinkedList<>();
            this.unanswered_reqs        = new HashMap<>();
            this.leader                 = in_leader;
            this.updated                = in_updated;
            this.election_decided       = true;
            this.Bank                   = bank;
            this.last_msg_seen          = new AtomicLong(lms);
            this.transactions_completed = new AtomicLong(lms);

            this.queue_lock             = new ReentrantLock();
            this.history_lock           = new ReentrantLock();
            this.unanswered_lock        = new ReentrantLock();

            this.s    = Serializer.builder().withTypes(
                                            FullBankTransfer.class,
                                            Bank.class,
                                            Account.class,
                                            Transaction.class,
                                            LocalDateTime.class,
                                            LockFreeBank.class,
                                            LockFreeAccount.class,
                                            MyPair.class,
                                            Address.class
                                        ).build();

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    System.out.println("Req queue size: " + req_queue.size());
                    System.out.println("Unanswered req size: " + unanswered_reqs.size());
                }
            });

            this.sconn = new SpreadConnection();
            this.sconn.connect(InetAddress.getByName("localhost"), connect_to,
                    "server:" + port, false, true);

            this.sg = new SpreadGroup();
            this.sg.join(this.sconn, "servers");

            // when I join the Group, i will send a message to ask others to update my state
            SpreadMessage str = new SpreadMessage();

            str.setData(s.encode(last_msg_seen.longValue()));
            str.setType(state_transfer_request);
            str.setReliable();
            str.setSafe();
            str.addGroup(this.sg);

            try
            {
                sconn.multicast(str);
            }
            catch (SpreadException e)
            {
                e.printStackTrace();
            }

            this.is_utd = false;

            this.sconn.add(new AdvancedMessageListener() {
                @Override
                // Deal with regular messages circulating in the server group(s)
                public void regularMessageReceived(SpreadMessage msg)
                {
                    switch ( msg.getType() )
                    {
                        case state_transfer_full:
                        {
                            // if anyone else sends a state transfer msg, it's ignored
                            if( !is_utd ) {
                                System.out.println("Receiving full state transfer");
                                no_st = false;

                                FullBankTransfer state_tr = s.decode(msg.getData());

                                last_msg_seen          = new AtomicLong(state_tr.getLast_msg_seen());
                                transactions_completed = new AtomicLong(state_tr.getLast_msg_seen());

                                Bank.update(state_tr.getBank());
                                is_utd = true;

                                updated.complete(last_msg_seen.longValue());
                            }
                            break;
                        }
                        case state_transfer_partial:
                        {
                            if( !is_utd )
                            {
                                System.out.println("Receiving partial state transfer");
                                no_st = false;

                                List<Transaction> state_tr = s.decode(msg.getData());

                                // update_backups state with movements made previous to the server joining the group
                                for (Transaction t : state_tr)
                                {
                                    update_bank(t);

                                    last_msg_seen.incrementAndGet();
                                    transactions_completed.incrementAndGet();
                                }

                                is_utd = true;
                                updated.complete(last_msg_seen.longValue());

                            }
                            break;
                        }
                        case state_transfer_request:
                        {
                            System.out.println("History: " + history.size());
                            // we should be careful not to process requests like these if we're not up to date
                            // or if we're the only member of the comm group, because that means we're the ones
                            // who sent it
                            if(msg.getGroups().length == 1 || (is_utd && (history.size() == MAX_HIST_SIZE || !no_st)))
                            {
                                long requests_processed_before_fail = s.decode(msg.getData());

                                // If the last request the new member saw was a recent one, we can transfer a small subset
                                // of requests (movements only, as they're the only ones that alter state) to him instead of
                                // the full bank object

                                //System.out.println(requests_processed_before_fail + " - " + last_msg_seen.longValue
                                // ());

                                if ( last_msg_seen.longValue() - requests_processed_before_fail <= history.size()
                                        && requests_processed_before_fail <= last_msg_seen.longValue())
                                {
                                    List<Transaction> payload = new ArrayList<>();

                                    for (long i = history.size() - (last_msg_seen.longValue() -
                                            requests_processed_before_fail); i < history.size(); i++) {
                                        payload.add(history.get((int)i));
                                    }

                                    // Send state over
                                    SpreadMessage state = new SpreadMessage();

                                    state.setData(s.encode(payload));
                                    state.setType(state_transfer_partial);
                                    state.setReliable();
                                    state.setSafe();

                                    // Set the message group to a singleton containing only the new member
                                    state.addGroup(msg.getSender());

                                    try {
                                        sconn.multicast(state);
                                    } catch (SpreadException e) {
                                        e.printStackTrace();
                                    }

                                }
                                // if it isn't, we have no choice but to send the entire bank object over
                                else
                                {
                                    FullBankTransfer payload = new FullBankTransfer(Bank, last_msg_seen.longValue());

                                    // Send state over
                                    SpreadMessage state = new SpreadMessage();

                                    state.setData(s.encode(payload));
                                    state.setType(state_transfer_full);
                                    state.setReliable();
                                    state.setSafe();

                                    // Set the message group to a singleton containing only the new member
                                    state.addGroup(msg.getSender());

                                    try {
                                        sconn.multicast(state);
                                    } catch (SpreadException e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                            break;
                        }
                        case state_update:
                        {
                            // state updates only matter if they come from the leader and I'm not it. Also, if election was already decided
                            if( !is_leader && election_decided)
                            {
                                // If I'm not the leader, I don't have to worry about concurrency control

                                is_utd = false; // just a precaution

                                MyPair<Address, Transaction> su = s.decode(msg.getData());

                                update_bank(su.getY());

                                add_to_history(su.getY());

                                is_utd = true;

                                last_msg_seen.incrementAndGet();
                                transactions_completed.incrementAndGet();

                                System.out.println("Leader informed me of a new client request");
                            }
                            // if I am, we'll do something different...
                            else
                            {
                                MyPair<Address, Transaction> su_msg = s.decode(msg.getData());

                                long id = su_msg.getY().getInternal_id();

                                if(unanswered_reqs.containsKey(id))
                                {
                                    unanswered_reqs.get(id).complete(id);
                                    unanswered_reqs.remove(id);
                                }

                                //System.out.println("Received my own status update_backups, so I can answer the client");
                            }
                            break;
                        }
                        case leader_election:
                        {
                            election_decided = false;
                            MyPair<Boolean, Long> myPair = s.decode(msg.getData());
                            e.process_candidature(msg.getSender(), myPair.getY(), myPair.getX());

                            if(e.getCompleted_candidatures() == e.getPotencial_leaders().size())
                            {
                                System.out.println("--- Election finished, leader is: " + e.getCurr_leader() + " ---");
                                if(e.getCurr_leader().equals(sconn.getPrivateGroup().toString()))
                                {   // I'm leader

                                    is_leader = true;
                                    System.out.println("Message seen when I became leader(after transitional view:) " +
                                            last_msg_seen.longValue() + " , " + transactions_completed.longValue());
                                    leader.complete(last_msg_seen.longValue());

                                    election_decided = true;

                                    if(req_queue.size() > 0)
                                    {
                                        if( last_msg_seen.longValue() == transactions_completed.longValue())
                                        {
                                            // if there are none, empty out the queue
                                            try
                                            {
                                                queue_lock.lock();

                                                while( !req_queue.isEmpty() )
                                                {
                                                    sconn.multicast(req_queue.removeFirst());
                                                }

                                                queue_lock.unlock();
                                            }
                                            catch (SpreadException e)
                                            {
                                                e.printStackTrace();
                                            }
                                        }
                                    }
                                }
                                else
                                {   // I'm not leader
                                    if(partner_queue.size() < e.getCompleted_candidatures())
                                    {   // I'm back after merge, i will send a message to ask others to update my state

                                        partner_queue.clear();
                                        partner_queue.addAll(e.getPotencial_leaders());

                                        str.setData(s.encode(last_msg_seen.longValue()));
                                        str.setType(state_transfer_request);
                                        str.setReliable();
                                        str.setSafe();
                                        str.addGroup(sg);

                                        try
                                        {
                                            sconn.multicast(str);
                                        }
                                        catch (SpreadException e)
                                        {
                                            e.printStackTrace();
                                        }
                                    }
                                    election_decided = true;
                                }
                            }
                        }
                    }
                }

                @Override
                // Deal with new members in the group (update_backups their state to the current one)
                // TODO deal with EVS? Maybe, we'll see
                public void membershipMessageReceived(SpreadMessage msg)
                {
                    MembershipInfo info = msg.getMembershipInfo();

                    if( info.isRegularMembership() ) {
                        if ( info.isCausedByJoin() && election_decided)
                        {   // store the list of servers in this group
                            SpreadGroup[] members = info.getMembers();
                            partner_queue.clear();
                            partner_queue.addAll(Collections.singleton(Arrays.toString(members)));

                            if( leader_queue.size() == 0 )
                            {   //we we're the ones joining

                                // store the list of servers who came before us
                                // one of these will be the leader, but we don't
                                // care which one
                                SpreadGroup[] sgs = msg.getGroups();
                                leader_queue.addAll(Arrays.asList(sgs));

                                is_leader = leader_queue.size() == 1;

                                // If I'm the leader, I'll complete the future
                                // thus informing the ServerSkeleton we are the Leader
                                if( is_leader )
                                {
                                    System.out.println("Message seen when I became leader: " +
                                            last_msg_seen.longValue() + " , " + transactions_completed.longValue());
                                    leader.complete(last_msg_seen.longValue());
                                }

                            }
                            // we don't care if others join after us
                        }
                        else if(info.isTransition() && is_leader)
                        {
                            is_leader = false;
                            System.out.println("Message seen when I recieved transitional view: " +
                                    last_msg_seen.longValue() + " , " + transactions_completed.longValue());
                            leader.complete(last_msg_seen.longValue());
                        }
                        else if(info.isCausedByNetwork() && election_decided)
                        {   // leader election
                            SpreadGroup[] members = msg.getGroups();
                            System.out.println("Members in this partition after transitional view: " + Arrays.toString(members));

                            if(members.length > TOTAL_MEMBERS/2)
                            {   // majority group
                                e = new Election(members);

                                MyPair<Boolean, Long> myPair = new MyPair<>(is_leader, last_msg_seen.longValue());
                                str.setData(s.encode(myPair));
                                str.setType(leader_election);
                                str.setReliable();
                                str.setSafe();
                                str.addGroup(sg);
                                try
                                {
                                    sconn.multicast(str);
                                }
                                catch (SpreadException e)
                                {
                                    e.printStackTrace();
                                }
                            }

                            else{
                                if(is_leader)
                                {   // Not a member of majority and leader
                                    is_leader = false;
                                    System.out.println("Message seen when I left as leader: " +
                                            last_msg_seen.longValue() + " , " + transactions_completed.longValue());
                                    leader.complete(last_msg_seen.longValue());
                                }
                                is_utd = false;
                            }
                        }
                        else if((info.isCausedByDisconnect() || info.isCausedByLeave()) && election_decided)
                        {
                            // If someone else left, we need to check if it's out turn to be the leader

                            leader_queue.remove(info.getLeft());
                            partner_queue.remove(info.getLeft().toString());

                            is_leader = leader_queue.size() == 1;

                            // If I'm the leader, I'll start receiving requests from client
                            if ( is_leader )
                            {
                                System.out.println("Message seen when I became leader: " +
                                        last_msg_seen.longValue() + " , " + transactions_completed.longValue());
                                leader.complete(last_msg_seen.longValue());
                            }
                        }
                    }
                }
            });
        }
        catch(SpreadException | UnknownHostException e)
        {
            e.printStackTrace();
        }
    }

    // Public methods, ServerSkeleton needs access to all of these

    public boolean is_ready ()
    {
        // A Server is only ready to receive messages from the various clients when it's up to date and is also the
        // leader, although being the leader usually means being up to date
        return this.is_leader && this.is_utd;
    }

    public long getTransactions_completed()
    {
        // There may be some transactions that have not been completed yet, but have been marked as such (they're
        // between the point where we update transactions_completed and the clearing of the queue), and, as such, we
        // have to account for that difference carefully
        return transactions_completed.longValue() - req_queue.size();
    }

    // update all backups about the new transaction
    public void update_backups(Address a, Transaction t, CompletableFuture<Long> cf)
    {
        // get the internal ID (an atomic operation)
        long my_lms = this.last_msg_seen.getAndIncrement();

        // set the Transaction's internal ID
        t.setInternal_id(my_lms);

        // update the history
        add_to_history(t);

        // update the unanswered requests list
        add_to_ureqs(cf, my_lms);

        // create the new packet to be sent, plus the new message
        MyPair<Address, Transaction> sump = new MyPair<>(a, t);

        SpreadMessage su_msg = new SpreadMessage();
        su_msg.setData(s.encode(sump));
        su_msg.setType(state_update);
        su_msg.setReliable();
        su_msg.setSafe();
        su_msg.addGroup(sg);


        // increment the number of completed transactions
        transactions_completed.incrementAndGet();

        // Add the new request to the request queue
        add_to_req_queue(su_msg);


        // we only enter this chunk if we see there are no concurrent requests happening
        // that means checking if the number of messages seen equals the number of messages processed, which only
        // happens when there are no more threads active at the same time as this one
        if( last_msg_seen.longValue() == transactions_completed.longValue() && is_leader )
        {
            // if there are none, empty out the queue
            try
            {
                queue_lock.lock();

                while( !req_queue.isEmpty() )
                {
                    sconn.multicast(req_queue.removeFirst());
                }

                queue_lock.unlock();
            }
            catch (SpreadException e)
            {
                e.printStackTrace();
            }
        }
    }


    // Private methods

    private void update_bank(Transaction t)
    {
        switch ( t.getType() )
        {
            case Transaction.MOVEMENT :
            {
                Bank.set(t.getAccount_to(), t.getAmount_after_to());
                break;
            }
            case Transaction.INTEREST:
            {
                Bank.interest();
                break;
            }
            case Transaction.TRANSFER:
            {
                this.Bank.set(t.getAccount_to(),   t.getAmount_after_to());
                this.Bank.set(t.getAccount_from(), t.getAmount_after_from());
                break;
            }
        }
    }

    private void add_to_history(Transaction t)
    {

        this.history_lock.lock();

        if ( this.history.size() == MAX_HIST_SIZE )
        {
            this.history.removeFirst();
        }

        this.history.addLast(t);

        this.history_lock.unlock();
    }

    private void add_to_ureqs(CompletableFuture<Long> cf, long my_lms)
    {
        unanswered_lock.lock();

        unanswered_reqs.put(my_lms, cf);

        unanswered_lock.unlock();
    }

    private void add_to_req_queue(SpreadMessage su_msg)
    {
        this.queue_lock.lock();

        req_queue.addLast(su_msg);

        this.queue_lock.unlock();
    }

    public void set_fut_leader(CompletableFuture<Long> fl)
    {
        this.leader = fl;
    }
}
