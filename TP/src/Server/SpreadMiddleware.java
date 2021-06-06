package Server;

import Bank.include.BankInterface;
import Bank.src.Account;
import Bank.src.Bank;
import Other.Transaction;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import spread.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;


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

    private static final short state_transfer_partial = 9001;     // Guião 3
    private static final short state_transfer_full    = 9002;     // Guião 3
    private static final short state_transfer_request = 9003;     // Guião 3
    private static final short state_update           = 9004;     // Guião 5
    private static final int   MAX_HIST_SIZE          = 100;      // Guião 3

    private SpreadConnection sconn;
    private SpreadGroup sg;

    private Serializer s;

    private boolean                            is_leader;
    private boolean                            is_utd;
    private boolean                            no_st;
    private long                               last_msg_seen;
    private LinkedList<Transaction>            history;
    private LinkedList<SpreadGroup>            leader_queue;

    private Map<Long, CompletableFuture<Long>> unanswered_reqs;
    private CompletableFuture<Long> leader;
    private CompletableFuture<Void>    updated;

    private Bank Bank; // Cringe, but it has to be done!

    public SpreadMiddleware(Bank bank, int port, int connect_to, long lms,
                            CompletableFuture<Long> in_leader,
                            CompletableFuture<Void> in_updated)
    {
        try
        {

            this.history         = new LinkedList<>();
            this.leader_queue    = new LinkedList<>();
            this.unanswered_reqs = new HashMap<>();
            this.leader          = in_leader;
            this.updated         = in_updated;
            this.Bank            = bank;
            this.last_msg_seen   = lms;

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
            this.sconn = new SpreadConnection();
            this.sconn.connect(InetAddress.getByName("localhost"), connect_to, "server:" + port, false, true);

            this.sg = new SpreadGroup();
            this.sg.join(this.sconn, "servers");

            // when I join the Group, i will send a message to ask others to update_backups my state
            SpreadMessage str = new SpreadMessage();

            str.setData(s.encode(last_msg_seen));
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
                    switch (msg.getType())
                    {
                        case state_transfer_full:
                        {
                            // if anyone else sends a state transfer msg, it's ignored
                            if( !is_utd ) {
                                System.out.println("Receiving full state transfer");
                                no_st = false;

                                FullBankTransfer state_tr = s.decode(msg.getData());

                                last_msg_seen = state_tr.getLast_msg_seen(); //TODO
                                Bank.update(state_tr.getBank());
                                is_utd = true;

                                updated.complete(null);
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
                                }

                                is_utd = true;
                                updated.complete(null);

                            }
                            break;
                        }
                        case state_transfer_request: //TODO
                        {
                            // we should be careful not to process requests like these if we're not up to date
                            // or if we're the only member of the comm group, because that means we're the ones
                            // who sent it
                            if(msg.getGroups().length == 1 || (is_utd && (history.size() == MAX_HIST_SIZE || !no_st)))
                            {
                                long requests_processed_before_fail = s.decode(msg.getData());

                                // If the last request the new member saw was a recent one, we can transfer a small subset
                                // of requests (movements only, as they're the only ones that alter state) to him instead of
                                // the full bank object

                                System.out.println(requests_processed_before_fail + " - " + last_msg_seen);

                                if ( last_msg_seen - requests_processed_before_fail <= history.size() && requests_processed_before_fail <= last_msg_seen)
                                {
                                    List<Transaction> payload = new ArrayList<>();

                                    for (long i = history.size() - (last_msg_seen - requests_processed_before_fail); i < history.size(); i++) {
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
                                    FullBankTransfer payload = new FullBankTransfer((Bank)Bank, last_msg_seen);

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
                        case state_update: //TODO
                        {
                            // state updates only matter if they come from the leader and I'm not it.
                            // TODO
                            if( !is_leader )
                            {
                                // If I'm not the leader, I don't have to worry about concurrency control

                                is_utd = false; // just a precaution

                                MyPair<Address, Transaction> su = s.decode(msg.getData());

                                update_bank(su.getY());

                                is_utd = true;

                                last_msg_seen++;

                                //System.out.println("Leader informed me of a new client request");
                            }
                            // if I am, we'll do something different...
                            else
                            {
                                MyPair<Address, Transaction> su_msg = s.decode(msg.getData());

                                long id = su_msg.getY().getInternal_id();

                                unanswered_reqs.get(id).complete(id);

                                unanswered_reqs.remove(id);

                                //System.out.println("Received my own status update_backups, so I can answer the client");
                            }
                            break;
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
                        if ( info.isCausedByJoin() ) {
                            if( leader_queue.size() == 0 ) { //we we're the ones joining

                                // store the list of servers who came before us
                                // one of these will be the leader, but we don't
                                // care which one
                                SpreadGroup[] sgs = msg.getGroups();
                                leader_queue.addAll(Arrays.asList(sgs));

                                is_leader = leader_queue.size() == 1;

                                // If I'm the leader, I'll complete the future
                                // thus informing the ServerSkeleton we are the Leader
                                if( is_leader )

                                    leader.complete(last_msg_seen);

                            }
                            // we don't care if others join after us
                        }
                        else if(info.isCausedByDisconnect() || info.isCausedByLeave())
                        {
                            leader_queue.remove(info.getLeft());

                            is_leader = leader_queue.size() == 1;

                            // If I'm the leader, I'll start receiving requests from client
                            if(is_leader)

                                leader.complete(last_msg_seen);
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

    // update_backups all backups about the new transaction
    public void update_backups(Address a, Transaction t, CompletableFuture<Long> cf)
    {
        MyPair<Address, Transaction> sump = new MyPair<>(a, t);

        last_msg_seen++;

        push_to_history(t);

        unanswered_reqs.put(t.getInternal_id(), cf);

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

    public boolean is_ready ()
    {

        return this.is_leader && this.is_utd;
    }

    private void push_to_history(Transaction t)
    {
        if ( this.history.size() == MAX_HIST_SIZE )
        {
            this.history.removeFirst();
        }

        this.history.addLast(t);
    }

    private void update_bank(Transaction t)
    {
        switch ( t.getType() )
        {
            case Transaction.MOVEMENT :
            {
                //System.out.println("Update from a MOVEMENT");
                Bank.set(t.getAccount_to(), t.getAmount_after_to());
                break;
            }
            case Transaction.INTEREST:
            {
                //System.out.println("Update from INTEREST");
                Bank.interest();
                break;
            }
            case Transaction.TRANSFER:
            {
                System.out.println("Update from TRANSFER");
                this.Bank.set(t.getAccount_to(),   t.getAmount_after_to());
                this.Bank.set(t.getAccount_from(), t.getAmount_after_from());
                break;
            }
        }
    }

    public long getLast_msg_seen()
    {

        return last_msg_seen;
    }
}
