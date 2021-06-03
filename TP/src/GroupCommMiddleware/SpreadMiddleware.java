package GroupCommMiddleware;

import Bank.include.BankInterface;
import Bank.src.Bank;
import Messages.ReqMessage;
import Messages.ResMessage;
import Other.Transaction;
import Server.QueuedRequest;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import spread.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;


// "Box" Containing the Full Bank Data
class FullBankTransfer
{
    private BankInterface bank;
    private int last_msg_seen;

    FullBankTransfer(BankInterface bank, int last_msg_seen) {
        this.bank = bank;
        this.last_msg_seen = last_msg_seen;
    }

    BankInterface getBank() {
        return bank;
    }

    int getLast_msg_seen() {
        return last_msg_seen;
    }
}

class BankUpdate
{
    private int from;
    private int to;
    private float f_bal;
    private float t_bal;
    private int reqId;
    private int type;

    // transfer constructor
    BankUpdate(int from, int to, float f_bal, float t_bal, int reqId) {
        this.from  = from;
        this.to    = to;
        this.f_bal = f_bal;
        this.t_bal = t_bal;
        this.reqId = reqId;
        this.type  = 6;
    }

    // movement constructor
    BankUpdate(int from, float f_bal, int reqId) {
        this.from  = from;
        this.f_bal = f_bal;
        this.reqId = reqId;

        this.t_bal = -1;
        this.to    = -1;

        this.type  = 1;
    }

    int getFrom() {
        return from;
    }

    int getTo() {
        return to;
    }

    float getF_bal() {
        return f_bal;
    }

    float getT_bal() {
        return t_bal;
    }

    int getReqId() {
        return reqId;
    }

    int getType() {
        return type;
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

    private static final short state_transfer_partial = 2;     // Guião 3
    private static final short state_transfer_full    = 3;     // Guião 3
    private static final short state_transfer_request = 4;     // Guião 3
    private static final short state_update           = 5;     // Guião 5
    private static final int   MAX_HIST_SIZE          = 100;   // Guião 3

    private SpreadConnection sconn;
    private SpreadGroup sg;

    private Serializer s;

    private boolean                     is_leader;
    private boolean                     is_utd;
    private boolean                     no_st;
    private int                         last_msg_seen;
    private LinkedList<BankUpdate>      history;
    private LinkedList<SpreadGroup>     leader_queue;

    public SpreadMiddleware(int port, int connect_to, int last_msg_seen)
    {
        try
        {
            this.history      = new LinkedList<>();
            this.leader_queue = new LinkedList<>();

            this.s    = Serializer.builder().withTypes(
                                                   ReqMessage.class,
                                                   ResMessage.class,
                                                   Bank.class,
                                                   ArrayList.class,
                                                   //QueuedRequest.class,
                                                   FullBankTransfer.class,
                                                   //MyPair.class,
                                                   Integer.class,
                                                   Address.class,
                                                   //BankUpdate.class

                                        ).build();
            this.sconn = new SpreadConnection();
            this.sconn.connect(InetAddress.getByName("localhost"), connect_to, "server:" + port, false, true);

            this.sg = new SpreadGroup();
            this.sg.join(this.sconn, "servers");

            // when I join the Group, i will send a message to ask others to update my state
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
                                Bank = state_tr.getBank();
                                is_utd = true;
                            }
                            break;
                        }
                        case state_transfer_partial: //TODO
                        {
                            if( !is_utd )
                            {
                                System.out.println("Receiving partial state transfer");
                                no_st = false;

                                List<BankUpdate> state_tr = s.decode(msg.getData());

                                // update state with movements made previous to the server joining the group
                                for (BankUpdate r : state_tr)
                                {
                                    if(r.getType() == Transaction.MOVEMENT)
                                    {
                                        Bank.set(r.getFrom(), r.getF_bal());
                                    }
                                    if(r.getType() == Transaction.TRANSFER )
                                    {
                                        Bank.set(r.getFrom(), r.getF_bal());
                                        Bank.set(r.getTo(), r.getT_bal());
                                    }

                                }

                                is_utd = true;

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
                                int requests_processed_before_fail = s.decode(msg.getData());

                                // If the last request the new member saw was a recent one, we can transfer a small subset
                                // of requests (movements only, as they're the only ones that alter state) to him instead of
                                // the full bank object
                                if ( last_msg_seen - requests_processed_before_fail <= history.size())
                                {
                                    List<BankUpdate> payload = new ArrayList<>();

                                    for (int i = history.size() - (last_msg_seen - requests_processed_before_fail); i < history.size(); i++) {
                                        payload.add(history.get(i));
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
                                    FullBankTransfer payload = new FullBankTransfer(Bank, last_msg_seen);

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
                            // state updates only matter if they come from the leader and I'm not it.
                            if( !is_leader )
                            {
                                // If I'm not the leader, I don't have to worry about concurrency control

                                is_utd = false; // just a precaution

                                MyPair<Address, BankUpdate> su = s.decode(msg.getData());

                                Bank.set(su.getY().getFrom(), su.getY().getF_bal());

                                if(su.getY().getType() == Transaction.TRANSFER)
                                    Bank.set(su.getY().getTo(), su.getY().getT_bal());

                                is_utd = true;

                                last_msg_seen++;

                                //System.out.println("Leader informed me of a new client request");
                            }
                            // if I am, we'll do something different...
                            else
                            {
                                HashMap<Integer,Float> su_msg = s.decode(msg.getData());

                                ResMessage<Boolean> res_msg = new ResMessage<>(su_msg.getY().getReqId(), true);

                                String type = su_msg.getY().getType() == Transaction.MOVEMENT ? "movement-res" :
                                        "transfer-res";

                                ms.sendAsync(su_msg.getX(), type, s.encode(res_msg));

                                unanswered_reqs.remove(su_msg);

                                //System.out.println("Received my own status update, so I can answer the client");
                            }
                            break;
                        }
                    }
                }

                @Override
                // Deal with new members in the group (update their state to the current one)
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

                                leader = leader_queue.size() == 1;

                                // If I'm the leader, I'll start receiving requests from client
                                if(is_leader){
                                    start_atomix();
                                }
                            }
                            // we don't care if others join after us
                        }
                        else if(info.isCausedByDisconnect() || info.isCausedByLeave())
                        {
                            leader_queue.remove(info.getLeft());
                            is_leader = leader_queue.size() == 1;

                            // If I'm the leader, I'll start receiving requests from client
                            if(is_leader){
                                start_atomix();
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

    public boolean is_ready ()
    {
        return this.is_leader && this.is_utd;
    }

}
