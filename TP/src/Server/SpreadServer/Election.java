package Server.SpreadServer;

import spread.SpreadGroup;

import java.util.ArrayList;

public  class Election {
    private String starting_leader;
    private String curr_leader;
    private ArrayList<String> potencial_leaders;
    private long max_seq_number;
    private long completed_candidatures;


    public Election(SpreadGroup[] members)
    {
        System.out.println("--- Leader election after partition ---");
        this.curr_leader = null;
        this.max_seq_number = -1;
        this.potencial_leaders  = new ArrayList<>();
        for(SpreadGroup m : members)
        {
            this.potencial_leaders.add(m.toString());
        }
        this.completed_candidatures = 0;
    }

    public void process_candidature(SpreadGroup member, long seq_number, boolean is_leader)
    {
        if(is_leader)
            starting_leader = member.toString();
        System.out.println("- " + member.toString() + " checked " + seq_number + " messages -");
        if(seq_number > max_seq_number || (seq_number == max_seq_number && (!(curr_leader.equals(starting_leader)) && member.toString().compareTo(curr_leader) > 0)) )
        {
            this.max_seq_number = seq_number;
            this.curr_leader = member.toString();
        }
        System.out.println("- After process this candidature, current leader is " + curr_leader + ", he knows " + max_seq_number + " messages -");
        this.completed_candidatures++;
    }

    public long getCompleted_candidatures()
    {
        return this.completed_candidatures;
    }

    public ArrayList<String> getPotencial_leaders()
    {
        return this.potencial_leaders;
    }

    public String getCurr_leader()
    {
        return this.curr_leader;
    }
}
