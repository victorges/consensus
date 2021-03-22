import java.util.ArrayList;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;

public class MaliciousNode implements Node {

    public MaliciousNode(double p_graph, double p_malicious, double p_txDistribution, int numRounds) {
    }

    public void setFollowees(boolean[] followees) {
        return;
    }

    public void setPendingTransaction(Set<Transaction> pendingTransactions) {
        return;
    }

    public Set<Transaction> sendToFollowers() {
        Set<Transaction> txs = new HashSet<Transaction>();
        txs.add(new Transaction(new Random().nextInt()));
        return txs;
    }

    public void receiveFromFollowees(Set<Candidate> candidates) {
        return;
    }
}
