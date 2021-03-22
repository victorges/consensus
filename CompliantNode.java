import java.util.*;

/* CompliantNode refers to a node that follows the rules (not malicious)*/
public class CompliantNode implements Node {

    private static final Comparator<Transaction> s_transactionComparator = Comparator.comparingInt(o -> o.id);

    private final double _p_graph;
    private final double _p_malicious;
    private final double _p_txDistribution;

    private final int _numRounds;
    private int _currRound = 0;

    private int _numNodes;
    private final Set<Integer> _followees = new TreeSet<>();

    private final Set<Transaction> _pendingTransactions = new TreeSet<>(s_transactionComparator);
    private final Set<Transaction> _consensusTransactions = new TreeSet<>(s_transactionComparator);

    private final Set<Integer> _maliciousNodes = new TreeSet<>();
    private int[] _followeeTransactions;

    public CompliantNode(double p_graph, double p_malicious, double p_txDistribution, int numRounds) {
        _p_graph = p_graph;
        _p_malicious = p_malicious;
        _p_txDistribution = p_txDistribution;
        _numRounds = numRounds;
    }

    public void setFollowees(boolean[] followees) {
        _numNodes = followees.length;

        _followees.clear();
        _followeeTransactions = new int[_numNodes];
        for (int i = 0; i < _numNodes; i++) {
            if (followees[i]) {
                _followees.add(i);
            }
        }
    }

    public void setPendingTransaction(Set<Transaction> pendingTransactions) {
        _pendingTransactions.clear();
        _pendingTransactions.addAll(pendingTransactions);
    }

    public Set<Transaction> sendToFollowers() {
        Set<Transaction> toSend = new TreeSet<>(s_transactionComparator);
        toSend.addAll(_consensusTransactions);
        if (_currRound++ >= _numRounds) {
            return toSend;
        }

        toSend.addAll(_pendingTransactions);
        return toSend;
    }

    public void receiveFromFollowees(Set<Candidate> candidates) {
        Map<Transaction, Integer> believers = new TreeMap<>(s_transactionComparator);
        _pendingTransactions.forEach(tx -> believers.put(tx, 1));
        _consensusTransactions.forEach(tx -> believers.put(tx, 1));

        int[] nextFolloweeTransactions = new int[_numNodes];
        for (Candidate candidate : candidates) {
            if (!_followees.contains(candidate.sender)) continue;

            _pendingTransactions.add(candidate.tx);
            if (_maliciousNodes.contains(candidate.sender)) continue;

            nextFolloweeTransactions[candidate.sender]++;

            Integer count = believers.get(candidate.tx);
            believers.put(candidate.tx, count == null ? 1 : count + 1);
        }
        detectMaliciousNodes(nextFolloweeTransactions, candidates);
        _followeeTransactions = nextFolloweeTransactions;

        int threshold = (int)((1+_followees.size()-_maliciousNodes.size())*0.5);
        for (Transaction transaction : believers.keySet()) {
            int count = believers.get(transaction);
            if (count > threshold) {
                _consensusTransactions.add(transaction);
            }
        }
    }

    private void detectMaliciousNodes(int[] nextFolloweeTransactions, Set<Candidate> candidates) {
        if (_currRound <= 1) return;

        for (int nodeId : _followees) {
            boolean decreasingNumberOfTxs = nextFolloweeTransactions[nodeId] < _followeeTransactions[nodeId];
            boolean returningNoTxs = _currRound > _numRounds/2 && _followeeTransactions[nodeId] == 0;
            if (decreasingNumberOfTxs || returningNoTxs) {
                _maliciousNodes.add(nodeId);
            }
        }
    }
}
