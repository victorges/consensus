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

    private final Map<Transaction, NodesSet> _transactionBelievers = new TreeMap<>(s_transactionComparator);
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
        _pendingTransactions.addAll(pendingTransactions);
    }

    public Set<Transaction> sendToFollowers() {
        if (_currRound++ < _numRounds) {
            return _pendingTransactions;
        }

        Set<Transaction> consensus = new TreeSet<>(s_transactionComparator);
        int threshold = (int)((_followees.size()-_maliciousNodes.size())*0.85);
        for (Transaction transaction : _transactionBelievers.keySet()) {
            NodesSet nodes = _transactionBelievers.get(transaction);
            if (nodes.roundCount() > threshold) {
                consensus.add(transaction);
            }
        }
        return consensus;
    }

    public void receiveFromFollowees(Set<Candidate> candidates) {
        _transactionBelievers.values().forEach(ns -> ns.bumpRound(_currRound));
        int[] nextFolloweeTransactions = new int[_numNodes];
        for (Candidate candidate : candidates) {
            if (!_followees.contains(candidate.sender)) continue;
            if (_maliciousNodes.contains(candidate.sender)) continue;

            if (!_transactionBelievers.containsKey(candidate.tx)) {
                _transactionBelievers.put(candidate.tx, new NodesSet(_numNodes, _currRound));
            }
            _transactionBelievers.get(candidate.tx).flagNode(candidate.sender);
            nextFolloweeTransactions[candidate.sender]++;
        }

        detectMaliciousNodes(nextFolloweeTransactions);
        _followeeTransactions = nextFolloweeTransactions;

        int threshold = (int)(_p_malicious * (_currRound / (double) _numRounds) * (_followees.size() - _maliciousNodes.size()));
        for (Transaction tx : _transactionBelievers.keySet()) {
            int count = _transactionBelievers.get(tx).roundCount();
            if (count > 0 && count >= threshold) {
                _pendingTransactions.add(tx);
            }
        }

        candidates.clear();
    }

    private void detectMaliciousNodes(int[] nextFolloweeTransactions) {
        if (_currRound <= 1) return;

        for (int nodeId : _followees) {
            boolean decreasingNumberOfTxs = nextFolloweeTransactions[nodeId] < _followeeTransactions[nodeId];
            boolean returningNoTxs = _currRound >= 3 && nextFolloweeTransactions[nodeId] == 0;
            boolean returningOnlyOwnTxs = _currRound > _numRounds/2 && nextFolloweeTransactions[nodeId] <= 2*500*_p_txDistribution;
            if (decreasingNumberOfTxs || returningNoTxs || returningOnlyOwnTxs) {
                _maliciousNodes.add(nodeId);
            }
        }

        for (Transaction tx : _transactionBelievers.keySet()) {
            NodesSet nodes = _transactionBelievers.get(tx);

            int peaceBelievers = 0;
            for (int nodeId : _followees) {
                if (_maliciousNodes.contains(nodeId) || !nodes.everFlagged(nodeId)) continue;

                if (!nodes.flaggedInRound(nodeId)) {
                    _maliciousNodes.add(nodeId);
                } else {
                    peaceBelievers++;
                }
            }

            int distrustTreshold = (int)((_followees.size()-_maliciousNodes.size())*0.95);
            if (_currRound > 2*_numRounds/3 && peaceBelievers > distrustTreshold) {
                for (int nodeId : _followees) {
                    if (_maliciousNodes.contains(nodeId)) continue;

                    if (!nodes.everFlagged(nodeId)) {
                        _maliciousNodes.add(nodeId);
                    }
                }
            }
        }
    }
}

class NodesSet {
    private final byte[] _nodesLastRound;
    private byte _currRound;
    private int _currRoundCount;

    public final int firstRoundSeen;

    public NodesSet(int numNodes, int round) {
        this._nodesLastRound = new byte[numNodes];
        firstRoundSeen = round;
        bumpRound(round);
    }

    public void bumpRound(int round) {
        if (round <= 0) throw new IllegalArgumentException("round must be positive");
        if ((int)(byte)round != round) throw new IllegalArgumentException("round must fit a byte");
        _currRound = (byte) round;
        _currRoundCount = 0;
    }

    public void flagNode(int nodeId) {
        if (_nodesLastRound[nodeId] == _currRound) return;

        _nodesLastRound[nodeId] = _currRound;
        _currRoundCount++;
    }

    public boolean flaggedInRound(int nodeId) {
        return _nodesLastRound[nodeId] == _currRound;
    }

    public boolean everFlagged(int nodeId) {
        return _nodesLastRound[nodeId] > 0;
    }

    public int roundCount() {
        return _currRoundCount;
    }
}
