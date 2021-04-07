import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class MaxFeeTxHandler {

    public static final Comparator<Transaction> s_transactionComparator = Comparator.comparing(Transaction::numInputs)
            .thenComparing(TransactionId::new)
            .reversed();
    private final UTXOPoolOperator _utxoPool;

    /**
     * Creates a public ledger whose current UTXOPool (collection of unspent transaction outputs) is {@code utxoPool}.
     */
    public MaxFeeTxHandler(UTXOPool utxoPool) {
        _utxoPool = new UTXOPoolOperator(new UTXOPool(utxoPool));
    }

    /**
     * @return true if:
     * (1) all outputs claimed by {@code tx} are in the current UTXO pool,
     * (2) the signatures on each input of {@code tx} are valid,
     * (3) no UTXO is claimed multiple times by {@code tx},
     * (4) all of {@code tx}s output values are non-negative, and
     * (5) the sum of {@code tx}s input values is greater than or equal to the sum of its output
     * values; and false otherwise.
     */
    public boolean isValidTx(Transaction tx) {
        return _utxoPool.isValidTransaction(tx);
    }

    /**
     * Handles each epoch by receiving an unordered array of proposed transactions, checking each
     * transaction for correctness, returning a mutually valid array of accepted transactions, and
     * updating the current UTXO pool as appropriate.
     */
    public Transaction[] handleTxs(Transaction[] possibleTxs) {
        RelatedTransactionGroup[] groups = createTxGroups(possibleTxs);

        ArrayList<Transaction> pickedTxs = new ArrayList<>();
        for (RelatedTransactionGroup group : groups) {
            HandleResult result = handleTxsGroup(_utxoPool.copy(), group, new ArrayList<>(group.transactionIds().size()), 0);
            pickedTxs.addAll(result.pickedTxs);
        }

        for (Transaction tx : pickedTxs) {
            _utxoPool.performTransaction(tx);
        }
        return pickedTxs.toArray(new Transaction[0]);
    }

    private static HandleResult handleTxsGroup(UTXOPoolOperator pool, RelatedTransactionGroup group, List<Transaction> performedTxs, double currFee) {
        if (group.isEmpty()) {
            HandleResult result = new HandleResult();
            result.pickedTxs = new ArrayList<>(performedTxs);
            result.totalFee = currFee;
            return result;
        }
        Transaction tx = group.pollFirstTransaction();
        boolean hasConflict = group.removeTransaction(tx);
        boolean isValid = pool.isValidTransaction(tx);

        HandleResult withTx = null, withoutTx = null;
        if (!isValid || hasConflict) {
            withoutTx = handleTxsGroup(pool, group, performedTxs, currFee);
        }
        if (isValid) {
            UTXOPoolOperator.PerformResult txResult = pool.performTransaction(tx);
            performedTxs.add(tx);

            withTx = handleTxsGroup(pool, group, performedTxs, currFee + txResult.fee);

            pool.undoTransaction(tx, txResult);
            performedTxs.remove(performedTxs.size() - 1);
        }

        group.pushFirstTransaction(tx);

        if (withTx == null) return withoutTx;
        if (withoutTx == null) return withTx;
        return withTx.totalFee > withoutTx.totalFee ? withTx : withoutTx;
    }

    private static class HandleResult {
        public ArrayList<Transaction> pickedTxs;
        public double totalFee;
    }

    private static RelatedTransactionGroup[] createTxGroups(Transaction[] possibleTxs) {
        HashMap<TransactionId, Transaction> txById = new HashMap<>();
        for (Transaction tx : possibleTxs) {
            txById.put(new TransactionId(tx), tx);
        }

        HashMap<UTXO, Set<TransactionId>> dependents = new HashMap<>();
        for (Transaction tx : possibleTxs) {
            TransactionId id = new TransactionId(tx);
            for (Transaction.Input in : tx.getInputs()) {
                UTXO utxo = new UTXO(in.prevTxHash, in.outputIndex);
                Set<TransactionId> srcDependents = dependents.computeIfAbsent(utxo, k -> new HashSet<>());
                srcDependents.add(id);
            }
        }

        List<RelatedTransactionGroup> groups = new ArrayList<>();
        Set<TransactionId> processedTxs = new HashSet<>();

        Transaction[] sortedTxs = Arrays.copyOf(possibleTxs, possibleTxs.length);
        Arrays.sort(sortedTxs, s_transactionComparator);
        for (Transaction tx : sortedTxs) {
            if (processedTxs.contains(new TransactionId(tx))) continue;

            RelatedTransactionGroup group = createTxGroup(tx, txById, dependents);
            processedTxs.addAll(group.transactionIds());
            groups.add(group);
        }
        return groups.toArray(new RelatedTransactionGroup[0]);
    }

    private static RelatedTransactionGroup createTxGroup(Transaction tx,
                                                         HashMap<TransactionId, Transaction> txById,
                                                         Map<UTXO, Set<TransactionId>> dependents) {
        RelatedTransactionGroup group = new RelatedTransactionGroup();

        SortedSet<Transaction> relatedTxs = new TreeSet<>(s_transactionComparator);
        relatedTxs.add(tx);

        while (!relatedTxs.isEmpty()) {
            Transaction nextTx = relatedTxs.first();
            relatedTxs.remove(nextTx);
            fillTxGroup(group, nextTx, relatedTxs, txById, dependents);
        }

        return group;
    }

    private static void fillTxGroup(RelatedTransactionGroup group, Transaction tx,
                                    Set<Transaction> relatedTxs,
                                    HashMap<TransactionId, Transaction> txById,
                                    Map<UTXO, Set<TransactionId>> dependents) {
        TransactionId ownId = new TransactionId(tx);
        if (group.contains(ownId)) return;

        tx.getInputs().stream()
                .map(in -> txById.get(new TransactionId(in.prevTxHash)))
                .filter(Objects::nonNull)
                .forEach(sameBlockSrc -> fillTxGroup(group, sameBlockSrc, relatedTxs, txById, dependents));

        group.addTransaction(tx);

        Stream<Set<TransactionId>> sameInputTxs = tx.getInputs().stream()
                .map(in -> new UTXO(in.prevTxHash, in.outputIndex))
                .map(dependents::get);
        Stream<Set<TransactionId>> outputConsumers = IntStream.range(0, tx.numOutputs())
                .mapToObj(i -> new UTXO(ownId.hash, i))
                .map(dependents::get)
                .filter(Objects::nonNull);

        Stream.concat(sameInputTxs, outputConsumers)
                .flatMap(Set::stream)
                .filter(id -> !ownId.equals(id) && !group.contains(id))
                .map(txById::get)
                .forEach(relatedTxs::add);
    }
}

class RelatedTransactionGroup {
    private final Set<TransactionId> _ids;
    private final LinkedList<Transaction> _transactions;
    private final Map<UTXO, Integer> _dependentCount;

    public RelatedTransactionGroup() {
        _ids = new HashSet<>();
        _transactions = new LinkedList<>();
        _dependentCount = new HashMap<>();
    }

    public boolean isEmpty() {
        return _transactions.isEmpty();
    }

    public Set<TransactionId> transactionIds() {
        return _ids;
    }

    public boolean contains(TransactionId id) {
        return _ids.contains(id);
    }

    public void addTransaction(Transaction tx) {
        _transactions.addLast(tx);
        _ids.add(new TransactionId(tx));
        updateDependentCount(tx, (utxo, dc) -> dc == null ? 1 : dc + 1);
    }

    public Transaction pollFirstTransaction() {
        return _transactions.pollFirst();
    }

    // Removes specified transaction and returns whether the resulting group still has any conflicting dependencies with
    // the given transaction.
    public boolean removeTransaction(Transaction tx) {
        _transactions.remove(tx);
        _ids.remove(new TransactionId(tx));
        int maxCount = updateDependentCount(tx, (utxo, dc) -> dc - 1);
        return maxCount > 0;
    }

    public void pushFirstTransaction(Transaction tx) {
        _transactions.addFirst(tx);
        _ids.add(new TransactionId(tx));
        updateDependentCount(tx, (utxo, dc) -> dc == null ? 1 : dc + 1);
    }

    private int updateDependentCount(Transaction tx, BiFunction<UTXO, Integer, Integer> remap) {
        int maxCount = 0;
        for (Transaction.Input in : tx.getInputs()) {
            UTXO utxo = new UTXO(in.prevTxHash, in.outputIndex);
            maxCount = Integer.max(maxCount, _dependentCount.compute(utxo, remap));
        }
        return maxCount;
    }
}

class TransactionId implements Comparable<TransactionId> {
    public final byte[] hash;

    public TransactionId(Transaction tx) {
        this(tx.getHash());
    }

    public TransactionId(byte[] hash) {
        this.hash = hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionId that = (TransactionId) o;
        return Arrays.equals(hash, that.hash);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(hash);
    }

    @Override
    public int compareTo(TransactionId o) {
        if (o == null) return 1;

        if (hash.length != o.hash.length) {
            return Integer.compare(hash.length, o.hash.length);
        }
        for (int i = 0; i < hash.length; i++) {
            int cmp = Byte.compare(hash[i], o.hash[i]);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }
}

class UTXOPoolOperator {
    public final UTXOPool pool;

    public UTXOPoolOperator(UTXOPool pool) {
        this.pool = pool;
    }

    public PerformResult performTransaction(Transaction tx) {
        double fee = 0;
        Transaction.Output[] inputSrcs = new Transaction.Output[tx.numInputs()];
        for (int i = 0; i < tx.numInputs(); i++) {
            Transaction.Input in = tx.getInput(i);
            UTXO utxo = new UTXO(in.prevTxHash, in.outputIndex);

            Transaction.Output consumedOutput = pool.getTxOutput(utxo);
            pool.removeUTXO(utxo);

            fee += consumedOutput.value;
            inputSrcs[i] = consumedOutput;
        }
        byte[] hash = tx.getHash();
        for (int i = 0; i < tx.numOutputs(); i++) {
            UTXO utxo = new UTXO(hash, i);

            Transaction.Output producedOutput = tx.getOutput(i);
            pool.addUTXO(utxo, producedOutput);

            fee -= producedOutput.value;
        }
        return new PerformResult(inputSrcs, fee);
    }

    public void undoTransaction(Transaction tx, PerformResult result) {
        for (int i = 0; i < tx.numInputs(); i++) {
            Transaction.Input in = tx.getInput(i);
            UTXO utxo = new UTXO(in.prevTxHash, in.outputIndex);

            pool.addUTXO(utxo, result.inputSources[i]);
        }
        byte[] hash = tx.getHash();
        for (int i = 0; i < tx.numOutputs(); i++) {
            UTXO utxo = new UTXO(hash, i);
            pool.removeUTXO(utxo);
        }
    }

    public boolean isValidTransaction(Transaction tx) {
        HashSet<UTXO> allClaimedUTXO = new HashSet<>();
        double inputSum = 0.0;
        for (int i = 0; i < tx.numInputs(); i++) {
            Transaction.Input in = tx.getInput(i);
            UTXO claimedTXO = new UTXO(in.prevTxHash, in.outputIndex);

            Transaction.Output inSrc = pool.getTxOutput(claimedTXO);
            if (inSrc == null || allClaimedUTXO.contains(claimedTXO)) {
                return false;
            }
            allClaimedUTXO.add(claimedTXO);

            byte[] signedData = tx.getRawDataToSign(i);
            if (!Crypto.verifySignature(inSrc.address, signedData, in.signature)) {
                return false;
            }

            inputSum += inSrc.value;
        }

        double outputSum = 0.0;
        for (Transaction.Output out : tx.getOutputs()) {
            if (out.value < 0) {
                return false;
            }
            outputSum += out.value;
        }

        return outputSum <= inputSum;
    }

    public UTXOPoolOperator copy() {
        return new UTXOPoolOperator(new UTXOPool(pool));
    }

    public static class PerformResult {
        public final Transaction.Output[] inputSources;
        public final double fee;

        public PerformResult(Transaction.Output[] inputSources, double fee) {
            this.inputSources = inputSources;
            this.fee = fee;
        }
    }
}
