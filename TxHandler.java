import java.util.ArrayList;
import java.util.HashSet;

public class TxHandler {

    private UTXOPool _utxoPool;

    /**
     * Creates a public ledger whose current UTXOPool (collection of unspent transaction outputs) is {@code utxoPool}.
     */
    public TxHandler(UTXOPool utxoPool) {
        _utxoPool = new UTXOPool(utxoPool);
    }

    /**
     * @return true if:
     * (1) all outputs claimed by {@code tx} are in the current UTXO pool, 
     * (2) the signatures on each input of {@code tx} are valid, 
     * (3) no UTXO is claimed multiple times by {@code tx},
     * (4) all of {@code tx}s output values are non-negative, and
     * (5) the sum of {@code tx}s input values is greater than or equal to the sum of its output
     *     values; and false otherwise.
     */
    public boolean isValidTx(Transaction tx) {
        return isValidTxForPool(tx, _utxoPool);
    }

    private static boolean isValidTxForPool(Transaction tx, UTXOPool utxoPool) {
        HashSet<UTXO> allClaimedUTXO = new HashSet<UTXO>();
        double inputSum = 0.0;
        for (int i = 0; i < tx.numInputs(); i++) {
            Transaction.Input in = tx.getInput(i);
            UTXO claimedTXO = new UTXO(in.prevTxHash, in.outputIndex);

            Transaction.Output inSrc = utxoPool.getTxOutput(claimedTXO);
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

    /**
     * Handles each epoch by receiving an unordered array of proposed transactions, checking each
     * transaction for correctness, returning a mutually valid array of accepted transactions, and
     * updating the current UTXO pool as appropriate.
     */
    public Transaction[] handleTxs(Transaction[] possibleTxs) {
        UTXOPool finalUtxos = new UTXOPool(_utxoPool);
        ArrayList<Transaction> performedTxs = new ArrayList<Transaction>();
        while (true) {
            boolean performedAny = false;
            for (Transaction tx : possibleTxs) {
                if (isValidTxForPool(tx, finalUtxos)) {
                    performTx(finalUtxos, tx);
                    performedTxs.add(tx);
                    performedAny = true;
                }
            }
            if (!performedAny) break;
        }
        _utxoPool = finalUtxos;
        return performedTxs.toArray(new Transaction[0]);
    }

    private static void performTx(UTXOPool pool, Transaction tx) {
        for (Transaction.Input in : tx.getInputs()) {
            UTXO utxo = new UTXO(in.prevTxHash, in.outputIndex);
            pool.removeUTXO(utxo);
        }
        byte[] hash = tx.getHash();
        for (int i = 0; i < tx.numOutputs(); i++) {
            UTXO utxo = new UTXO(hash, i);
            pool.addUTXO(utxo, tx.getOutput(i));
        }
    }
}
