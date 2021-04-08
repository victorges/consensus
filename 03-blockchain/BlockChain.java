import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

// Block Chain should maintain only limited block nodes to satisfy the functions
// You should not have all the blocks added to the block chain in memory
// as it would cause a memory overflow.

public class BlockChain {
    public static final int CUT_OFF_AGE = 10;

    private final TransactionPool _txPool = new TransactionPool();
    private final ChainHeadBlockTree _blockTree;

    /**
     * create an empty block chain with just a genesis block. Assume {@code genesisBlock} is a valid
     * block
     */
    public BlockChain(Block genesisBlock) {
        _blockTree = new ChainHeadBlockTree(CUT_OFF_AGE);

        UTXOPool genesisUTXOs = getResultingPool(new UTXOPool(), genesisBlock);
        boolean added = _blockTree.addBlock(genesisBlock, genesisUTXOs);
        if (genesisBlock == null || !added) {
            throw new IllegalArgumentException("Bad genesis block");
        }
    }

    /** Get the maximum height block */
    public Block getMaxHeightBlock() {
        return _blockTree.getMaxHeightBlock().block;
    }

    /** Get the UTXOPool for mining a new block on top of max height block */
    public UTXOPool getMaxHeightUTXOPool() {
        return _blockTree.getMaxHeightBlock().utxoPool;
    }

    /** Get the transaction pool to mine a new block */
    public TransactionPool getTransactionPool() {
        return new TransactionPool(_txPool);
    }

    /**
     * Add {@code block} to the block chain if it is valid. For validity, all transactions should be
     * valid and block should be at {@code height > (maxHeight - s_cutOffAge)}.
     *
     * <p>
     * For example, you can try creating a new block over the genesis block (block height 2) if the
     * block chain height is {@code <=
     * s_cutOffAge + 1}. As soon as {@code height > s_cutOffAge + 1}, you cannot create a new block
     * at height 2.
     *
     * @return true if block is successfully added
     */
    public boolean addBlock(Block block) {
        if (block.getPrevBlockHash() == null) {
            return false;
        }

        BlockInfo parent = _blockTree.getParentBlock(block);
        if (parent == null) {
            return false;
        }
        UTXOPool resultingPool = getResultingPool(parent.utxoPool, block);
        if (resultingPool == null) {
            return false;
        }

        if (!_blockTree.addBlock(block, resultingPool)) {
            return false;
        }

        for (Transaction tx : block.getTransactions()) {
            _txPool.removeTransaction(tx.getHash());
        }
        return true;
    }

    /** Adds a transaction to the transaction pool */
    public void addTransaction(Transaction tx) {
        _txPool.addTransaction(tx);
    }

    private static UTXOPool getResultingPool(UTXOPool prevPool, Block block) {
        TxHandler handler = new TxHandler(prevPool);
        Transaction[] txs = block.getTransactions().toArray(new Transaction[0]);
        Transaction[] successTxs = handler.handleTxs(txs);
        if (txs.length != successTxs.length) {
            return null;
        }

        UTXOPool pool = handler.getUTXOPool();
        if (!addCoinbaseTransaction(pool, block)) {
            return null;
        }
        return pool;
    }

    private static boolean addCoinbaseTransaction(UTXOPool pool, Block block) {
        Transaction coinbase = block.getCoinbase();
        if (!coinbase.isCoinbase() || coinbase.numInputs() > 0) {
            return false;
        }
        byte[] hash = coinbase.getHash();
        for (int i = 0; i < coinbase.numOutputs(); i++) {
            UTXO utxo = new UTXO(hash, i);
            Transaction.Output producedOutput = coinbase.getOutput(i);
            pool.addUTXO(utxo, producedOutput);
        }
        return true;
    }
}

class BlockInfo {
    public final Date createdAt = new Date();
    public final Block block;
    public final long height;
    public final UTXOPool utxoPool;

    public BlockInfo(Block block, long height, UTXOPool pool) {
        this.block = block;
        this.height = height;
        this.utxoPool = pool;
    }
}

class ChainHeadBlockTree {
    public static final Comparator<BlockInfo> s_blockInfoComparator =
        Comparator.<BlockInfo>comparingLong(b -> b.height)
            .reversed()
            .thenComparing(b -> b.createdAt);

    private final Map<ByteArrayWrapper, BlockInfo> _knownBlocks = new HashMap<>();
    private final SortedSet<BlockInfo> _sortedBlocks = new TreeSet<>(s_blockInfoComparator);

    private final long _cutOffAge;

    public ChainHeadBlockTree(long cutOffAge) {
        this._cutOffAge = cutOffAge;
    }

    public BlockInfo getMaxHeightBlock() {
        return _sortedBlocks.isEmpty() ? null : _sortedBlocks.first();
    }

    public long getMaxKnownHeight() {
        BlockInfo block = getMaxHeightBlock();
        return block == null ? 0 : block.height;
    }

    public BlockInfo getParentBlock(Block block) {
        return _knownBlocks.get(parentBlockKey(block));
    }

    public long getBlockHeight(Block block) {
        if (block.getPrevBlockHash() == null) return 1;

        BlockInfo parent = getParentBlock(block);
        if (parent == null) {
            return -1;
        }
        return parent.height+1;
    }

    public boolean addBlock(Block block, UTXOPool pool) {
        long height = getBlockHeight(block);
        long maxKnownHeight = getMaxKnownHeight();
        if (height <= Math.max(0, maxKnownHeight-_cutOffAge)) {
            return false;
        }

        BlockInfo info = new BlockInfo(block, height, pool);
        _knownBlocks.put(blockKey(block), info);
        _sortedBlocks.add(info);

        if (height > maxKnownHeight) {
            cutOffOldBlocks();
        }
        return true;
    }

    private void cutOffOldBlocks() {
        // We need to remember blocks at height 1 below the new blocks cut-off
        // height to still be able to create blocks at exactly the cutOffHeight.
        long cutOffHeight = getMaxKnownHeight()-_cutOffAge-1;
        while (!_sortedBlocks.isEmpty()) {
            BlockInfo leastHeight = _sortedBlocks.last();
            if (leastHeight.height > cutOffHeight) {
                break;
            }
            _sortedBlocks.remove(leastHeight);
            _knownBlocks.remove(blockKey(leastHeight.block));
        }
    }

    private static ByteArrayWrapper blockKey(Block block) {
        return new ByteArrayWrapper(block.getHash());
    }

    private static ByteArrayWrapper parentBlockKey(Block block) {
        return new ByteArrayWrapper(block.getPrevBlockHash());
    }
}