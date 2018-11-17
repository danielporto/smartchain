/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.tom.server.defaultservices.blockchain;

import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.reconfiguration.util.TOMConfiguration;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.StateManager;
import bftsmart.statemanagement.standard.StandardStateManager;
import bftsmart.tom.AsynchServiceProxy;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplicaContext;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.server.BatchExecutable;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.util.BatchBuilder;
import bftsmart.tom.util.TOMUtil;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author joao
 */
public abstract class StrongBlockchainRecoverable implements Recoverable, BatchExecutable {
    
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    
    public String batchDir;    
    
    private TOMConfiguration config;
    private ServerViewController controller;
    private StateManager stateManager;
    
    private BatchLogger log;
    private LinkedList<TOMMessage> results;
    
    private int nextNumber;
    private int lastCheckpoint;
    private int lastReconfig;
    private byte[] lastBlockHash;
    
    private AsynchServiceProxy proxy;
    
    private ReentrantLock mapLock = new ReentrantLock();
    private Condition gotCertificate = mapLock.newCondition();
    private Map<Integer, Map<Integer,byte[]>> certificates;
    private int currentCommit;
    
    public StrongBlockchainRecoverable() {
        
        nextNumber = 0;
        lastCheckpoint = -1;
        lastReconfig = -1;
        lastBlockHash = new byte[] {-1};
        
        results = new LinkedList<>(); 
        
        currentCommit = -1;
        certificates =  new HashMap<>();
    }
    
    @Override
    public void setReplicaContext(ReplicaContext replicaContext) {

        try {
            
            config = replicaContext.getStaticConfiguration();
            controller = replicaContext.getSVController();
            
            proxy = new AsynchServiceProxy(config.getProcessId());
        
            //batchDir = config.getConfigHome().concat(System.getProperty("file.separator")) +
            batchDir =    "files".concat(System.getProperty("file.separator"));
            log = config.getLogBatchType().equalsIgnoreCase("buffer") ? 
                    BufferBatchLogger.getInstance(config.getProcessId(), batchDir) : 
                    ParallelBatchLogger.getInstance(config.getProcessId(), batchDir);
            
            //write genesis block
            byte[][] hashes = log.markEndTransactions();
            log.storeHeader(nextNumber, lastCheckpoint, lastReconfig, hashes[0], hashes[1], lastBlockHash);
                        
            lastBlockHash = computeBlockHash(nextNumber, lastCheckpoint, lastReconfig, hashes[0], hashes[1], lastBlockHash);
                        
            nextNumber++;
            
        } catch (Exception ex) {
            
            throw new RuntimeException("Could not set replica context", ex);
        }
        getStateManager().askCurrentConsensusId();
    }

    @Override
    public ApplicationState getState(int cid, boolean sendState) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int setState(ApplicationState state) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public StateManager getStateManager() {
        if (stateManager == null) {
            stateManager = new StandardStateManager(); // might need to be other implementation
        }
        return stateManager;
    }

    @Override
    public void Op(int CID, byte[] requests, MessageContext msgCtx) {
        
        // Since we are logging entire batches, we do not use this
    }

    @Override
    public void noOp(int CID, byte[][] operations, MessageContext[] msgCtxs, boolean isCheckpoint) {
        
        executeBatch(operations, msgCtxs, isCheckpoint, true);
    }

    @Override
    public TOMMessage[] executeBatch(byte[][] operations, MessageContext[] msgCtxs, boolean isCheckpoint) {
        
        return executeBatch(operations, msgCtxs, isCheckpoint, false);
    }

    @Override
    public byte[] executeUnordered(byte[] command, MessageContext msgCtx) {
        
        if (controller.isCurrentViewMember(msgCtx.getSender())) {
                        
            ByteBuffer buff = ByteBuffer.wrap(command);
            
            int cid = buff.getInt();
            byte[] sig = new byte[buff.getInt()];
            buff.get(sig);
            
            logger.debug("Received signature from {}: {}", msgCtx.getSender(), Base64.encodeBase64String(sig));

            if (currentCommit <= cid) {
            
                mapLock.lock();

                Map<Integer,byte[]> signatures = certificates.get(cid);
                if (signatures == null) {

                    signatures = new HashMap<>();
                    certificates.put(cid, signatures);
                }

                signatures.put(msgCtx.getSender(), sig);

                logger.debug("got {} sigs for CID {}", signatures.size(), cid);

                if (currentCommit == cid && signatures.size() > controller.getQuorum()) {

                    logger.debug("Signaling main thread");
                    gotCertificate.signalAll();
                }

                mapLock.unlock();
            
            }
            return new byte[0];
        }
        else {
            return appExecuteUnordered(command, msgCtx);
        }
    }

    @Override
    public byte[] takeCheckpointHash(int cid) {
        return TOMUtil.computeHash(getSnapshot());
    }
    
    private TOMMessage[] executeBatch(byte[][] operations, MessageContext[] msgCtxs, boolean isCheckpoint, boolean noop) {
        
        int cid = msgCtxs[0].getConsensusId();
        TOMMessage[] replies = new TOMMessage[0];
        
        try {
            
            log.storeTransactions(cid, operations, msgCtxs);
                        
            if (!noop) {
                
                byte[][] results = appExecuteBatch(operations, msgCtxs, false);
                //replies = new TOMMessage[results.length];
                
                log.storeResults(results);
                
                for (int i = 0; i < results.length; i++) {
                    
                    TOMMessage request = msgCtxs[i].recreateTOMMessage(operations[i]);                    
                    request.reply = new TOMMessage(config.getProcessId(), request.getSession(), request.getSequence(), request.getOperationId(),
                            results[i], controller.getCurrentViewId(), request.getReqType());
                    
                    this.results.add(request);
                }
            }
            
            if (isCheckpoint || (this.results.size() % config.getLogBatchLimit() == 0)) {
                
                byte[][] hashes = log.markEndTransactions();
                
                log.storeHeader(nextNumber, lastCheckpoint, lastReconfig, hashes[0], hashes[1], lastBlockHash);
                
                lastBlockHash = computeBlockHash(nextNumber, lastCheckpoint, lastReconfig, hashes[0], hashes[1], lastBlockHash);
                nextNumber++;
                                
                replies = new TOMMessage[this.results.size()];
                
                this.results.toArray(replies);
                this.results.clear();
                
                if (isCheckpoint) log.clearCached();
                
                logger.info("Executing COMMIT phase at CID {} for block number {}", cid, (nextNumber - 1));
                
                byte[] mySig = TOMUtil.signMessage(config.getPrivateKey(), lastBlockHash);
                
                ByteBuffer buff = ByteBuffer.allocate((Integer.BYTES * 2) + mySig.length);
                
                buff.putInt(cid);
                buff.putInt(mySig.length);
                buff.put(mySig);
                
                int context = proxy.invokeAsynchRequest(buff.array(), null, TOMMessageType.UNORDERED_REQUEST);
                proxy.cleanAsynchRequest(context);
                
                mapLock.lock();
                
                certificates.remove(currentCommit);

                currentCommit = cid;
                
                Map<Integer,byte[]> signatures = certificates.get(cid);
                if (signatures == null) {
                    
                    signatures = new HashMap<>();
                    certificates.put(cid, signatures);
                }
                
                while (!(signatures.size() > controller.getQuorum())) {
                    
                    logger.debug("blocking main thread");
                    gotCertificate.await(200, TimeUnit.MILLISECONDS);
                    //gotCertificate.await();
                    
                    //signatures = certificates.get(cid);
                }
                
                signatures = certificates.get(cid);
                
                Map<Integer,byte[]> copy = new HashMap<>();
                
                signatures.forEach((id,sig) -> {
                    
                    copy.put(id, sig);
                });
                                
                mapLock.unlock();
                
                log.storeCertificate(copy);
                                
                logger.info("Synching log at CID {} and Block {}", cid, (nextNumber - 1));
                
                log.sync();
                
                
            }
            
            return replies;
        } catch (IOException | NoSuchAlgorithmException | InterruptedException ex) {
            logger.error("Error while logging/executing batch for CID " + cid, ex);
            return new TOMMessage[0];
        } finally {
            if (mapLock.isHeldByCurrentThread()) mapLock.unlock();
        }
    }
    
    private byte[] computeBlockHash(int number, int lastCheckpoint, int lastReconf,  byte[] transHash, byte[] resultsHash, byte[] prevBlock) throws NoSuchAlgorithmException {
    
        ByteBuffer buff = ByteBuffer.allocate(Integer.BYTES * 6 + (prevBlock.length + transHash.length + resultsHash.length));
        
        buff.putInt(number);
        buff.putInt(lastCheckpoint);
        buff.putInt(lastReconf);

        buff.putInt(transHash.length);
        buff.put(transHash);
        
        buff.putInt(resultsHash.length);
        buff.put(resultsHash);
        
        buff.putInt(prevBlock.length);
        buff.put(prevBlock);

        MessageDigest md = TOMUtil.getHashEngine();
        
        return md.digest(buff.array());
    }
    
    
    public boolean verifyBatch(byte[][] commands, MessageContext[] msgCtxs){
        
        //first off, re-create the batch received in the PROPOSE message of the consensus instance
        int totalMsgsSize = 0;
        byte[][] messages = new byte[commands.length][];
        byte[][] signatures = new byte[commands.length][];
        
        for (int i = 0; i <commands.length; i++) {
            
            TOMMessage msg = msgCtxs[i].recreateTOMMessage(commands[i]);
            messages[i] = msg.serializedMessage;
            signatures[i] = msg.serializedMessageSignature;
            totalMsgsSize += msg.serializedMessage.length;
        }
        
        BatchBuilder builder = new BatchBuilder(0);
        byte[] serializeddBatch = builder.createBatch(msgCtxs[0].getTimestamp(), msgCtxs[0].getNumOfNonces(), msgCtxs[0].getSeed(),
                commands.length, totalMsgsSize, true, messages, signatures);
        
        // now we can obtain the hash contained in the ACCEPT messages from the proposed value
        byte[] hashedBatch = TOMUtil.computeHash(serializeddBatch);
        
        //we are now ready to verify each message that comprises the proof
        int countValid = 0;
        int certificate = (2*controller.getCurrentViewF()) + 1;
        
        HashSet<Integer> alreadyCounted = new HashSet<>();
        
        for (ConsensusMessage consMsg : msgCtxs[0].getProof()) {
            
            ConsensusMessage cm = new ConsensusMessage(consMsg.getType(),consMsg.getNumber(),
                    consMsg.getEpoch(), consMsg.getSender(), consMsg.getValue());
            
            cm.setCheckpointHash(consMsg.getCheckpointHash());

            ByteArrayOutputStream bOut = new ByteArrayOutputStream(248);
            try {
                new ObjectOutputStream(bOut).writeObject(cm);
            } catch (IOException ex) {
                logger.error("Could not serialize message",ex);
            }

            byte[] data = bOut.toByteArray();
                        
            PublicKey pubKey = config.getPublicKey(consMsg.getSender());

            byte[] signature = (byte[]) consMsg.getProof();

            if (Arrays.equals(consMsg.getValue(), hashedBatch) &&
                    TOMUtil.verifySignature(pubKey, data, signature) && !alreadyCounted.contains(consMsg.getSender())) {

                alreadyCounted.add(consMsg.getSender());
                countValid++;
            } else {
                logger.error("Invalid signature in message from " + consMsg.getSender());
            }
        }
        
        boolean ret = countValid >=  certificate;
        logger.info("Proof for CID {} is {} ({} valid messages, needed {})",
                msgCtxs[0].getConsensusId(), (ret ? "valid" : "invalid"), countValid, certificate);
        return ret;
    }
 
    /**
     * Given a snapshot received from the state transfer protocol, install it
     * @param state The serialized snapshot
     */
    public abstract void installSnapshot(byte[] state);
    
    /**
     * Returns a serialized snapshot of the application state
     * @return A serialized snapshot of the application state
     */
    public abstract byte[] getSnapshot();
    
    /**
     * Execute a batch of ordered requests
     * 
     * @param commands The batch of requests
     * @param msgCtxs The context associated to each request
     * @param fromConsensus true if the request arrived from a consensus execution, false if it arrives from the state transfer protocol
     * 
     * @return the respective replies for each request
     */
    public abstract byte[][] appExecuteBatch(byte[][] commands, MessageContext[] msgCtxs, boolean fromConsensus);
    
    /**
     * Execute an unordered request
     * 
     * @param command The unordered request
     * @param msgCtx The context associated to the request
     * 
     * @return the reply for the request issued by the client
     */
    public abstract byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx);

}
