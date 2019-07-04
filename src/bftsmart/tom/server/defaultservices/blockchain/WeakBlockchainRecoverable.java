/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.tom.server.defaultservices.blockchain;

import bftsmart.communication.ServerCommunicationSystem;
import bftsmart.tom.server.defaultservices.blockchain.logger.ParallelBatchLogger;
import bftsmart.tom.server.defaultservices.blockchain.logger.BufferBatchLogger;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.reconfiguration.util.TOMConfiguration;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.SMMessage;
import bftsmart.statemanagement.StateManager;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplicaContext;
import bftsmart.tom.core.messages.ForwardedMessage;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.server.BatchExecutable;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.server.defaultservices.CommandsInfo;
import bftsmart.tom.server.defaultservices.blockchain.logger.AsyncBatchLogger;
import bftsmart.tom.server.defaultservices.blockchain.logger.VoidBatchLogger;
import bftsmart.tom.server.defaultservices.blockchain.strategy.BlockchainSMMessage;
import bftsmart.tom.server.defaultservices.blockchain.strategy.BlockchainState;
import bftsmart.tom.server.defaultservices.blockchain.strategy.BlockchainStateManager;
import bftsmart.tom.util.BatchBuilder;
import bftsmart.tom.util.TOMUtil;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author joao
 */
public abstract class WeakBlockchainRecoverable implements Recoverable, BatchExecutable {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    
    public String batchDir;    
    
    private TOMConfiguration config;
    private ServerViewController controller;
    private BlockchainStateManager stateManager;
    private ServerCommunicationSystem commSystem;
    
    private TOMMessageGenerator TOMgen;
    
    private BatchLogger log;
    private LinkedList<TOMMessage> results;
    
    private int nextNumber;
    private int lastCheckpoint;
    private int lastReconfig;
    private byte[] lastBlockHash;
    
    private byte[] appState;
    private byte[] appStateHash;
    
    //private Timer timer;
    private Set<SMMessage> stateMsgs;
    private Map<Integer, Set<Integer>> timeouts;
    
    private ReentrantLock timerLock = new ReentrantLock();
        
    public WeakBlockchainRecoverable() {
        
        nextNumber = 0;
        lastCheckpoint = -1;
        lastReconfig = -1;
        lastBlockHash = new byte[] {-1};
        
        results = new LinkedList<>();      
        
        timeouts =  new HashMap<>();
        stateMsgs = new HashSet<>();
    }
    
    @Override
    public void setReplicaContext(ReplicaContext replicaContext) {

        try {
            
            config = replicaContext.getStaticConfiguration();
            controller = replicaContext.getSVController();
            
            commSystem = replicaContext.getServerCommunicationSystem();
            
            appState = getSnapshot();
            appStateHash = TOMUtil.computeHash(appState);
            
            TOMgen = new TOMMessageGenerator(controller);
            
            //batchDir = config.getConfigHome().concat(System.getProperty("file.separator")) +
            batchDir =    "files".concat(System.getProperty("file.separator"));
            
            initLog();
            
            log.startNewFile(nextNumber, config.getCheckpointPeriod());
            
            //write genesis block
            byte[] transHash = log.markEndTransactions()[0];
            log.storeHeader(nextNumber, lastCheckpoint, lastReconfig, transHash, new byte[0], lastBlockHash);
            
            log.sync();
                        
            lastBlockHash = computeBlockHash(nextNumber, lastCheckpoint, lastReconfig, transHash, lastBlockHash);
                        
            nextNumber++;
            
            //log.startNewFile(nextNumber, config.getCheckpointPeriod());
            
        } catch (Exception ex) {
            
            throw new RuntimeException("Could not set replica context", ex);
        }
        
        ((BlockchainStateManager) getStateManager()).setTOMgen(TOMgen);
        getStateManager().askCurrentConsensusId();
    }

    @Override
    public ApplicationState getState(int cid, boolean sendState) {
                
        logger.info("CID requested: " + cid + ". Last checkpoint: " + lastCheckpoint + ". Last CID: " + log.getLastStoredCID());
        
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        //boolean hasCached = log.getFirstCachedCID() != -1 && log.getLastCachedCID() != -1;
        //boolean hasState = cid >= lastCheckpoint && cid <= log.getLastStoredCID();
        
        CommandsInfo[] batches = null;

        int lastCID = -1;
        
        BlockchainState ret = new BlockchainState();
                                
            logger.info("Constructing ApplicationState up until CID " + cid);
                        
            lastCID = cid;
            
            ret = new BlockchainState(log.getCachedBatches(), log.getCachedResults(), log.getCachedHeaders(), log.getCachedCertificates(),
                    lastCheckpoint, lastCID, (sendState ? appState : null), appStateHash, config.getProcessId(), nextNumber, lastReconfig, lastBlockHash);
        
        return ret;
    }

    @Override
    public int setState(ApplicationState recvState) {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        
        int lastCID = -1;
        if (recvState instanceof BlockchainState) {

            BlockchainState state = (BlockchainState) recvState;

            int lastCheckpointCID = state.getLastCheckpointCID();
            lastCID = state.getLastCID();

            logger.info("I'm going to update myself from CID "
                    + lastCheckpointCID + " to CID " + lastCID);                
            try {

                if (state.getSerializedState() != null) {
                    logger.info("The state is not null. Will install it");
                    
                    nextNumber = state.getNextNumber();
                    lastCheckpoint = state.getLastCheckpointCID();
                    lastReconfig = state.getLastReconfig();
                    lastBlockHash = state.getLastBlockHash();
                    appState = state.getState();
                    appStateHash = state.getStateHash();
        
                    initLog();
                    log.setCached(state.getLastCheckpointCID(), state.getLastCID(), 
                            state.getBatches(), state.getResults(), state.getHeaders(), state.getCertificates());
                    installSnapshot(state.getSerializedState());
                    
                    
                }
                
                writeCheckpointToDisk(lastCheckpoint, appState);
                
                // block while fetching blocks
                //stateManager.fetchBlocks(lastCID);
                
                //fetch blocks without blocking
                final BlockchainStateManager bsm = stateManager;
                final int lcid = lastCID;
                
                Thread t = new Thread() {
                    
                    public void run() {
                        bsm.fetchBlocks(lcid);
                    }
                };
                
                t.start();
                
                if (lastCID % config.getCheckpointPeriod() == 0) {
                    log.startFileFromCache(config.getCheckpointPeriod());
                }
                else {
                    log.openFile(lastCheckpoint,config.getCheckpointPeriod());
                }
                
                for (int cid = lastCheckpointCID + 1; cid <= lastCID; cid++) {

                    logger.debug("Processing and verifying batched requests for cid " + cid);

                    CommandsInfo cmdInfo = state.getBatches().get(cid);
                    byte[][] commands = cmdInfo.commands; // take a batch
                    MessageContext[] msgCtxs = cmdInfo.msgCtx;
                    
                    //executeBatch(config.getProcessId(), controller.getCurrentViewId(), commands, msgCtxs, msgCtxs[0].isNoOp(),false);
                    
                    if (commands == null || msgCtxs == null || msgCtxs[0].isNoOp()) {
                        continue;
                    }
                    
                    LinkedList<byte[]> transList = new LinkedList<>();
                    LinkedList<MessageContext> ctxList = new LinkedList<>(); 
                                
                    for (int i = 0; i < commands.length; i++) {
                        
                        if (!controller.isCurrentViewMember(msgCtxs[i].getSender())) {
                            
                            transList.add(commands[i]);
                            ctxList.add(msgCtxs[i]);
                        }
                    }
                    
                    if (transList.size() > 0) {
                
                        byte[][] transApp = new byte[transList.size()][];
                        MessageContext[] ctxApp = new MessageContext[ctxList.size()];

                        transList.toArray(transApp);
                        ctxList.toArray(ctxApp);
                    
                        appExecuteBatch(transApp, ctxApp, false);
                    
                    }
                }

            } catch (Exception e) {
                logger.error("Failed to process and verify batched requests",e);
                if (e instanceof ArrayIndexOutOfBoundsException) {
                    logger.info("Last checkpoint, last consensus ID (CID): " + state.getLastCheckpointCID());
                    logger.info("Last CID: " + state.getLastCID());
                    logger.info("number of messages expected to be in the batch: " + (state.getLastCID() - state.getLastCheckpointCID() + 1));
                    logger.info("number of messages in the batch: " + state.getMessageBatches().length);
                 }
            }

        }
        
        return lastCID;
    }

    @Override
    public StateManager getStateManager() {
        if (stateManager == null) {
            stateManager = new BlockchainStateManager(false, false); // might need to be other implementation
        }
        return stateManager;
    }

    @Override
    public void Op(int CID, byte[] requests, MessageContext msgCtx) {
        
        // Since we are logging entire batches, we do not use this
    }

    @Override
    public void noOp(int CID, byte[][] operations, MessageContext[] msgCtxs) {
        
        executeBatch(-1, -1, operations, msgCtxs, true,true);
    }

    @Override
    public TOMMessage[] executeBatch(int processID, int viewID, byte[][] operations, MessageContext[] msgCtxs) {
        
        return executeBatch(processID, viewID, operations, msgCtxs, false, true);
    }

    @Override
    public byte[] executeUnordered(byte[] command, MessageContext msgCtx) {
        
        return appExecuteUnordered(command, msgCtx);
    }

    @Override
    public byte[] takeCheckpointHash(int cid) {
        return TOMUtil.computeHash(getSnapshot());
    }
    
    private TOMMessage[] executeBatch(int processID, int viewID, byte[][] operations, MessageContext[] msgCtxs, boolean noop, boolean fromConsensus) {
        
        int cid = msgCtxs[0].getConsensusId();
        TOMMessage[] replies = new TOMMessage[0];
        boolean timeout = false;
        
        try {
            
            LinkedList<byte[]> transList = new LinkedList<>();
            LinkedList<MessageContext> ctxList = new LinkedList<>(); 
            
            log.storeTransactions(cid, operations, msgCtxs);
            
            for (int i = 0; i < operations.length ; i++) {
                
                if (controller.isCurrentViewMember(msgCtxs[i].getSender())) {
                                        
                    ByteBuffer buff = ByteBuffer.wrap(operations[i]);
                    
                    int l = buff.getInt();
                    byte[] b = new byte[l];
                    buff.get(b);
                    
                    if ((new String(b)).equals("TIMEOUT")) {
                        
                        int n = buff.getInt();
                        
                        if (n == nextNumber) {
                            
                            logger.info("Got timeout for current block from replica {}!", msgCtxs[i].getSender());
                            
                            Set<Integer> t = timeouts.get(nextNumber);
                            if (t == null) {
                                
                                t = new HashSet<>();
                                timeouts.put(nextNumber, t);
                                
                            }
                            
                            t.add(msgCtxs[i].getSender());
                            
                            if (t.size() >= (controller.getCurrentViewF() + 1)) {
                                
                                timeout = true;
                            }
                        }
                        
                    } else if ((new String(b)).equals("STATE")) {
                                                
                        int id = buff.getInt();
                        
                        BlockchainSMMessage smsg = new BlockchainSMMessage(msgCtxs[i].getSender(),
                            cid, TOMUtil.SM_REQUEST, id, null, null, -1, -1);
                        
                        stateMsgs.add(smsg);
                        
                    }                
                    
                } else if (!noop) {
                    
                    transList.add(operations[i]);
                    ctxList.add(msgCtxs[i]);
                }
                
            }                        
            
            if (transList.size() > 0) {
                
                byte[][] transApp = new byte[transList.size()][];
                MessageContext[] ctxApp = new MessageContext[ctxList.size()];
            
                transList.toArray(transApp);
                ctxList.toArray(ctxApp);
                       
                //byte[][] resultsApp = executeBatch(transApp, ctxApp);
                byte[][] resultsApp = appExecuteBatch(transApp, ctxApp, fromConsensus);
                
                for (int i = 0; i < resultsApp.length; i++) {
                    
                    TOMMessage reply = getTOMMessage(processID,viewID,transApp[i], ctxApp[i], resultsApp[i]);
                    
                    this.results.add(reply);
                }
                
                /*if (timer != null) timer.cancel();
                timer = new Timer();

                timer.schedule(new TimerTask() {

                    @Override
                    public void run() {

                        logger.info("Timeout for block {}, asking to close it", nextNumber);

                        ByteBuffer buff = ByteBuffer.allocate("TIMEOUT".getBytes().length + (Integer.BYTES * 2));
                        buff.putInt("TIMEOUT".getBytes().length);
                        buff.put("TIMEOUT".getBytes());
                        buff.putInt(nextNumber);

                        sendTimeout(buff.array());
                    }
                    
                }, config.getLogBatchTimeout());*/
            }
            
            boolean isCheckpoint = cid % config.getCheckpointPeriod() == 0;
            
            //if (timeout | isCheckpoint ||  (cid % config.getLogBatchLimit() == 0)
            //        /*(this.results.size() > config.getMaxBatchSize() * config.getLogBatchLimit())*/) {
                
                byte[] transHash = log.markEndTransactions()[0];
                
                if (isCheckpoint) {
                    
                    logger.info("Performing checkpoint at CID {}", cid);
                    
                    log.clearCached();
                    lastCheckpoint = cid;
                    
                    appState = getSnapshot();
                    appStateHash = TOMUtil.computeHash(appState);
                    
                    logger.info("Storing checkpoint at CID {}", cid);
                    
                    writeCheckpointToDisk(cid, appState);
                }
                
                log.storeHeader(nextNumber, lastCheckpoint, lastReconfig, transHash, new byte[0], lastBlockHash);
                
                lastBlockHash = computeBlockHash(nextNumber, lastCheckpoint, lastReconfig, transHash, lastBlockHash);
                nextNumber++;
                                
                replies = new TOMMessage[this.results.size()];
                
                this.results.toArray(replies);
                this.results.clear();
                
                logger.info("Synching log at CID " + cid);
                
                log.sync();
                
                if (isCheckpoint) {
                    log.startNewFile(cid, config.getCheckpointPeriod());
                }
                
                if (isCheckpoint) log.startNewFile(nextNumber, config.getCheckpointPeriod());
                
                timeouts.remove(nextNumber-1);
            //}
            
            for (SMMessage smsg : stateMsgs) {
                
                stateManager.SMRequestDeliver(smsg, config.isBFT());
            }
            
            stateMsgs.clear();
            
            return replies;
        } catch (IOException | NoSuchAlgorithmException | InterruptedException ex) {
            logger.error("Error while logging/executing batch for CID " + cid, ex);
            return new TOMMessage[0];
        } 
                
    }
    
    private void sendTimeout(byte[] payload) {
        
        try {
            
            timerLock.lock();
                    
            TOMMessage timeoutMsg = TOMgen.getNextOrdered(payload);
            
            byte[] data = TOMMessageGenerator.serializeTOMMsg(timeoutMsg);
            
            timeoutMsg.serializedMessage = data;
            
            if (config.getUseSignatures() == 1) {
                
                timeoutMsg.serializedMessageSignature = TOMUtil.signMessage(controller.getStaticConf().getPrivateKey(), data);
                timeoutMsg.signed = true;
                
            }
            
            commSystem.send(controller.getCurrentViewAcceptors(),
                    new ForwardedMessage(this.controller.getStaticConf().getProcessId(), timeoutMsg));
            
        } catch (IOException ex) {
            logger.error("Error while sending timeout message.", ex);
        } finally {
            
            timerLock.unlock();
        }
        
    }
    
    private byte[] computeBlockHash(int number, int lastCheckpoint, int lastReconf,  byte[] transHash,  byte[] prevBlock) throws NoSuchAlgorithmException {
    
        ByteBuffer buff = ByteBuffer.allocate(Integer.BYTES * 5 + (prevBlock.length + transHash.length));
        
        buff.putInt(number);
        buff.putInt(lastCheckpoint);
        buff.putInt(lastReconf);

        buff.putInt(transHash.length);
        buff.put(transHash);
        
        buff.putInt(prevBlock.length);
        buff.put(prevBlock);

        MessageDigest md = TOMUtil.getHashEngine();
        
        return md.digest(buff.array());
    }
    
    private void initLog() throws FileNotFoundException, NoSuchAlgorithmException {
        
        if (config.getLogBatchType().equalsIgnoreCase("buffer")) {
            log = BufferBatchLogger.getInstance(config.getProcessId(), batchDir);
        } else if(config.getLogBatchType().equalsIgnoreCase("parallel")) {
            log = ParallelBatchLogger.getInstance(config.getProcessId(), batchDir);
        } else if(config.getLogBatchType().equalsIgnoreCase("async")) {
            log = AsyncBatchLogger.getInstance(config.getProcessId(), batchDir);
        } else {
            log = VoidBatchLogger.getInstance(config.getProcessId(), batchDir);
        }
        
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
    
    private void writeCheckpointToDisk(int cid, byte[] checkpoint) throws IOException {
        
        String checkpointPath = batchDir + "checkpoint." + config.getProcessId() + "." + String.valueOf(cid) + ".log";
        
        RandomAccessFile log = new RandomAccessFile(checkpointPath, "rwd");
        
        log.write(checkpoint);
        
        log.close();
    }
    
    @Override
    public byte[][] executeBatch(byte[][] operations, MessageContext[] msgCtxs) {

        //not used
        return null;
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
