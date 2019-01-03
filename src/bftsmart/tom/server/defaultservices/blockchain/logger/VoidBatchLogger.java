/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.tom.server.defaultservices.blockchain.logger;

import bftsmart.tom.server.defaultservices.blockchain.BatchLogger;
import bftsmart.tom.MessageContext;
import bftsmart.tom.server.defaultservices.CommandsInfo;
import bftsmart.tom.util.TOMUtil;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author joao
 */
public class VoidBatchLogger implements BatchLogger {
    
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    
    private int id;
    private int lastCachedCID = -1;
    private int firstCachedCID = -1;
    private int lastStoredCID = -1;
    private TreeMap<Integer,CommandsInfo> cachedBatches;
    private TreeMap<Integer,byte[][]> cachedResults;
    private TreeMap<Integer,byte[]> cachedHeaders;
    private TreeMap<Integer,byte[]> cachedCertificates;
    private MessageDigest transDigest;
    private MessageDigest resultsDigest;
    
    private VoidBatchLogger() {
        //not to be used
        
    }
    
    private VoidBatchLogger(int id, String logDir) throws FileNotFoundException, NoSuchAlgorithmException {
        this.id = id;
        
        cachedBatches = new TreeMap<>();
        cachedResults = new TreeMap<>();
        cachedHeaders = new TreeMap<>();
        cachedCertificates = new TreeMap<>();
        
        File directory = new File(logDir);
        if (!directory.exists()) directory.mkdir();
         
        transDigest = TOMUtil.getHashEngine();
        resultsDigest = TOMUtil.getHashEngine();
        
        logger.info("Void batch logger instantiated");

    }
    
    public void startNewFile(int cid, int period) throws IOException {
        
        //nothing to do
        
    }
    
    public void openFile(int cid, int period) throws IOException {
        
        //nothing
    }
    
    public static BatchLogger getInstance(int id, String logDir) throws FileNotFoundException, NoSuchAlgorithmException {
        VoidBatchLogger ret = new VoidBatchLogger(id, logDir);
        return ret;
    }
    
    public void storeTransactions(int cid, byte[][] requests, MessageContext[] contexts) throws IOException, InterruptedException {
        
        if (firstCachedCID == -1) firstCachedCID = cid;
        lastCachedCID = cid;
        lastStoredCID = cid;
        CommandsInfo cmds = new CommandsInfo(requests, contexts);
        cachedBatches.put(cid, cmds);
        writeTransactionsToDisk(cid, cmds);
        
    }
    
    public void storeResults(byte[][] results) throws IOException, InterruptedException {
     
        for (int i = 0; i < results.length ; i++) {
            
            if (results[i] == null) results[i] = new byte[0];
        }
        
        cachedResults.put(lastStoredCID, results);
        writeResultsToDisk(results);
    }
    
    public byte[][] markEndTransactions() throws IOException, InterruptedException {
        
        return new byte[][] {transDigest.digest(), resultsDigest.digest()};
    }
    
    public void storeHeader(int number, int lastCheckpoint, int lastReconf,  byte[] transHash,  byte[] resultsHash,  byte[] prevBlock) throws IOException, InterruptedException {
     
        logger.debug("writting header for block #{} to disk", number);
        
        ByteBuffer buff = prepareHeader(number, lastCheckpoint, lastReconf, transHash, resultsHash, prevBlock);
        
        cachedHeaders.put(lastStoredCID, buff.array());
        
        //do nothing
        
        logger.debug("wrote header for block #{} to disk", number);
    }
    
    public void storeCertificate(Map<Integer, byte[]> sigs) throws IOException, InterruptedException {
        
        logger.debug("writting certificate to disk");
        
        ByteBuffer buff = prepareCertificate(sigs);
        
        cachedCertificates.put(lastStoredCID, buff.array());
        
        //do nothing
        
        logger.debug("wrote certificate to disk");
    }
    
    public int getLastCachedCID() {
        return lastCachedCID;
    }

    public int getFirstCachedCID() {
        return firstCachedCID;
    }
    
    public int getLastStoredCID() {
        return lastStoredCID;
    }
    
    public Map<Integer, CommandsInfo> getCachedBatches() {
        
        return cachedBatches;
        
    }
    
    public Map<Integer, byte[][]> getCachedResults() {
        
        return cachedResults;
        
    }
    
    public Map<Integer, byte[]> getCachedHeaders() {
        
        return cachedHeaders;
        
    }
    
    public Map<Integer, byte[]> getCachedCertificates() {
        
        return cachedCertificates;
        
    }
    
    public void clearCached() {
        
        cachedBatches.clear();
        cachedResults.clear();
        cachedHeaders.clear();
        cachedCertificates.clear();
        
        firstCachedCID = -1;
        lastCachedCID = -1;
    }
    
    public void setCached(int firstCID, int lastCID, Map<Integer, CommandsInfo> batches,
            Map<Integer, byte[][]> results, Map<Integer, byte[]> headers, Map<Integer, byte[]> certificates) {
        
        clearCached();
        
        
        cachedBatches.putAll(batches);
        cachedResults.putAll(results);
        cachedHeaders.putAll(headers);
        cachedCertificates.putAll(certificates);
        
        lastStoredCID = firstCID;
        firstCachedCID = firstCID;
        lastCachedCID = lastCID;
    }
    
    public void startFileFromCache(int period) throws IOException {
        
        //nothing
    }
    
    private void writeTransactionsToDisk(int cid, CommandsInfo commandsInfo) throws IOException, InterruptedException {
        
        logger.debug("writting transactios to disk");
        
        byte[] transBytes = serializeTransactions(commandsInfo);
                
        //update the transactions hash for the entire block
        transDigest.update(transBytes);
                
        logger.debug("wrote transactions to disk");

    }
    
    private void writeResultsToDisk(byte[][] results) throws IOException, InterruptedException {
        
        logger.debug("writting results to disk");
        
        for (byte[] result : results) { //update the results hash for the entire block
        
            resultsDigest.update(result);

        }
        
        logger.debug("wrote results to disk");

    }
    
    public void sync() throws IOException, InterruptedException {
        
        logger.debug("synching log to disk");

        //do nothing
        
        logger.debug("synced log to disk");
    }
    
}
