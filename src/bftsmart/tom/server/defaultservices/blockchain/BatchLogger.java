/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.tom.server.defaultservices.blockchain;

import bftsmart.tom.MessageContext;
import bftsmart.tom.server.defaultservices.CommandsInfo;
import bftsmart.tom.util.TOMUtil;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author joao
 */
public class BatchLogger {
    
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    
    private int id;
    private int lastCachedCID = -1;
    private int firstCachedCID = -1;
    private LinkedList<CommandsInfo> cachedBatches;
    private LinkedList<byte[][]> cachedResults;
    private RandomAccessFile log;
    private FileChannel channel;
    private String logPath;
    private MessageDigest transDigest;
    private MessageDigest resultsDigest;
    
    private BatchLogger() {
        //not to be used
        
    }
    
    private BatchLogger(int id, String logDir) throws FileNotFoundException, NoSuchAlgorithmException {
        this.id = id;
        cachedBatches = new LinkedList<>();
        cachedResults = new LinkedList<>();
        
        File directory = new File(logDir);
        if (!directory.exists()) directory.mkdir();
        
        logPath = logDir + String.valueOf(this.id) + "." + System.currentTimeMillis() + ".log";
        
        logger.debug("Logging to file " + logPath);
        log = new RandomAccessFile(logPath, "rw");
        channel = log.getChannel();
         
        transDigest = TOMUtil.getHashEngine();
        resultsDigest = TOMUtil.getHashEngine();

    }
    
    public static BatchLogger getInstance(int id, String logDir) throws FileNotFoundException, NoSuchAlgorithmException {
        BatchLogger ret = new BatchLogger(id, logDir);
        return ret;
    }
    
    public void storeTransactions(int cid, byte[][] requests, MessageContext[] contexts) throws IOException {
        
        if (firstCachedCID == -1) firstCachedCID = cid;
        lastCachedCID = cid;
        CommandsInfo cmds = new CommandsInfo(requests, contexts);
        cachedBatches.add(cmds);
        writeTransactionsToDisk(cid, cmds);
        
    }
    
    public void storeResults(byte[][] results) throws IOException{
     
        cachedResults.add(results);
        writeResultsToDisk(results);
    }
    
    public byte[][] markEndTransactions() throws IOException {
        
        ByteBuffer buff = ByteBuffer.allocate(Integer.BYTES);
        buff.putInt(-1);
        
        buff.flip();
        
        channel.write(buff);
        return new byte[][] {transDigest.digest(), resultsDigest.digest()};
    }
    
    public void storeHeader(int number, int lastCheckpoint, int lastReconf,  byte[] transHash,  byte[] resultsHash,  byte[] prevBlock) throws IOException {
     
        logger.debug("writting header for block #{} to disk", number);
        
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
        
        buff.flip();
                
        channel.write(buff);
        
        logger.debug("wrote header for block #{} to disk", number);
    }
    
    public void storeCertificate(Map<Integer, byte[]> sigs) throws IOException {
        
        logger.debug("writting certificate to disk");
        
        
        int certSize = 0;
        for (int id : sigs.keySet()) {
            
            certSize += sigs.get(id).length;
        }
        
        ByteBuffer buff = ByteBuffer.allocate(Integer.BYTES * (1 + (sigs.size() * 2)) + (certSize));
        
        buff.putInt(sigs.size());
        
        for (int id : sigs.keySet()) {
        
            buff.putInt(id);
            buff.putInt(sigs.get(id).length);
            buff.put(sigs.get(id));
        }
        
        buff.flip();
        
        channel.write(buff);
        
        logger.debug("wrote certificate to disk");
    }
    
    public int getLastCachedCID() {
        return lastCachedCID;
    }

    public int getFirstCachedCID() {
        return firstCachedCID;
    }
    
    public CommandsInfo[] getCached() {
        
        CommandsInfo[] cmds = new CommandsInfo[cachedBatches.size()];
        cachedBatches.toArray(cmds);
        return cmds;
        
    }
    public void clearCached() {
        
        cachedBatches.clear();
        firstCachedCID = -1;
        lastCachedCID = -1;
    }
    
    private void writeTransactionsToDisk(int cid, CommandsInfo commandsInfo) throws IOException {
        
        logger.debug("writting transactios to disk");
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(commandsInfo);
        oos.flush();

        byte[] transBytes = bos.toByteArray();
        oos.close();
        bos.close();
        
        //update the transactions hash for the entire block
        transDigest.update(transBytes);

        ByteBuffer buff = ByteBuffer.allocate((Integer.BYTES * 2) + transBytes.length);
        
        buff.putInt(cid);
        buff.putInt(transBytes.length);
        buff.put(transBytes);
        
        buff.flip();
        
        channel.write(buff);
        
        logger.debug("wrote transactions to disk");

    }
    
    private void writeResultsToDisk(byte[][] results) throws IOException {
        
        logger.debug("writting transactios to disk");
        
        int resultsSize = 0;
        for (byte[] result : results) {
            resultsSize += result.length;
        }
        
        ByteBuffer buff = ByteBuffer.allocate(Integer.BYTES * (1 + results.length) + (resultsSize));
        
        buff.putInt(results.length);
        
        for (byte[] result : results) {
        
            //update the results hash for the entire block
            resultsDigest.update(result);

            buff.putInt(result.length);
            buff.put(result);
        }
        
        buff.flip();
        
        channel.write(buff);
        
        logger.debug("wrote transactions to disk");

    }
    
    public void sync() throws IOException {
        
        logger.debug("synching log to disk");

        //log.getFD().sync();
        channel.force(false);
        
        logger.debug("synced log to disk");
    }
    

}
