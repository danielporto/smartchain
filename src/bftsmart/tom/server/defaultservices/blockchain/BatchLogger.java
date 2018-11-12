/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.tom.server.defaultservices.blockchain;

import bftsmart.tom.MessageContext;
import bftsmart.tom.server.defaultservices.CommandsInfo;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
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
    private RandomAccessFile log;
    private String logPath;
    
    private BatchLogger() {
        //not to be used
        
    }
    
    private BatchLogger(int id, String logDir) throws FileNotFoundException {
        this.id = id;
        cachedBatches = new LinkedList<>();
        
        File directory = new File(logDir);
        if (!directory.exists()) directory.mkdir();
        
        logPath = logDir + String.valueOf(this.id) + "." + System.currentTimeMillis() + ".log";
        
        logger.debug("Logging to file " + logPath);
        log = new RandomAccessFile(logPath, "rw");

    }
    
    public static BatchLogger getInstance(int id, String logDir) throws FileNotFoundException {
        BatchLogger ret = new BatchLogger(id, logDir);
        return ret;
    }
    
    public void store(int cid, byte[][] requests, MessageContext[] contexts) throws IOException {
        
        if (firstCachedCID == -1) firstCachedCID = cid;
        lastCachedCID = cid;
        CommandsInfo cmds = new CommandsInfo(requests, contexts);
        cachedBatches.add(cmds);
        writeToDisk(cid, cmds);
        
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
    
    private void writeToDisk(int cid, CommandsInfo commandsInfo) throws IOException {
        
        logger.debug("writting to disk");
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(commandsInfo);
        oos.flush();

        byte[] batchBytes = bos.toByteArray();
        oos.close();
        bos.close();

        ByteBuffer bf = ByteBuffer.allocate((2 * Integer.BYTES) + batchBytes.length);
        bf.putInt(cid);
        bf.putInt(batchBytes.length);
        bf.put(batchBytes);
        //bf.putInt(EOF);
        //bf.putInt(consensusId);

        log.write(bf.array());
        
        logger.debug("wrote to disk");

        //log.seek(log.length() - 2 * Integer.BYTES);// Next write will overwrite the EOF mark

    }
    
    
    public void sync() throws IOException {
        
        logger.debug("synching to disk");

        //log.getFD().sync();
        log.getChannel().force(false);
        
        logger.debug("synced to disk");
    }
    

}
