/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.tom.server.defaultservices.blockchain;

import bftsmart.tom.MessageContext;
import bftsmart.tom.server.defaultservices.CommandsInfo;
import java.io.IOException;
import java.util.Map;


/**
 *
 * @author joao
 */
public interface BatchLogger {
    
    
    public void storeTransactions(int cid, byte[][] requests, MessageContext[] contexts) throws IOException, InterruptedException;
    
    public void storeResults(byte[][] results) throws IOException, InterruptedException;
    
    public void storeCertificate(Map<Integer, byte[]> sigs) throws IOException, InterruptedException;
    
    public void storeHeader(int number, int lastCheckpoint, int lastReconf,  byte[] transHash,  byte[] resultsHash,  byte[] prevBlock) throws IOException, InterruptedException;
    
    public byte[][] markEndTransactions() throws IOException, InterruptedException;
    
    public void sync() throws IOException, InterruptedException;
    
    public int getLastCachedCID();
    
    public int getFirstCachedCID();
    
    public CommandsInfo[] getCached();
    
    public void clearCached();
    
}
