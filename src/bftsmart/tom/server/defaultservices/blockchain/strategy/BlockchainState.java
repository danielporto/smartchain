/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.tom.server.defaultservices.blockchain.strategy;

import bftsmart.tom.server.defaultservices.CommandsInfo;
import bftsmart.tom.server.defaultservices.DefaultApplicationState;

/**
 *
 * @author joao
 */
public class BlockchainState extends DefaultApplicationState {
    
    protected int nextNumber;
    protected int lastReconfig;
    protected byte[] lastBlockHash;
    
    public BlockchainState(CommandsInfo[] messageBatches, int lastCheckpointCID, int lastCID,
            byte[] state, byte[] stateHash, int pid, int nextNumber, int lastReconfig, byte[] lastBlockHash) {
        
        super(messageBatches, lastCheckpointCID, lastCID, state, stateHash,pid);
        
        this.nextNumber = nextNumber;
        this.lastReconfig = lastReconfig;
        this.lastBlockHash = lastBlockHash;
    }
    
    public BlockchainState(CommandsInfo[] messageBatches, byte[] logHash, int lastCheckpointCID, int lastCID,
            byte[] state, byte[] stateHash, int pid, int nextNumber, int lastReconfig, byte[] lastBlockHash) {
        
        super(messageBatches, logHash, lastCheckpointCID, lastCID, state, stateHash,pid);
        
        this.nextNumber = nextNumber;
        this.lastReconfig = lastReconfig;
        this.lastBlockHash = lastBlockHash;
    }
    
    public BlockchainState() {
        
        super();
        
        this.nextNumber = -1;
        this.lastReconfig = -1;
        this.lastBlockHash = null;
    }

    public int getNextNumber() {
        return nextNumber;
    }

    public int getLastReconfig() {
        return lastReconfig;
    }

    public byte[] getLastBlockHash() {
        return lastBlockHash;
    }
}
