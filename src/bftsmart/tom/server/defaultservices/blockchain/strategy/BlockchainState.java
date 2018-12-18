/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.tom.server.defaultservices.blockchain.strategy;

import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.leaderchange.CertifiedDecision;
import bftsmart.tom.server.defaultservices.CommandsInfo;
import bftsmart.tom.server.defaultservices.DefaultApplicationState;
import bftsmart.tom.util.BatchBuilder;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 *
 * @author joao
 */
public class BlockchainState extends DefaultApplicationState {
    
    protected Map<Integer,CommandsInfo> cachedBatches;
    protected Map<Integer,byte[][]> cachedResults;
    protected Map<Integer,byte[]> cachedHeaders;
    protected Map<Integer,byte[]> cachedCertificates;
    
    protected int nextNumber;
    protected int lastReconfig;
    protected byte[] lastBlockHash;
    
    public BlockchainState(Map<Integer, CommandsInfo> batches, Map<Integer, byte[][]> results,
            Map<Integer, byte[]> headers, Map<Integer, byte[]> certificates, int lastCheckpointCID, int lastCID,
            byte[] state, byte[] stateHash, int pid, int nextNumber, int lastReconfig, byte[] lastBlockHash) {
        
        super(null, lastCheckpointCID, lastCID, state, stateHash,pid);
        
        this.nextNumber = nextNumber;
        this.lastReconfig = lastReconfig;
        this.lastBlockHash = lastBlockHash;
        
        cachedBatches = batches;
        cachedResults = results;
        cachedHeaders = headers;
        cachedCertificates = certificates;
        
    }
    
    public BlockchainState(Map<Integer, CommandsInfo> batches, Map<Integer, byte[][]> results,
            Map<Integer, byte[]> headers, Map<Integer, byte[]> certificates, byte[] logHash,
            int lastCheckpointCID, int lastCID,  byte[] state, byte[] stateHash, int pid, int nextNumber, int lastReconfig, byte[] lastBlockHash) {
        
        super(null, logHash, lastCheckpointCID, lastCID, state, stateHash,pid);
        
        this.nextNumber = nextNumber;
        this.lastReconfig = lastReconfig;
        this.lastBlockHash = lastBlockHash;
        
        cachedBatches = batches;
        cachedResults = results;
        cachedHeaders = headers;
        cachedCertificates = certificates;
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
    
    public Map<Integer, CommandsInfo> getBatches() {
        
        return cachedBatches;
    } 
            
    public Map<Integer, byte[][]> getResults() {
        return cachedResults;
    }
    
    public Map<Integer, byte[]> getHeaders() {

        return cachedHeaders;
    }
    
    public Map<Integer, byte[]> getCertificates() {
        
        return cachedCertificates;
    }
    
    @Override
    public CertifiedDecision getCertifiedDecision(ServerViewController controller) {
        CommandsInfo ci = cachedBatches.get(getLastCID());
        
        if (ci != null && ci.msgCtx[0].getProof() != null) { // do I have a proof for the consensus?
                        
            Set<ConsensusMessage> proof = ci.msgCtx[0].getProof();
            LinkedList<TOMMessage> requests = new LinkedList<>();
            
            //Recreate all TOMMessages ordered in the consensus
            for (int i = 0; i < ci.commands.length; i++) {
                
                requests.add(ci.msgCtx[i].recreateTOMMessage(ci.commands[i]));
                
            }
            
            //Serialize the TOMMessages to re-create the proposed value
            BatchBuilder bb = new BatchBuilder(0);
            byte[] value = bb.makeBatch(requests, ci.msgCtx[0].getNumOfNonces(),
                    ci.msgCtx[0].getSeed(), ci.msgCtx[0].getTimestamp(), controller.getStaticConf().getUseSignatures() == 1);
                        
            //Assemble and return the certified decision
            return new CertifiedDecision(pid, getLastCID(), value, proof);
        }
        else return null; // there was no proof for the consensus
    }
}
