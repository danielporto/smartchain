/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.tom.server.defaultservices.blockchain;

import bftsmart.reconfiguration.ServerViewController;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

/**
 *
 * @author joao
 */
public class TOMMessageGenerator {
    
    private int id;
    
    private int session;
    private int unorderedSeq;
    private int orderedSeq;
    private int requestID;
    
    private ServerViewController controller;
    
    public TOMMessageGenerator(ServerViewController controller) {
        
        this.controller = controller;
        
        id = this.controller.getStaticConf().getProcessId();
        
        Random rand = new Random(System.nanoTime());
        
        session = rand.nextInt();
        requestID = 0;
        unorderedSeq = 0;
        orderedSeq = 0;
    }
    
    public TOMMessage getNextOrdered(byte[] payload) {
        
        TOMMessage ret = new TOMMessage(id,
                    session, orderedSeq, requestID, payload, controller.getCurrentViewId(), TOMMessageType.ORDERED_REQUEST);
        
        requestID++;
        orderedSeq++;
        
        return ret;
    }
    
    public TOMMessage getNextUnordered(byte[] payload) {
        
        TOMMessage ret = new TOMMessage(id,
                    session, unorderedSeq, requestID, payload, controller.getCurrentViewId(), TOMMessageType.UNORDERED_REQUEST);
        
        requestID++;
        unorderedSeq++;
        
        return ret;
    }
    
    public static byte[] serializeTOMMsg(TOMMessage msg) throws IOException {
        
        DataOutputStream dos = null;
            byte[] data = null;
            
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        dos = new DataOutputStream(baos);
        msg.wExternal(dos);
        dos.flush();
        return baos.toByteArray();
    }
}
