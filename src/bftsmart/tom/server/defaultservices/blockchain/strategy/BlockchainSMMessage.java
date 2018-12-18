/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.tom.server.defaultservices.blockchain.strategy;

import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.standard.StandardSMMessage;

/**
 *
 * @author joao
 */
public class BlockchainSMMessage extends StandardSMMessage {
    
    public BlockchainSMMessage(int sender, int cid, int type, int replica, ApplicationState state, View view, int regency, int leader) {
        super(sender, cid, type, replica, state, view, regency, leader);
    }
    
    @Override
    public boolean equals(Object o) {
        
        return (o instanceof BlockchainSMMessage && ((BlockchainSMMessage) o).getSender() == getSender());
    }
    
    @Override
    public int hashCode() {
        
        return 47 * getSender();
    }
    
}
