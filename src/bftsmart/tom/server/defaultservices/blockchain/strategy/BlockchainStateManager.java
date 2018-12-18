/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.tom.server.defaultservices.blockchain.strategy;

import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.standard.StandardStateManager;
import bftsmart.tom.core.DeliveryThread;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.core.messages.ForwardedMessage;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.server.defaultservices.CommandsInfo;
import bftsmart.tom.server.defaultservices.blockchain.TOMMessageGenerator;
import bftsmart.tom.util.TOMUtil;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author joao
 */
public class BlockchainStateManager extends StandardStateManager implements Runnable {
    
    private boolean containsResults;
    private boolean containsCertificate;
    private ServerSocket welcomeSocket = null;
    private String logDir = null;
    private ExecutorService outExec = null;
    private ExecutorService inExec = null;
    private TOMMessageGenerator TOMgen;

    public BlockchainStateManager (boolean containsResults, boolean containsCertificate) {
        
        this.containsResults = containsResults;
        this.containsCertificate = containsCertificate;

    }
    
    public void setTOMgen(TOMMessageGenerator TOMgen) {
        
        this.TOMgen = TOMgen;
    }
    
    @Override
    public void init(TOMLayer tomLayer, DeliveryThread dt) {
        
        super.init(tomLayer,dt);
        
        logDir =    "files".concat(System.getProperty("file.separator"));
        
        try {
            
            File directory = new File(logDir);
            if (!directory.exists()) directory.mkdir();
                
            welcomeSocket = new ServerSocket(
                    this.SVController.getStaticConf().getPort(this.SVController.getStaticConf().getProcessId()) + 2);
            
            int nWorkers = this.SVController.getStaticConf().getNumNettyWorkers();
            nWorkers = nWorkers > 0 ? nWorkers : Runtime.getRuntime().availableProcessors();
            
            outExec = Executors.newFixedThreadPool(nWorkers);
            inExec = Executors.newFixedThreadPool(nWorkers);
            
            (new Thread(this)).start();
            
        } catch (IOException ex) {
            
            logger.error("Error creating blockchain socket.",ex);
        }
    }

    @Override
    protected boolean enoughReplies() {
        
        //we override this method so that we can also verify that all other data related to blocks are consistent
        
        if (senderStates.size() > SVController.getCurrentViewF()) {
            
            int count = 0;
            
            for (ApplicationState s : senderStates.values()) {
                
                int nextNumber = -2;
                int lastReconfig = -2;
                byte[] lastBlockHash = null;
                
                Map<Integer,CommandsInfo> cachedBatches = null;
                Map<Integer,byte[][]> cachedResults = null;
                Map<Integer,byte[]> cachedHeaders = null;
                Map<Integer,byte[]> cachedCertificates = null;
    
                BlockchainState state = (BlockchainState) s;
                
                if (nextNumber == -2) nextNumber = state.nextNumber;
                if (lastReconfig == -2) lastReconfig = state.lastReconfig;
                if (lastBlockHash == null) lastBlockHash = state.lastBlockHash;
                
                if (cachedBatches == null) cachedBatches = state.cachedBatches;
                if (cachedResults == null) cachedResults = state.cachedResults;
                if (cachedHeaders == null) cachedHeaders = state.cachedHeaders;
                if (cachedCertificates == null) cachedCertificates = state.cachedCertificates;
                
                if (nextNumber == state.nextNumber && lastReconfig == state.lastReconfig && Arrays.equals(lastBlockHash, state.lastBlockHash)
                        && state.cachedBatches.equals(cachedBatches) && state.cachedResults.equals(cachedResults) && state.cachedHeaders.equals(cachedHeaders)) {
                 
                    
                    //TODO: verify certificates
                    
                    count++;
                    
                    
                }
                
            }
            
            return count > SVController.getCurrentViewF();
        }
        else return false;
    }
    
    @Override
    protected void requestState() {
        
        try {
            
            ordered = true;
            changeReplica(); // always ask the state/ledger to a different replica
            
            ByteBuffer buff = ByteBuffer.allocate((Integer.BYTES * 2) + "STATE".getBytes().length);
            buff.putInt("STATE".getBytes().length);
            buff.put("STATE".getBytes());
            buff.putInt(replica);
            
            TOMMessage stateMsg = TOMgen.getNextOrdered(buff.array());
            
            byte[] data = TOMMessageGenerator.serializeTOMMsg(stateMsg);
            
            stateMsg.serializedMessage = data;
            
            if (SVController.getStaticConf().getUseSignatures() == 1) {
                
                stateMsg.serializedMessageSignature = TOMUtil.signMessage(SVController.getStaticConf().getPrivateKey(), data);
                stateMsg.signed = true;
                
            }
            
            tomLayer.getCommunication().send(SVController.getCurrentViewOtherAcceptors(),
                    new ForwardedMessage(SVController.getStaticConf().getProcessId(), stateMsg));
        } catch (IOException ex) {
            logger.error("Error asking for the state", ex);
        }
    }
        
    private boolean validateBlock(byte[] block) throws NoSuchAlgorithmException {
                
        ByteBuffer buff = ByteBuffer.wrap(block);
        MessageDigest transDigest = TOMUtil.getHashEngine();
        MessageDigest resultsDigest = TOMUtil.getHashEngine();
        MessageDigest headerDigest = TOMUtil.getHashEngine();

        //body
        while (true) {
            
            int cid = -1;
            int l = 0;
            byte[] trans = null;

            cid = buff.getInt();
            
            logger.info("cid: " + cid);
            
            if (cid == -1) break;
            
            l = buff.getInt();
            trans = new byte[l];

            buff.get(trans);
            
            transDigest.update(trans);
            
            if (containsResults) {
                
                int nResults = buff.getInt();
                
                for (int i = 0; i < nResults; i++) {
                    
                    l = buff.getInt();
                    byte[] res = new byte[l];
                    buff.get(res);
                    
                    resultsDigest.update(res);
                    
                }
            }
        }
                
        //header
        int number = buff.getInt();
        int lastCheckpoint = buff.getInt();
        int lastReconf = buff.getInt();

        int l = buff.getInt();
        
        byte[] transHash = new byte[l];
        buff.get(transHash);
        
        l = buff.getInt();
        
        byte[] resultsHash = new byte[l];
        buff.get(resultsHash);
        
        l = buff.getInt();
        
        byte[] prevBlock = new byte[l];
        buff.get(prevBlock);
        
        //certificate
        HashMap<Integer,byte[]> sigs = null;
        
        if (containsCertificate) {
            
            int nSigs = buff.getInt();
            sigs = new HashMap<>();
            
            for (int i = 0; i < nSigs; i++) {

                int id = buff.getInt();
                l = buff.getInt();

                byte[] sig = new byte[l];
                buff.get(sig);
                
                sigs.put(id, sig);
            }
        }
        
        //calculate hashes
        byte[] myTransHash = transDigest.digest();
        byte[] myResHash = new byte[0];
        if (containsResults) myResHash = resultsDigest.digest();
        
        boolean sameTransHash = Arrays.equals(transHash, myTransHash);
        boolean sameResHash = Arrays.equals(resultsHash, myResHash);
        
        //logger.info("[{}] Same trans hash: {}", number, sameTransHash);
        //logger.info("[{}] Same res hash: {}", number, sameResHash);
        
        if (sigs != null) {
            
            ByteBuffer header = ByteBuffer.allocate(Integer.BYTES * (containsResults ? 6 : 5) 
                + (prevBlock.length + transHash.length + (containsResults ? resultsHash.length : 0)));
        
            header.putInt(number);
            header.putInt(lastCheckpoint);
            header.putInt(lastReconf);

            header.putInt(transHash.length);
            header.put(transHash);

            if (containsResults) {

                header.putInt(resultsHash.length);
                header.put(resultsHash);
            }

            header.putInt(prevBlock.length);
            header.put(prevBlock);
        
            byte[] headerHash = headerDigest.digest(header.array());
            int count = 0;
            
            for (int id : sigs.keySet()) {
                
                if (TOMUtil.verifySignature(SVController.getStaticConf().getPublicKey(id), headerHash, sigs.get(id))) count++;
                
            }
            
            logger.info("[{}] Number of valid sigs: {}/{}", number, count,sigs.size());
            
            //TODO: there is an issue related this certificate and hah headers. Hash headers a not deterministic because of the consensus
            //proof contained in the context object. I cannot hack the context object with a transient proof because that will mess with
            //this state transfer manager. So I perform signature verification just to create the overhead necessary for experimental evaluation.
        }
        
        return true;
    }
    public void fetchBlocks(int lastCID) {
        
        File directory = new File(logDir);
        
        File [] files = directory.listFiles((File pathname) -> 
                pathname.getName().startsWith(""+ SVController.getStaticConf().getProcessId()) && pathname.getName().endsWith(".log"));
        
        int[] cids = new int[files.length]; 
        
        for (int i = 0; i < files.length; i++) {
            
            String[] tokens = files[i].getName().split("[.]");
                        
            logger.info("Got cids {} through {}",tokens[1],tokens[2]);
            cids[i] = new Integer(tokens[1]).intValue();
            
        }
            
        Arrays.sort(cids);
        
        int myLastCID = cids[cids.length-1]; 
        
        try {
            logger.info("Fetching blocks from {} to {} (exclusively) from replica {} at port {}",
                    myLastCID, lastCID,SVController.getCurrentView().getAddress(replica).getHostName(),SVController.getStaticConf().getPort(replica) + 2);
            
            final CountDownLatch latch = new CountDownLatch((lastCID-myLastCID) / SVController.getStaticConf().getCheckpointPeriod());
            
            for (int i = myLastCID; i < lastCID; i += SVController.getStaticConf().getCheckpointPeriod()) {
                
                final int cid = i;
                                    
                Socket clientSocket = new Socket( SVController.getCurrentView().getAddress(replica).getHostName() , SVController.getStaticConf().getPort(replica) + 2 );

                inExec.submit(new Thread() {

                    @Override
                    public void run() {

                        try {

                            byte[] aByte = new byte[1];
                            int bytesRead;

                            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
                            InputStream inFromServer = clientSocket.getInputStream();

                            outToServer.writeInt(cid);

                            String blockPath = logDir + String.valueOf(SVController.getStaticConf().getProcessId()) +
                                    "." + cid + "." +  (cid+SVController.getStaticConf().getCheckpointPeriod()) + ".log";

                            ByteArrayOutputStream baos = new ByteArrayOutputStream();

                            FileOutputStream fos = new FileOutputStream( blockPath );
                            BufferedOutputStream bos = new BufferedOutputStream(fos);

                            bytesRead = inFromServer.read(aByte, 0, aByte.length);

                            do {
                                baos.write(aByte);
                                bytesRead = inFromServer.read(aByte);
                            } while (bytesRead != -1);
                            
                            logger.info("finished cids {} through {}", cid, (cid+SVController.getStaticConf().getCheckpointPeriod()));
                            
                            byte[] block = baos.toByteArray();
                            
                            validateBlock(block);

                            bos.write(block);
                            bos.flush();
                            bos.close();
                            baos.close();
                            fos.close();
                            outToServer.close();
                            inFromServer.close();

                            clientSocket.close();


                        } catch (NoSuchAlgorithmException | IOException ex) {
                            Logger.getLogger(BlockchainStateManager.class.getName()).log(Level.SEVERE, null, ex);

                        } finally {

                            latch.countDown();
                        }

                    }
                });
                    
            }
            latch.await();
            //System.exit(0);
        } catch (IOException | InterruptedException ex) {
            
            logger.error("Interruption error", ex);
        }
    }
    
    @Override
    public void run() {
        
        try {
            
            logger.info("Waiting for block requests at port {}", welcomeSocket.getLocalPort());
            
            while (true) {
            
                Socket connectionSocket = welcomeSocket.accept();
                
                outExec.submit(new Thread() {
                    
                    @Override
                    public void run() {
                    
                        DataInputStream inToClient = null;
                        BufferedOutputStream outFromClient = null;
                        
                        try {
                            
                            inToClient = new DataInputStream(connectionSocket.getInputStream());
                            outFromClient = new BufferedOutputStream(connectionSocket.getOutputStream());
                            
                            int blockNumber = inToClient.readInt();
                            
                            String blockPath = logDir + 
                                    String.valueOf(SVController.getStaticConf().getProcessId()) + "." + blockNumber + ".log";
                            
                            File blockFile = new File(blockPath);
                            byte[] filearray = new byte[(int) blockFile.length()];

                            FileInputStream fis = new FileInputStream(blockFile);
                            BufferedInputStream bis = new BufferedInputStream(fis);
                                                                             
                            bis.read(filearray, 0, filearray.length);
                            outFromClient.write(filearray, 0, filearray.length);
                            outFromClient.flush();
                            outFromClient.close();
                            inToClient.close();
                            connectionSocket.close();
                            
                            bis.close();
                            fis.close();
                            
                        } catch (IOException ex) {
                            
                            logger.error("Socket error.",ex);
                        }
                                        
                    }
                });
            }
            
        } catch (IOException ex) {
            logger.error("Socket error.",ex);
        }
    }
    
    
}
