/**
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bftsmart.reconfiguration.util;

import bftsmart.tom.util.KeyLoader;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.spec.EncodedKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;

/**
 * Used to load JCA public and private keys from conf/keys/publickey<id> and
 * conf/keys/privatekey<id>
 */
public class ECDSAKeyLoader implements KeyLoader {

    private String path;
    private int id;
    private PrivateKey priKey;

    private String sigAlgorithm;
    
    private Map<Integer, PublicKey> pubKeys;

    private static String DEFAULT_PKEY = "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgXa3mln4anewXtqrM" +
                                    "hMw6mfZhslkRa/j9P790ToKjlsihRANCAARnxLhXvU4EmnIwhVl3Bh0VcByQi2um" +
                                    "9KsJ/QdCDjRZb1dKg447voj5SZ8SSZOUglc/v8DJFFJFTfygjwi+27gz";
    
    private static String DEFAULT_UKEY =  "MIICNjCCAd2gAwIBAgIRAMnf9/dmV9RvCCVw9pZQUfUwCgYIKoZIzj0EAwIwgYEx" +
                                    "CzAJBgNVBAYTAlVTMRMwEQYDVQQIEwpDYWxpZm9ybmlhMRYwFAYDVQQHEw1TYW4g" +
                                    "RnJhbmNpc2NvMRkwFwYDVQQKExBvcmcxLmV4YW1wbGUuY29tMQwwCgYDVQQLEwND" +
                                    "T1AxHDAaBgNVBAMTE2NhLm9yZzEuZXhhbXBsZS5jb20wHhcNMTcxMTEyMTM0MTEx" +
                                    "WhcNMjcxMTEwMTM0MTExWjBpMQswCQYDVQQGEwJVUzETMBEGA1UECBMKQ2FsaWZv" +
                                    "cm5pYTEWMBQGA1UEBxMNU2FuIEZyYW5jaXNjbzEMMAoGA1UECxMDQ09QMR8wHQYD" +
                                    "VQQDExZwZWVyMC5vcmcxLmV4YW1wbGUuY29tMFkwEwYHKoZIzj0CAQYIKoZIzj0D" +
                                    "AQcDQgAEZ8S4V71OBJpyMIVZdwYdFXAckItrpvSrCf0HQg40WW9XSoOOO76I+Umf" +
                                    "EkmTlIJXP7/AyRRSRU38oI8Ivtu4M6NNMEswDgYDVR0PAQH/BAQDAgeAMAwGA1Ud" +
                                    "EwEB/wQCMAAwKwYDVR0jBCQwIoAginORIhnPEFZUhXm6eWBkm7K7Zc8R4/z7LW4H" +
                                    "ossDlCswCgYIKoZIzj0EAwIDRwAwRAIgVikIUZzgfuFsGLQHWJUVJCU7pDaETkaz" +
                                    "PzFgsCiLxUACICgzJYlW7nvZxP7b6tbeu3t8mrhMXQs956mD4+BoKuNI";

    private boolean defaultKeys;

    /** Creates a new instance of RSAKeyLoader */
    public ECDSAKeyLoader(int id, String configHome, boolean defaultKeys, String sigAlgorithm) {

            this.id = id;
            this.defaultKeys = defaultKeys;
            this.sigAlgorithm = sigAlgorithm;
            
            if (configHome.equals("")) {
                    path = "config" + System.getProperty("file.separator") + "ecdsakeys" +
                                    System.getProperty("file.separator");
            } else {
                    path = configHome + System.getProperty("file.separator") + "ecdsakeys" +
                                    System.getProperty("file.separator");
            }
            
            pubKeys = new HashMap<>();
    }

    /**
     * Loads the public key of some processes from configuration files
     *
     * @return the PublicKey loaded from config/keys/publickey<id>
     * @throws Exception problems reading or parsing the key
     */
    public PublicKey loadPublicKey(int id) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, CertificateException {

            if (defaultKeys) {
                return getPublicKeyFromString(ECDSAKeyLoader.DEFAULT_UKEY);
            }
            
            PublicKey ret = pubKeys.get(id);
            
            if (ret == null) {
                
                FileReader f = new FileReader(path + "publickey" + id);
                BufferedReader r = new BufferedReader(f);
                String tmp = "";
                String key = "";
                while ((tmp = r.readLine()) != null) {
                        key = key + tmp;
                }
                f.close();
                r.close();
                ret = getPublicKeyFromString(key);
                
                pubKeys.put(id, ret);
            }
            
            return ret;
    }

    public PublicKey loadPublicKey() throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, CertificateException {

            if (defaultKeys) {                    
                return getPublicKeyFromString(ECDSAKeyLoader.DEFAULT_UKEY);
            }
            
            PublicKey ret = pubKeys.get(this.id);

            if (ret == null) {
                                
                FileReader f = new FileReader(path + "publickey" + this.id);
                BufferedReader r = new BufferedReader(f);
                String tmp = "";
                String key = "";
                while ((tmp = r.readLine()) != null) {
                        key = key + tmp;
                }
                f.close();
                r.close();
                ret = getPublicKeyFromString(key);
                
                pubKeys.put(this.id, ret);
            }
            
            return ret;
    }

    /**
     * Loads the private key of this process
     *
     * @return the PrivateKey loaded from config/keys/publickey<conf.getProcessId()>
     * @throws Exception problems reading or parsing the key
     */
    public PrivateKey loadPrivateKey() throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {

            if (defaultKeys) {
                return getPrivateKeyFromString(ECDSAKeyLoader.DEFAULT_PKEY);
            }

            if (priKey == null) {
                    FileReader f = new FileReader(path + "privatekey" + this.id);
                    BufferedReader r = new BufferedReader(f);
                    String tmp = "";
                    String key = "";
                    while ((tmp = r.readLine()) != null) {
                            key = key + tmp;
                    }
                    f.close();
                    r.close();
                    priKey = getPrivateKeyFromString(key);
            }
            return priKey;
    }

    //utility methods for going from string to public/private key
    private PrivateKey getPrivateKeyFromString(String key) throws NoSuchAlgorithmException, InvalidKeySpecException {
            KeyFactory keyFactory = KeyFactory.getInstance("EC");
            EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(Base64.decodeBase64(key));
            PrivateKey privateKey = keyFactory.generatePrivate(privateKeySpec);
            return privateKey;
    }

    private PublicKey getPublicKeyFromString(String key) throws NoSuchAlgorithmException, InvalidKeySpecException, CertificateException {
            KeyFactory keyFactory = KeyFactory.getInstance("EC");
            CertificateFactory kf = CertificateFactory.getInstance("X.509");
            InputStream certstream = new ByteArrayInputStream (Base64.decodeBase64(key));
            
            return kf.generateCertificate(certstream).getPublicKey();
            
            /*EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(Base64.decodeBase64(key));
            PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);
            return publicKey;*/
    }

    @Override
    public String getSignatureAlgorithm() {
        
        return this.sigAlgorithm;
    }
}
