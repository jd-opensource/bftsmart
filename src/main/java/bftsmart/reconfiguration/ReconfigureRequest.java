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
package bftsmart.reconfiguration;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Hashtable;
import java.util.Iterator;

/**
 *
 * @author eduardo
 */
public class ReconfigureRequest implements Externalizable{

    private int sender;
    private Hashtable<Integer,String> properties = new Hashtable<Integer,String>();
    private byte[] extendInfo;
    private byte[] signature;
    
    
    public ReconfigureRequest() {
    }
    
    public ReconfigureRequest(int sender) {
        this.sender = sender;
    }

    public void setSignature(byte[] signature) {
        this.signature = signature;
    }

    public byte[] getSignature() {
        return signature;
    }

    public void setExtendInfo(byte[] extendInfo) {
        this.extendInfo = extendInfo;
    }

    public byte[] getExtendInfo() {
        return extendInfo;
    }

    public Hashtable<Integer, String> getProperties() {
        return properties;
    }

    public int getSender() {
        return sender;
    }
    
    public void setProperty(int prop, String value){
        this.properties.put(prop, value);
    }
    
     @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(sender);
        
        int num = properties.keySet().size();
        
        out.writeInt(num);
        
        Iterator<Integer> it = properties.keySet().iterator() ;
        
        while(it.hasNext()){
            int key = it.next();
            String value = properties.get(key);
            
            out.writeInt(key);
            out.writeUTF(value);
        }
        
        if (signature == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(signature.length);
            out.write(signature);
        }

        if (extendInfo == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(extendInfo.length);
            out.write(extendInfo);
        }
    }

     
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        sender = in.readInt();
        
        int num = in.readInt();
        
        for(int i = 0; i < num; i++){
            int key = in.readInt();
            String value = in.readUTF();
            properties.put(key, value);
        }

        int signatureLen = in.readInt();
        if (signatureLen != -1) {
            this.signature = new byte[signatureLen];
            in.read(this.signature);
        }
        int extendInfoLen = in.readInt();
        if (extendInfoLen != -1) {
            this.extendInfo = new byte[extendInfoLen];
            in.read(this.extendInfo);
        }

    }
    
    
    @Override
     public String toString(){
        String ret = "Sender :"+ sender+";";
        Iterator<Integer> it = properties.keySet().iterator() ;
        while(it.hasNext()){
            int key = it.next();
            String value = properties.get(key);
            ret = ret+key+value;
        }
        return ret;
     }
    
}
