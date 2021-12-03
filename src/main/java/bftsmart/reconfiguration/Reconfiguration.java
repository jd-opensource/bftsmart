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

import bftsmart.tom.ServiceProxy;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.util.BytesUtils;
import bftsmart.tom.util.TOMUtil;
import utils.net.NetworkAddress;

/**
 *
 * @author eduardo
 */
public class Reconfiguration {

    private ReconfigureRequest request;
    private ServiceProxy proxy;
    private int id;
    
    public Reconfiguration(int id, ServiceProxy serviceProxy) {
        this.id = id;
        this.proxy = serviceProxy;
    }
    
    public void addServer(int id, NetworkAddress address){
        this.setReconfiguration(ServerViewController.ADD_SERVER, id + ":" + address.getHost() + ":" + address.getPort() + ":" + address.isSecure());
    }
    
    public void removeServer(int id){
        this.setReconfiguration(ServerViewController.REMOVE_SERVER, String.valueOf(id));
    }
    

    public void setF(int f){
      this.setReconfiguration(ServerViewController.CHANGE_F,String.valueOf(f));  
    }
    
    
    public void setReconfiguration(int prop, String value){
        if(request == null){
            //request = new ReconfigureRequest(proxy.getViewManager().getStaticConf().getProcessId());
            request = new ReconfigureRequest(id);
        }
        request.setProperty(prop, value);
    }

    public void addExtendInfo(byte[] extendInfo) {
        if (request == null) {
            throw new IllegalStateException("[Reconfiguration] addExtendInfo request is null exception!");
        }
        request.setExtendInfo(extendInfo);
    }

    
    public ReconfigureReply execute(){
        byte[] signature = TOMUtil.signMessage(proxy.getViewManager().getStaticConf().getRSAPrivateKey(),
//                                                                            request.toString().getBytes());
                                                                              BytesUtils.getBytes(request.toString()));
        request.setSignature(signature);
        byte[] reply = proxy.invoke(TOMUtil.getBytes(request), TOMMessageType.RECONFIG);
        request = null;
        return (ReconfigureReply)TOMUtil.getObject(reply);
    }
    
    
    public void close(){
        proxy.close();
        proxy = null;
    }
    
}
