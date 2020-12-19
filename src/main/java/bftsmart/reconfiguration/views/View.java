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
package bftsmart.reconfiguration.views;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author eduardo
 */
public class View implements Serializable {

	private static final long serialVersionUID = 4052550874674512359L;

	private int id;
	private int f;
	private int[] processes;
	private Map<Integer, NodeNetwork> addresses;

	public View(int id, int[] processes, int f, NodeNetwork[] addresses) {
		this.id = id;
		this.processes = processes.clone();
		Arrays.sort(this.processes);

		this.addresses = new HashMap<Integer, NodeNetwork>();
		for (int i = 0; i < this.processes.length; i++) {
			this.addresses.put(processes[i], addresses[i]);
		}
		this.f = f;
	}

	public boolean isMember(int id) {
		for (int i = 0; i < this.processes.length; i++) {
			if (this.processes[i] == id) {
				return true;
			}
		}
		return false;
	}

	public int getPos(int id) {
		for (int i = 0; i < this.processes.length; i++) {
			if (this.processes[i] == id) {
				return i;
			}
		}
		return -1;
	}

	public int getId() {
		return id;
	}

	public int getF() {
		return f;
	}

	public int getN() {
		return this.processes.length;
	}

	public int[] getProcesses() {
		return processes.clone();
	}

	public Map<Integer, NodeNetwork> getAddresses() {
		return new HashMap<>(addresses);
	}

	@Override
	public String toString() {
		String ret = "ID:" + id + "; F:" + f + "; Processes:";
		for (int i = 0; i < processes.length; i++) {
			ret = ret + processes[i] + "(" + addresses.get(processes[i]) + "),";
		}

		return ret;
	}

	public NodeNetwork getAddress(int id) {
		return addresses.get(id);
	}

	public void setAddresses(int id, NodeNetwork nodeNetwork) {
		addresses.put(id, nodeNetwork);
	}

	public int computeBFT_F() {
		return computeBFT_F(this.processes.length);
	}

	public int computeCFT_F() {
		return computeCFT_F(this.processes.length);
	}

	/**
	 * 计算当前视图在 BFT 模式下的共识法定数量，即达成共识所需要的最小赞成票数；
	 * <p>
	 * 返回结果是基于 n 计算产生的结果：
	 * <p>
	 * 1. n 为视图中的总节点数，即 {@link #getProcesses()} 的数组长度；<br>
	 * 2. f 为视图中在 n 值下的容错数，按照 f = floor((n - 1) / 3 ) 取值；<br>
	 * 3. BFT 模式的共识法定值 QUORUM = n - f ；<br>
	 * 
	 * @return
	 */
	public int computeBFT_QUORUM() {
		return this.processes.length - computeBFT_F();
	}

	/**
	 * 计算当前视图在 CFT 模式下的共识法定数量，即达成共识所需要的最小赞成票数；
	 * <p>
	 * 返回结果是基于 n 计算产生的结果：
	 * <p>
	 * 1. n 为视图中的总节点数，即 {@link #getProcesses()} 的数组长度；<br>
	 * 2. f 为视图中在 n 值下的容错数，按照 f = floor((n - 1) / 2 ) 取值；<br>
	 * 3. CFT 模式的共识法定值 QUORUM = n - f ；<br>
	 * 
	 * @return
	 */
	public int computeCFT_QUORUM() {
		return this.processes.length - computeCFT_F();
	}

	/**
	 * 计算在指定节点数量下的拜占庭容错数；
	 * 
	 * @param n
	 * @return
	 */
	public static int computeBFT_F(int n) {
		if (n < 1) {
			throw new IllegalArgumentException("Node number is less than 1!");
		}
		int f = 0;
		while (true) {
			if (n >= (3 * f + 1) && n < (3 * (f + 1) + 1)) {
				break;
			}
			f++;
		}
		return f;
	}

	public static int computeCFT_F(int n) {
		if (n < 1) {
			throw new IllegalArgumentException("Node number is less than 1!");
		}
		int f = 0;
		while (true) {
			if (n >= (2 * f + 1) && n < (2 * (f + 1) + 1)) {
				break;
			}
			f++;
		}
		return f;
	}

	/**
	 * 判断指定视图 Id 和进程 Id 列表是否与当前视图的一致；
	 * 
	 * @param viewId
	 * @param viewProcesses
	 * @return
	 */
	public boolean equals(int viewId, int[] viewProcesses) {
		return this.id == viewId && Arrays.equals(this.processes, viewProcesses);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof View) {
			View v = (View) obj;
//            return (this.addresses.equals(v.addresses) &&
			return (this.addresses.keySet().equals(v.addresses.keySet()) && Arrays.equals(this.processes, v.processes)
					&& this.id == v.id && this.f == v.f);

		}
		return false;
	}

	@Override
	public int hashCode() {
		int hash = 1;
		hash = hash * 31 + this.id;
		hash = hash * 31 + this.f;
		if (this.processes != null) {
			for (int i = 0; i < this.processes.length; i++) {
				hash = hash * 31 + this.processes[i];
			}
		} else {
			hash = hash * 31 + 0;
		}
		hash = hash * 31 + this.addresses.hashCode();
		return hash;
	}
}
