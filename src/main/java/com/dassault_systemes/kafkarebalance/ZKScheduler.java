package com.dassault_systemes.kafkarebalance;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * Class of a ZK connector to establish/close connection with Zookeeper   
 * @author SKI44
 *
 */
class ZKConnector {

	private static ZooKeeper zk;
	private static java.util.concurrent.CountDownLatch connSignal = new java.util.concurrent.CountDownLatch(1);
	
	/**
	 * Method to establish connection to Zookeeper cluster
	 * @param host commas separated of ZK servers IPs
	 * @return the ZK object to use when the connection is established
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws IllegalStateException
	 */
	public ZooKeeper connect(String host) throws IOException, InterruptedException, IllegalStateException {
		zk = new ZooKeeper(host, 5000, new Watcher() {
			public void process(WatchedEvent event) {
				if (event.getState() == KeeperState.SyncConnected) {
					connSignal.countDown();
				}
			}
		});
		connSignal.await();
		return zk;
	}
	/**
	 * Method to close ZK connection
	 * @throws InterruptedException
	 */
	public void close() throws InterruptedException {
		zk.close();
	}
}
/**
 * Class of ZK scheduler that provides helpful primitives (methods) to schedules scaling tasks through writes and retrieves of Znodes in different ways 
 * @author SKI44
 *
 */
public class ZKScheduler {

	private static ZooKeeper zk;
	private static ZKConnector zkc;
	
	/**
	 * Creates a Znode as a child in a particular path and writes data in 
	 * @param path path to create Znode in  
	 * @param data data to write in Znode once created 
	 * @param host commas separated of ZK servers IPs
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws IllegalStateException
	 * @throws IOException
	 */
	public void writeZNodeData(String path, byte[] data, String host)
			throws KeeperException, InterruptedException, IllegalStateException, IOException {
		zkc = new ZKConnector();
		zk = zkc.connect(host);
		zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}
	/**
	 * Get data from a particular Znode
	 * @param path full path of the Znode (ending by Znode name) to get data from
	 * @param host commas separated of ZK servers IPs
	 * @return Data in the Znode from bytes to String (UTF-8)
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws IllegalStateException
	 * @throws IOException
	 */
	public String getZNodeData(String path, String host)
			throws KeeperException, InterruptedException, IllegalStateException, IOException {
		zkc = new ZKConnector();
		zk = zkc.connect(host);
		byte[] data = zk.getData(path, null, null);
		return new String(data, "UTF-8");
	}
	/**
	 * Updates data in a particular Znode 
	 * @param path full path of the Znode (ending by Znode name) to update its data 
	 * @param host commas separated of ZK servers IPs
	 * @param data data to update Znode data with 
	 * @throws IllegalStateException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void updateZnodeData(String path, String host, String data) throws IllegalStateException, IOException, InterruptedException, KeeperException {
		zkc = new ZKConnector();
		zk = zkc.connect(host);
		zk.setData(path, data.getBytes(), -1);
	}
	
	
	/**
	 * Particular method to parse Scaling data by broker id and scaling type  
	 * @param scalingMetadata scaling data in Znodes to parse 
	 * @return Map with data parsed in keys-values
	 */
	public Map<String, String> parseZNodeData(String scalingMetadata) {
		Map<String, String> parsedDataMap = new HashMap<String, String>();
		String[] splittedData = scalingMetadata.split("-");
		parsedDataMap.put("broker-id", splittedData[0]);
		parsedDataMap.put("operation", splittedData[1]);

		return parsedDataMap;

	}
	
	/**
	 * Deletes a Znode 
	 * @param path full path of the Znode (ending by Znode name) to delete
	 * @param host commas separated of ZK servers IPs
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws IllegalStateException
	 * @throws IOException
	 */
	public void deleteZNode(String path, String host)
			throws KeeperException, InterruptedException, IllegalStateException, IOException {
		zkc = new ZKConnector();
		zk = zkc.connect(host);
		zk.delete(path, -1);
	}
	
	/**
	 * Gets all children of a particular root Znode 
	 * @param path root path to get children Znodes from
	 * @param host commas separated of ZK servers IPs
	 * @return List of Znodes names (null if Znode root is a leaf)
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws IllegalStateException
	 * @throws IOException
	 * @throws ParseException
	 */
	public List<String> getChildren(String path, String host)
			throws KeeperException, InterruptedException, IllegalStateException, IOException, ParseException {
		zkc = new ZKConnector();
		zk = zkc.connect(host);
		List<String> ChildrenZNodes = zk.getChildren(path, true);
		try {
			return ChildrenZNodes;
		} catch (NoSuchElementException emptyZnodeRootError) {
			return null;
		}
	}
	
	/**
	 * Gets the first child created among Znode root children named in format "yyyy-MM-dd_HH:mm:ss.SSS"
	 * @param path root path to get children Znodes from
	 * @param host commas separated of ZK servers IPs
	 * @return the earliest Znode child created  
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws IllegalStateException
	 * @throws IOException
	 * @throws ParseException
	 */
	public String getZNodeEarliestChild(String path, String host)
			throws KeeperException, InterruptedException, IllegalStateException, IOException, ParseException {
		zkc = new ZKConnector();
		zk = zkc.connect(host);
		List<String> ChildrenZNodes = zk.getChildren(path, true);
		List<Date> ChildrenZNodesDates = new ArrayList<Date>();
		Iterator<String> itr = ChildrenZNodes.iterator();
		while (itr.hasNext()) {
			ChildrenZNodesDates.add(new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS").parse(itr.next()));
		}
		Collections.sort(ChildrenZNodesDates);
		Iterator<Date> itrDate = ChildrenZNodesDates.iterator();
		try {
			return new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS").format(itrDate.next());
		} catch (NoSuchElementException emptyZnodeRootError) {
			return "";
		}
	}
	
	
	
	/**
	 * Checks if children Znodes in a root path has same data as data-input
	 * @param path root path to get children Znodes from
	 * @param host commas separated of ZK servers IPs
	 * @param data data-input to compare with 
	 * @return true if a Znode has similar data as data input
	 * @throws IllegalStateException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public boolean checkForSimilarZnode(String path, String host, String data)
			throws IllegalStateException, IOException, InterruptedException, KeeperException {
		zkc = new ZKConnector();
		zk = zkc.connect(host);
		List<String> ChildrenZNodes = zk.getChildren(path, true);
		Iterator<String> itrChild = ChildrenZNodes.iterator();
		try {
			while (itrChild.hasNext()) {
				if (getZNodeData(path + "/" + itrChild.next(), host).equals(data)) {
					return true;
				}
			}
			return false;
		} catch (NoSuchElementException emptyZnodeRootError) {
			return false;
		}
	}
	/**
	 * hecks if children Znodes in a root path has same name as a given Znode name
	 * @param path root path to get children Znodes from
	 * @param host ommas separated of ZK servers IPs
	 * @param znodeName name of the Znode to compare with 
	 * @return true if another znode child has a same name as znodeName 
	 * @throws IllegalStateException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public boolean checkForSimilarZnodeName(String path, String host, String znodeName)
			throws IllegalStateException, IOException, InterruptedException, KeeperException {
		zkc = new ZKConnector();
		zk = zkc.connect(host);
		List<String> ChildrenZNodes = zk.getChildren(path, true);
		Iterator<String> itrChild = ChildrenZNodes.iterator();
		try {

			while (itrChild.hasNext()) {
				if (itrChild.next().equals(znodeName)) {
					return true;
				}
			}
			return false;
		} catch (NoSuchElementException emptyZnodeRootError) {
			return false;
		}

	}

	
}
