package com.dassault_systemes.kafkarebalancetimer;

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

class ZKConnector {

	private static ZooKeeper zk;
	private static java.util.concurrent.CountDownLatch connSignal = new java.util.concurrent.CountDownLatch(1);

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

	public void close() throws InterruptedException {
		zk.close();
	}
}

public class ZKScheduler {

	private static ZooKeeper zk;
	private static ZKConnector zkc;

	public void writeZNodeData(String path, byte[] data, String host)
			throws KeeperException, InterruptedException, IllegalStateException, IOException {
		zkc = new ZKConnector();
		zk = zkc.connect(host);
		zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	public String getZNodeData(String path, String host)
			throws KeeperException, InterruptedException, IllegalStateException, IOException {
		zkc = new ZKConnector();
		zk = zkc.connect(host);
		byte[] data = zk.getData(path, null, null);
		return new String(data, "UTF-8");
	}
	
	public void updateZnodeData(String path, String host, String data) throws IllegalStateException, IOException, InterruptedException, KeeperException {
		zkc = new ZKConnector();
		zk = zkc.connect(host);
		zk.setData(path, data.getBytes(), -1);
	}

	public Map<String, String> parseZNodeData(String scalingMetadata) {
		Map<String, String> parsedDataMap = new HashMap<String, String>();
		String[] splittedData = scalingMetadata.split("-");
		parsedDataMap.put("broker-id", splittedData[0]);
		parsedDataMap.put("operation", splittedData[1]);

		return parsedDataMap;

	}

	public void deleteZNode(String path, String host)
			throws KeeperException, InterruptedException, IllegalStateException, IOException {
		zkc = new ZKConnector();
		zk = zkc.connect(host);
		zk.delete(path, -1);
	}
	
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
	
	
	

	public boolean checkForSimilarZnode(String pathRoot, String host, String data)
			throws IllegalStateException, IOException, InterruptedException, KeeperException {
		zkc = new ZKConnector();
		zk = zkc.connect(host);
		List<String> ChildrenZNodes = zk.getChildren(pathRoot, true);
		Iterator<String> itrChild = ChildrenZNodes.iterator();
		try {
			while (itrChild.hasNext()) {
				if (getZNodeData(pathRoot + "/" + itrChild.next(), host).equals(data)) {
					return true;
				}
			}
			return false;
		} catch (NoSuchElementException emptyZnodeRootError) {
			return false;
		}
	}

	public boolean checkForSimilarZnodeName(String pathRoot, String host, String znodeName)
			throws IllegalStateException, IOException, InterruptedException, KeeperException {
		zkc = new ZKConnector();
		zk = zkc.connect(host);
		List<String> ChildrenZNodes = zk.getChildren(pathRoot, true);
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

	public static void main(String[] args)
			throws IllegalStateException, IOException, InterruptedException, KeeperException, ParseException {
		ZKScheduler zklogger = new ZKScheduler();
		Iterator<String> itr = zklogger.getChildren("/RebalancingTimer", "34.243.221.164,18.200.134.230,54.77.52.77").iterator();
		while (itr.hasNext()) {
			System.out.println(itr.next());
		}
	}
}
