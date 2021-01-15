package com.dassault_systemes.kafkarebalancetimer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.zookeeper.KeeperException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class RebalanceTimerMain {
	private static ZKScheduler zklogger = new ZKScheduler();

	public static void exportDataToCSV(String filePath, String[] data, String[] headers) {
		File file = new File(filePath);

		try {
			if (!file.exists()) {
				FileWriter Fwriter = new FileWriter(file, true);
				for (int i = 0; i < headers.length; i++) {
					Fwriter.append(headers[i]);
					Fwriter.append(",");
				}
				Fwriter.append("\n");
				Fwriter.close();
			}
			FileWriter Fwriter = new FileWriter(file, true);

			for (int i = 0; i < data.length; i++) {
				Fwriter.append(data[i]);
				Fwriter.append(",");
			}
			Fwriter.append("\n");
			Fwriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Properties getPropsFromFile(String path) throws IOException {
		Properties props = new Properties();
		InputStream is = RebalanceTimerMain.class.getClassLoader().getResourceAsStream(path);
		if (is != null) {
			props.load(is);
			return props;
		} else {
			throw new FileNotFoundException("propertie file path " + path + "couldn't succesfully be read");
		}
	}

	public static void main(String[] args) throws IOException, ParseException, IllegalStateException,
			InterruptedException, KeeperException, java.text.ParseException {

		String ZKRoot = "/RebalancingTimer";
		String ZKHost = getPropsFromFile("cruise-control.properties").getProperty("zk.connect");
		String executorState = HttpRestAPIRequest
				.HttpGETRequest(getPropsFromFile("cruise-control.properties").getProperty("cc.url")
						+ "/kafkacruisecontrol/state?substates=executor&json=true", "all");
		JSONObject executorStateJSON = (JSONObject) new JSONParser().parse(executorState);
		JSONObject stateJSON = (JSONObject) executorStateJSON.get("ExecutorState");

		if (stateJSON.get("state").equals("INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS")) {
			// triggeredSelfHealingTaskId or triggeredUserTaskId
			if (zklogger.checkForSimilarZnodeName(ZKRoot, ZKHost, (String) stateJSON.get("triggeredSelfHealingTaskId"))) {
				String timeData = zklogger.getZNodeData(ZKRoot + "/" + (String) stateJSON.get("triggeredSelfHealingTaskId"),
						ZKHost);

				JSONObject timeDataJSON = (JSONObject) new JSONParser().parse(timeData);
				timeDataJSON.put("ending-time", new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS").format(new Date()));
				zklogger.updateZnodeData(ZKRoot + "/" + (String) stateJSON.get("triggeredSelfHealingTaskId"), ZKHost,
						timeDataJSON.toString());

			} else {
				zklogger.writeZNodeData(ZKRoot + "/" + (String) stateJSON.get("triggeredSelfHealingTaskId"),
						("{\"starting-time\":\"" + new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS").format(new Date())
								+ "\",\"ending-time\":\""
								+ new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS").format(new Date()) + "\"}")
										.getBytes(),
						ZKHost);
			}

		} else {
			if (stateJSON.get("state").equals("NO_TASK_IN_PROGRESS")) {
				if (!zklogger.getChildren(ZKRoot, ZKHost).isEmpty()) {
					String timerZnode = zklogger.getChildren(ZKRoot, ZKHost).iterator().next();
					String timeData = zklogger.getZNodeData(ZKRoot + "/" + timerZnode, ZKHost);
					JSONObject timeDataJSON = (JSONObject) new JSONParser().parse(timeData);
					Double rebalancingTime = ((new Date().getTime() + new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS")
							.parse((String) timeDataJSON.get("ending-time")).getTime()) * 0.5
							- new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS")
									.parse((String) timeDataJSON.get("starting-time")).getTime())
							/ 1000;
					exportDataToCSV(
							getPropsFromFile("cruise-control.properties").getProperty("path.log.rebalancing.csv")
									+ "/self-healing.csv",
							new String[] { timerZnode,
									 timeDataJSON.get("starting-time").toString(),
									rebalancingTime.toString() },
							new String[] { "id", "timeStamp", "rebalancingTime(sec)" });
					zklogger.deleteZNode(ZKRoot + "/" + timerZnode, ZKHost);

				}

			}
		}

	}
}
