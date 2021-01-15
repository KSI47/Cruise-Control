package com.dassault_systemes.kafkarebalance;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used to trigger Kafka scaling operations to cruise control 
 * @author SKI44
 *
 */
public class Rebalancer {
	private final static Logger logger = LoggerFactory.getLogger(Rebalancer.class);
	public static Properties getPropsFromFile(String path) throws IOException {
		Properties props = new Properties();
		InputStream is = Rebalancer.class.getClassLoader().getResourceAsStream(path);
		if (is != null) {
			props.load(is);
			return props;
		} else {
			throw new FileNotFoundException("propertie file path " + path + "couldn't succesfully be read");
		}
	}
	
	/**
	 * Check if there are any uncompleted rebalancing tasks in cruise control
	 * @return true if a rebalancing task isn't completed
	 * @throws IOException
	 * @throws ParseException
	 */
	public boolean checkActiveRebalancingTasks() throws IOException, ParseException {
		String taskList = HttpRestAPIRequest
				.HttpGETRequest(getPropsFromFile("cruise-control.properties").getProperty("cc.url")
						+ "/kafkacruisecontrol/user_tasks?&json=true", "all");

		JSONObject taskListJSON = (JSONObject) new JSONParser().parse(taskList);
		JSONArray taskListJSONArray = (JSONArray) taskListJSON.get("userTasks");
		for (Object task : taskListJSONArray) {
			JSONObject taskJSON = (JSONObject) task;
			if ((taskJSON.get("RequestURL").toString().contains("REBALANCE")
					|| taskJSON.get("RequestURL").toString().contains("rebalance")
					|| taskJSON.get("RequestURL").toString().contains("ADD_BROKER")
					|| taskJSON.get("RequestURL").toString().contains("add_broker")
					|| taskJSON.get("RequestURL").toString().contains("remove_broker")
					|| taskJSON.get("RequestURL").toString().contains("REMOVE_BROKER"))
					&& !taskJSON.get("Status").toString().contains("Completed")) {
				return true;
			}
		}
		return false;
	}
	/**
	 * Calculates metric to use for unbalancedness evaluation, which is the variation in unbalancedness score of a proposal at execution time
	 * @return difference in unbalancedness score 
	 * @throws ParseException
	 * @throws IOException
	 */
	public Double calculateScoreVariation() throws ParseException, IOException {
		String proposalRebalancing = new String();
		proposalRebalancing = HttpRestAPIRequest
				.HttpGETRequest(getPropsFromFile("cruise-control.properties").getProperty("cc.url")
						+ "/kafkacruisecontrol/proposals?json=true", "all");

		JSONObject proposalRebalancingJSON = (JSONObject) new JSONParser().parse(proposalRebalancing);
		proposalRebalancingJSON = (JSONObject) proposalRebalancingJSON.get("summary");

		Double unbalanceDifferenceValue = (Double) proposalRebalancingJSON.get("onDemandBalancednessScoreAfter")
				- (Double) proposalRebalancingJSON.get("onDemandBalancednessScoreBefore");

		return unbalanceDifferenceValue;
	}
	
	/**
	 * Evaluates the score variation and returns back:<br/>
	 * - 'safe' tag if scoreVariation < 0.7 * unbalanceTreshold <br/>
	 * - 'unsafe' tag 0.7 * unbalanceTreshold <= scoreVariation < 0.9 * unbalanceTreshold <br/>
	 * - 'alert' tag if  0.9 * unbalanceTreshold <= scoreVariation <br/>
	 * @param scoreVariation scoreVariation to evaluate 
	 * @param unbalanceTreshold threshold to use in the evaluation 
	 * @return tag reflecting the unbalancedness 'safe','unsafe','alert'
	 */
	public String evaluateBalencenessUsingScoreVariation(Double scoreVariation, Double unbalanceThreshold) {
		if (scoreVariation < unbalanceThreshold * 0.7) { // Safe
			return "safe";
		} else {
			if ((unbalanceThreshold * 0.7 <= scoreVariation) && (scoreVariation < unbalanceThreshold * 0.9)) { // Alert
				return "unsafe";
			} else {
				return "alert";
			}
		}

	}
	/**
	 * Launches rebalancing action by triggering the execution of the valid proposal of cruise control at execution time 
	 * (with 2 steps verification enabled)
	 * @throws IOException
	 * @throws ParseException
	 * @throws InterruptedException
	 */
	public void launchRebalancingOperation() throws IOException, ParseException, InterruptedException {
		logger.info(
				"[EXECUTION] Rebalancing operation has been triggered (with default parameters), sending rebalancing request...");
		String RebalancingTaskData;
		RebalancingTaskData = HttpRestAPIRequest
				.HttpPOSTRequest(getPropsFromFile("cruise-control.properties").getProperty("cc.url")
						+ "/kafkacruisecontrol/rebalance?dryrun=false&json=true");

		JSONObject RebalancingTaskDataJSON = (JSONObject) new JSONParser().parse(RebalancingTaskData);
		JSONArray RequestInfoJSONArray = (JSONArray) RebalancingTaskDataJSON.get("RequestInfo");
		JSONObject RequestInfoJSON = (JSONObject) RequestInfoJSONArray.get(0);
		logger.info("[EXECUTION] Rebalancing operation has been scheduled with id=" + RequestInfoJSON.get("Id"));

		HttpRestAPIRequest.HttpPOSTRequest(getPropsFromFile("cruise-control.properties").getProperty("cc.url")
				+ "/kafkacruisecontrol/review?approve=" + RequestInfoJSON.get("Id"));

		logger.info("[EXECUTION] Rebalancing operation has been approved with id=" + RequestInfoJSON.get("Id"));

		Map<String, String> RebalancingTaskInfo = HttpRestAPIRequest
				.HttpPOSTRequestWithHeaders(getPropsFromFile("cruise-control.properties").getProperty("cc.url")
						+ "/kafkacruisecontrol/rebalance?review_id=" + RequestInfoJSON.get("Id"));
		HttpRestAPIRequest.HttpGETRequest(getPropsFromFile("cruise-control.properties").getProperty("cc.url")
				+ "/kafkacruisecontrol/user_tasks?user_task_ids=" + RebalancingTaskInfo.get("TaskID") + "&json=true",
				"all");
		logger.info("[EXECUTION] Rebalancing has started under task-id : " + RebalancingTaskInfo.get("TaskID"));

	}
	/**
	 * Launches rebalancing action for scaling out operations (with 2 steps verification enabled)
	 * @param brokerID broker id used for scaling
	 * @throws IOException
	 * @throws ParseException
	 * @throws InterruptedException
	 */
	public void launchAddingBrokerOperation(Integer brokerID) throws IOException, ParseException, InterruptedException {

		String kafkaClusterState = HttpRestAPIRequest
				.HttpGETRequest(getPropsFromFile("cruise-control.properties").getProperty("cc.url")
						+ "/kafkacruisecontrol/kafka_cluster_state?json=true", "all");

		JSONObject kafkaClusterStateJSON = (JSONObject) new JSONParser().parse(kafkaClusterState);
		JSONObject kafkaBrokersStateJSON = (JSONObject) kafkaClusterStateJSON.get("KafkaBrokerState");
		JSONObject brokerListJSON = (JSONObject) kafkaBrokersStateJSON.get("IsController");

		Set<String> brokerIDSet = brokerListJSON.keySet();
		if (!brokerIDSet.contains(brokerID.toString())) {
			logger.info(
					"[EXECUTION] broker to be added with id " + brokerID + " couldn't be detected by cruise control");
		} else {

			String ScalingOutTaskData = HttpRestAPIRequest
					.HttpPOSTRequest(getPropsFromFile("cruise-control.properties").getProperty("cc.url")
							+ "/kafkacruisecontrol/add_broker?dryrun=false&json=true&brokerid=" + brokerID);

			JSONObject ScalingOutTaskDataJSON = (JSONObject) new JSONParser().parse(ScalingOutTaskData);
			JSONArray RequestInfoJSONArray = (JSONArray) ScalingOutTaskDataJSON.get("RequestInfo");
			JSONObject RequestInfoJSON = (JSONObject) RequestInfoJSONArray.get(0);
			logger.info("[EXECUTION] adding broker operation operation has been scheduled with id="
					+ RequestInfoJSON.get("Id"));

			HttpRestAPIRequest.HttpPOSTRequest(getPropsFromFile("cruise-control.properties").getProperty("cc.url")
					+ "/kafkacruisecontrol/review?approve=" + RequestInfoJSON.get("Id"));

			logger.info("[EXECUTION] adding broker operation has been approved with id=" + RequestInfoJSON.get("Id"));
			Map<String, String> RebalancingTaskInfo = HttpRestAPIRequest
					.HttpPOSTRequestWithHeaders(getPropsFromFile("cruise-control.properties").getProperty("cc.url")
							+ "/kafkacruisecontrol/add_broker?review_id=" + RequestInfoJSON.get("Id"));

			HttpRestAPIRequest.HttpGETRequest(getPropsFromFile("cruise-control.properties").getProperty("cc.url")
					+ "/kafkacruisecontrol/user_tasks?user_task_ids=" + RebalancingTaskInfo.get("TaskID")
					+ "&json=true", "all");
			logger.info("[EXECUTION] adding broker has started under task-id : " + RebalancingTaskInfo.get("TaskID"));
		}

	}
	/**
	 * Launches rebalancing action for scaling in operations (with 2 steps verification enabled)
	 * @param brokerID broker id used for scaling
	 * @throws IOException
	 * @throws ParseException
	 * @throws InterruptedException
	 */
	public void launchRemovingBrokerOperation(Integer brokerID)
			throws IOException, ParseException, InterruptedException {

		String kafkaClusterState = HttpRestAPIRequest
				.HttpGETRequest(getPropsFromFile("cruise-control.properties").getProperty("cc.url")
						+ "/kafkacruisecontrol/kafka_cluster_state?json=true", "all");

		JSONObject kafkaClusterStateJSON = (JSONObject) new JSONParser().parse(kafkaClusterState);
		JSONObject kafkaBrokersStateJSON = (JSONObject) kafkaClusterStateJSON.get("KafkaBrokerState");
		JSONObject brokerListJSON = (JSONObject) kafkaBrokersStateJSON.get("IsController");

		Set<String> brokerIDSet = brokerListJSON.keySet();
		if (!brokerIDSet.contains(brokerID.toString())) {
			logger.info(
					"[EXECUTION] broker to be added with id " + brokerID + " couldn't be detected by cruise control");
		} else {

			String ScalingOutTaskData = HttpRestAPIRequest
					.HttpPOSTRequest(getPropsFromFile("cruise-control.properties").getProperty("cc.url")
							+ "/kafkacruisecontrol/remove_broker?dryrun=false&json=true&brokerid=" + brokerID);

			JSONObject ScalingOutTaskDataJSON = (JSONObject) new JSONParser().parse(ScalingOutTaskData);
			JSONArray RequestInfoJSONArray = (JSONArray) ScalingOutTaskDataJSON.get("RequestInfo");
			JSONObject RequestInfoJSON = (JSONObject) RequestInfoJSONArray.get(0);
			logger.info("[EXECUTION] removing broker operation operation has been scheduled with id="
					+ RequestInfoJSON.get("Id"));

			HttpRestAPIRequest.HttpPOSTRequest(getPropsFromFile("cruise-control.properties").getProperty("cc.url")
					+ "/kafkacruisecontrol/review?approve=" + RequestInfoJSON.get("Id"));

			logger.info("[EXECUTION] removing broker operation has been approved with id=" + RequestInfoJSON.get("Id"));

			Map<String, String> RebalancingTaskInfo = HttpRestAPIRequest
					.HttpPOSTRequestWithHeaders(getPropsFromFile("cruise-control.properties").getProperty("cc.url")
							+ "/kafkacruisecontrol/remove_broker?review_id=" + RequestInfoJSON.get("Id"));

			HttpRestAPIRequest.HttpGETRequest(getPropsFromFile("cruise-control.properties").getProperty("cc.url")
					+ "/kafkacruisecontrol/user_tasks?user_task_ids=" + RebalancingTaskInfo.get("TaskID")
					+ "&json=true", "all");
			logger.info("[EXECUTION] removing broker has started under task-id : " + RebalancingTaskInfo.get("TaskID"));

		}

	}
}
