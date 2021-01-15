package com.dassault_systemes.kafkarebalance;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.zookeeper.KeeperException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "KafkaRebalancer", mixinStandardHelpOptions = true, version = "KafkaRebalancer 1.0", description = "Command line tool that uses Cruise Control API to manage rebalancing operations triggering on Kafka clusters based on custom criterias")

public class KafkaRebalanceMain implements Callable<Integer> {
	private final static Logger logger = LoggerFactory.getLogger(KafkaRebalanceMain.class);
	private static Rebalancer rebalancer = new Rebalancer();
	private static ZKScheduler zklogger = new ZKScheduler();

	@Option(names = { "-m",
			"--mode" }, required = true, defaultValue = "simulation", description = "Whether to run the rebalancer in 'simulation' or 'execution' mode")
	String Mode;

	@Option(names = { "-a",
			"--action" }, required = false, defaultValue = "rebalance-current-cluster", description = "What action should we launch : 'rebalance-current-cluster' [default], 'add-broker', 'remove-broker'")
	String actionType;

	@Option(names = { "-i",
			"--broker-id" }, required = false, defaultValue = "-1", description = "broker-id to add (scale out) or remove")
	Integer brokerID;

	@Option(names = { "-f",
			"--force" }, required = false, description = "Whether to force rebalancing operation or not")
	boolean force_rebalance;

	@Option(names = { "-t",
			"--unbalance-threshold" }, required = false, defaultValue = "-1", description = "threshhold above which rebalancing operation is immediatly sent")
	Double unbalanceTreshold;

	public static Properties getPropsFromFile(String path) throws IOException {
		Properties props = new Properties();
		InputStream is = KafkaRebalanceMain.class.getClassLoader().getResourceAsStream(path);
		if (is != null) {
			props.load(is);
			return props;
		} else {
			throw new FileNotFoundException("propertie file path " + path + "couldn't succesfully be read");
		}
	}

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

	public Integer call() throws IOException, ParseException, IllegalStateException, KeeperException,
			InterruptedException, java.text.ParseException {
		String rebalancingState;
		String zkRoot = "/scalingBrokersOperations";
		String zkHost = getPropsFromFile("cruise-control.properties").getProperty("zk.connect");

		switch (Mode) {
		case ("simulation"):
			switch (actionType) {
			case "rebalance-current-cluster": // rebalancing workflow
				if (unbalanceTreshold == -1) {
					logger.info("[SIMULATION] no unbalance-threshold has been set");
					break;
				} else {
					if (force_rebalance) { // forced
						logger.info("[SIMULATION] forced rebalancing operation has been triggered");
						// check for other rebalancing tasks that are still in execution status
						if (rebalancer.checkActiveRebalancingTasks()) {
							logger.info("[SIMULATION] other uncompleted rebalancing tasks has been triggered");
						} else { // no rebalancing active tasks in execution time, we can run the forced
									// rebalancing
							// get summary
							logger.info("[SIMULATION] Rebalancing summary :\n" + HttpRestAPIRequest
									.HttpGETRequest(getPropsFromFile("cruise-control.properties").getProperty("cc.url")
											+ "/kafkacruisecontrol/proposals?json=true", "all"));
							logger.info(
									"[SIMULATION] Rebalancing operation has been triggered (with default parameters), sending rebalancing request...");
							// get scheduled rebalancing task information
							logger.info("[SIMULATION] Rebalancing operation has been scheduled with id= [EMPTY]");

							logger.info("[SIMULATION] Rebalancing operation has been approved with id=[EMPTY]");
							logger.info("[SIMULATION] Rebalancing has started under task-id : [EMPTY]");
						}
						break;
					} else { // non-forced
						// check for scaling scheduled operations in ZK by checking the first scheduled
						// task
						if (!zklogger.getZNodeEarliestChild(zkRoot, zkHost).isEmpty()) {
							logger.info(
									"[SIMULATION] rebalancer has found a scaling operation in pending status and will try to re-launch scaling now");
							// parse znode and extract scaling operation data
							Map<String, String> scalingMetaData = zklogger.parseZNodeData(zklogger.getZNodeData(
									zkRoot + "/" + zklogger.getZNodeEarliestChild(zkRoot, zkHost), zkHost));

							brokerID = Integer.parseInt(scalingMetaData.get("broker-id"));

							if (scalingMetaData.get("operation").equals("add")) { // move to scaling out
								actionType = "add-broker";
							}
							if (scalingMetaData.get("operation").equals("remove")) { // move to scaling in
								actionType = "remove-broker";
							}

						} else { // no scheduled operation left, we do the periodical cluster checking
							rebalancingState = "check";
							switch (rebalancingState) {
							case "check":
								// Start
								logger.info("[SIMULATION] rebalancing operation has been triggered");

								// Get Proposal
								String proposalRebalancing = new String();
								try {
									proposalRebalancing = HttpRestAPIRequest.HttpGETRequest(
											getPropsFromFile("cruise-control.properties").getProperty("cc.url")
													+ "/kafkacruisecontrol/proposals?json=true",
											"all");
								} catch (IOException e) {
									logger.info(
											" [SIMULATION] Internal HTTP Server Error, Rebalancer couldn't retrieve the actual state of the cluster through GET /proposals");
									e.printStackTrace();
								}

								// Parse proposal JSON
								JSONObject proposalRebalancingJSON = (JSONObject) new JSONParser()
										.parse(proposalRebalancing);
								proposalRebalancingJSON = (JSONObject) proposalRebalancingJSON.get("summary");

								// Calculate score variation
								Double scoreVariation = rebalancer.calculateScoreVariation();
								logger.info("[SIMULATION] Score balancing impact variation measured / Actual score  : "
										+ scoreVariation + " / "
										+ (Double) proposalRebalancingJSON.get("onDemandBalancednessScoreBefore"));

								// CSV measurement logging
								DateFormat dateformat = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");
								Date date = new Date();
								exportDataToCSV(
										getPropsFromFile("cruise-control.properties")
												.getProperty("path.log.rebalancing.csv")
												+ "/[SIMULATION]rebalancing_impact_data.csv",
										new String[] { dateformat.format(date).toString(), scoreVariation.toString() },
										new String[] { "TimeStamp", "unbalancing-difference" });

								// Evaluating impact
								String currentState = rebalancer.evaluateBalencenessUsingScoreVariation(scoreVariation,
										unbalanceTreshold);
								if (currentState == "safe") { // Safe
									logger.info(
											"[SIMULATION] the current unbalancing state is safe, no need yet for rebalancing");
									break;
								} else {
									if (currentState == "unsafe") {
										// without immediate action
										logger.info(
												"[SIMULATION] unbalacing state has increased significally, it is recommended to start a rebalancing operation (use the force-rebalance option)");
										break;
									} else {
										// Immediate re-balancing
										logger.info(
												"[SIMULATION] unbalacing state has reached a critical point, an automatic rebalancing operation will be launched");

										// Check if there is any previous rebalancing is still in execution
										if (rebalancer.checkActiveRebalancingTasks()) {
											logger.info(
													"[SIMULATION] some other uncompleted rebalancing/scaling tasks has been triggered");
											break;
										}
									}
								}
							case "rebalance":
								// launch rebalancing
								logger.info("[SIMULATION] Rebalancing will be started");
								logger.info(
										"[SIMULATION] Rebalancing summary :\n"
												+ HttpRestAPIRequest.HttpGETRequest(
														getPropsFromFile("cruise-control.properties").getProperty(
																"cc.url") + "/kafkacruisecontrol/proposals?json=true",
														"all"));
								break;
							}
							break;
						}
					}
				}
			case "add-broker":
				if (brokerID == -1) {
					logger.info("[SIMULATION] no broker id has been given ");
				} else { // start scaling out
					// check for other rebalancing tasks that are still in execution status
					if (rebalancer.checkActiveRebalancingTasks()) {
						logger.info("[SIMULATION] some other uncompleted rebalancing/scaling tasks has been triggered");
						// check if this scaling has already been scheduled before
						if (zklogger.checkForSimilarZnode(zkRoot, zkHost, brokerID.toString() + "-add")) {
							logger.info("[EXECUTION] this scaling operation has already been scheduled");
						} else {
							// schedule scaling task in ZK
							logger.info(
									"[SIMULATION] scaling operation has been scheduled in Zookeeper to be executed in the future");
						}
					} else { // launch scaling out
						// check if there are any scheduled scaling operations by checking the earliest
						// scheduled operation
						if (!zklogger.getZNodeEarliestChild(zkRoot, zkHost).isEmpty()) {
							/**
							 * check if the current scaling operation to be launched is the earliest
							 * scheduled operation. if so, it will be deleted from ZK. if it is not, it
							 * means that this is an out of schedule operation and it will be privileged on
							 * scheduled ones
							 */
							if ((brokerID.toString() + "-add").equals(zklogger.getZNodeData(
									// delete currently executed task if it exist in ZK
									zkRoot + "/" + zklogger.getZNodeEarliestChild(zkRoot, zkHost), zkHost))) {
							}
						}
						logger.info("[SIMULATION] adding broker with brokerID:" + brokerID);
						break;
					}
				}
				break;
			case "remove-broker":
				if (brokerID == -1) {
					logger.info("[SIMULATION] no broker id has been given ");
				} else {
					if (rebalancer.checkActiveRebalancingTasks()) {
						logger.info("[SIMULATION] some other uncompleted rebalancing/scaling tasks has been triggered");
						if (zklogger.checkForSimilarZnode(zkRoot, zkHost, brokerID.toString() + "-remove")) {
							logger.info("[EXECUTION] this scaling operation has already been scheduled");
						} else {
							logger.info(
									"[SIMULATION] scaling operation has been scheduled in Zookeeper to be executed in the future");
						}
					} else {
						if (!zklogger.getZNodeEarliestChild(zkRoot, zkHost).isEmpty()) {
							if ((brokerID.toString() + "-remove").equals(zklogger.getZNodeData(
									zkRoot + "/" + zklogger.getZNodeEarliestChild(zkRoot, zkHost), zkHost))) {
							}
						}
						logger.info("[SIMULATION] removing broker with brokerID:" + brokerID);
						break;
					}
				}
			case "stop-action": {

			}
			}
			break;
		case ("execution"):
			switch (actionType) {
			case "rebalance-current-cluster":
				if (unbalanceTreshold == -1) {
					logger.info("[EXECUTION] no unbalance-threshold has been set");
					break;
				} else {
					if (force_rebalance) { // forced
						logger.info("[EXECUTION] forced rebalancing operation has been triggered");
						// check for other rebalancing tasks that are still in execution status
						if (rebalancer.checkActiveRebalancingTasks()) {
							logger.info(
									"[EXECUTION] some other uncompleted rebalancing/scaling tasks has been triggered");
						} else {
							logger.info("[EXECUTION] Rebalancing will be started");
							// Get summary
							logger.info("[EXECUTION] Rebalancing summary :\n" + HttpRestAPIRequest
									.HttpGETRequest(getPropsFromFile("cruise-control.properties").getProperty("cc.url")
											+ "/kafkacruisecontrol/proposals?json=true", "all"));
							rebalancer.launchRebalancingOperation();
						}
						break;
					} else { // non-forced

						// Check if there are any scheduled operations
						if (!zklogger.getZNodeEarliestChild(zkRoot, zkHost).isEmpty()) {
							logger.info(
									"[EXECUTION] rebalancer has found a scaling operation in pending status and will try to re-launch scaling now");
							// Get data from scheduling znode
							Map<String, String> scalingMetaData = zklogger.parseZNodeData(zklogger.getZNodeData(
									zkRoot + "/" + zklogger.getZNodeEarliestChild(zkRoot, zkHost), zkHost));

							// Parse znode data

							// assign brokerID of the scaling operation
							brokerID = Integer.parseInt(scalingMetaData.get("broker-id"));

							// redirect to corresponding scaling operation
							if (scalingMetaData.get("operation").equals("add")) {
								actionType = "add-broker";
							}
							if (scalingMetaData.get("operation").equals("remove")) {
								actionType = "remove-broker";
							}

						} else { // no scheduled scaling
							rebalancingState = "check";
							switch (rebalancingState) {
							case "check":
								// Start
								logger.info("[EXECUTION] rebalancing operation has been triggered");

								// Get Proposal
								String proposalRebalancing = new String();
								try {
									proposalRebalancing = HttpRestAPIRequest.HttpGETRequest(
											getPropsFromFile("cruise-control.properties").getProperty("cc.url")
													+ "/kafkacruisecontrol/proposals?json=true",
											"all");
								} catch (IOException e) {
									logger.info(
											"[EXECUTION] Internal HTTP Server Error, Rebalancer couldn't retrieve the actual state of the cluster through GET /proposals");
									e.printStackTrace();
								}

								// Parse proposal JSON
								JSONObject proposalRebalancingJSON = (JSONObject) new JSONParser()
										.parse(proposalRebalancing);
								proposalRebalancingJSON = (JSONObject) proposalRebalancingJSON.get("summary");

								// Calculate score variation
								Double scoreVariation = rebalancer.calculateScoreVariation();
								logger.info("[EXECUTION] Score balancing impact variation measured / Actual score  : "
										+ scoreVariation + " / "
										+ (Double) proposalRebalancingJSON.get("onDemandBalancednessScoreBefore"));

								// CSV measurement logging
								DateFormat dateformat = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");
								Date date = new Date();
								exportDataToCSV(
										getPropsFromFile("cruise-control.properties").getProperty(
												"path.log.rebalancing.csv") + "/[EXECUTION]rebalancing_impact_data.csv",
										new String[] { dateformat.format(date).toString(), scoreVariation.toString() },
										new String[] { "TimeStamp", "unbalancing-difference" });

								// Evaluating impact
								String currentState = rebalancer.evaluateBalencenessUsingScoreVariation(scoreVariation,
										unbalanceTreshold);
								if (currentState == "safe") { // Safe
									logger.info(
											"[EXECUTION] the current unbalancing state is safe, no need yet for rebalancing");
									break;
								} else {
									if (currentState == "unsafe") {
										// without immediate action
										logger.info(
												"[EXECUTION] unbalacing state has increased significally, it is recommended to start a rebalancing operation (use the force-rebalance option)");
										break;
									} else {
										// Immediate re-balancing
										logger.info(
												"[EXECUTION] unbalacing state has reached a critical point, an automatique rebalancing operation will be launched");

										// Check if there is any previous rebalancing is still in execution
										if (rebalancer.checkActiveRebalancingTasks()) {
											logger.info(
													"[EXECUTION] another uncompleted rebalancing/scaling tasks has been triggered");
											break;
										}
									}
								}
							case "rebalance":
								logger.info("[EXECUTION] Rebalancing will be started");
								logger.info(
										"[EXECUTION] Rebalancing summary :\n"
												+ HttpRestAPIRequest.HttpGETRequest(
														getPropsFromFile("cruise-control.properties").getProperty(
																"cc.url") + "/kafkacruisecontrol/proposals?json=true",
														"all"));
								rebalancer.launchRebalancingOperation();
								break;
							}
							break;
						}
					}
				}
			case "add-broker":
				if (brokerID == -1) {
					logger.info("[EXECUTION] no broker id has been given ");
				} else {
					// start scaling out
					// check for other rebalancing tasks that are still in execution status
					if (rebalancer.checkActiveRebalancingTasks()) {
						logger.info("[EXECUTION] some other uncompleted rebalancing/scaling tasks has been triggered");

						// check if this scaling has already been scheduled before
						if (zklogger.checkForSimilarZnode(zkRoot, zkHost, brokerID.toString() + "-add")) {
							logger.info("[EXECUTION] this scaling operation has already been scheduled");
						} else {
							// schedule scaling task in ZK
							zklogger.writeZNodeData(
									zkRoot + "/" + new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS").format(new Date()),
									(brokerID.toString() + "-add").getBytes(), zkHost);
							logger.info(
									"[EXECUTION] scaling operation has been scheduled in Zookeeper to be executed in the future");
						}
					} else {
						// launch scaling out
						// check if there are any scheduled scaling operations by checking the earliest
						// scheduled operation
						if (!zklogger.getZNodeEarliestChild(zkRoot, zkHost).isEmpty()) {
							/**
							 * check if the current scaling operation to be launched is the earliest
							 * scheduled operation. if so, it will be deleted from ZK. if it is not, it
							 * means that this is an out of schedule operation and it will be privileged on
							 * scheduled ones
							 */
							if ((brokerID.toString() + "-add").equals(zklogger.getZNodeData(
									zkRoot + "/" + zklogger.getZNodeEarliestChild(zkRoot, zkHost), zkHost))) {
								// delete currently executed task if it exist in zk
								zklogger.deleteZNode(zkRoot + "/" + zklogger.getZNodeEarliestChild(zkRoot, zkHost),
										zkHost);
							}
						}
						logger.info("[EXECUTION] adding broker with brokerID:" + brokerID);
						rebalancer.launchAddingBrokerOperation(brokerID);

						break;
					}
				}
				break;
			case "remove-broker":
				if (brokerID == -1) {
					logger.info("[EXECUTION] no broker id has been given ");
				} else {
					if (rebalancer.checkActiveRebalancingTasks()) {
						logger.info("[EXECUTION] some other uncompleted rebalancing/scaling tasks has been triggered");
						if (zklogger.checkForSimilarZnode(zkRoot, zkHost, brokerID.toString() + "-remove")) {
							logger.info("[EXECUTION] this scaling operation has already been scheduled");
						} else {
							zklogger.writeZNodeData(
									zkRoot + "/" + new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS").format(new Date()),
									(brokerID.toString() + "-remove").getBytes(), zkHost);
							logger.info(
									"[EXECUTION] scaling operation has been scheduled in Zookeeper to be executed in the future");
						}
					} else {
						if (!zklogger.getZNodeEarliestChild(zkRoot, zkHost).isEmpty()) {
							if ((brokerID.toString() + "-remove").equals(zklogger.getZNodeData(
									zkRoot + "/" + zklogger.getZNodeEarliestChild(zkRoot, zkHost), zkHost))) {
								zklogger.deleteZNode(zkRoot + "/" + zklogger.getZNodeEarliestChild(zkRoot, zkHost),
										zkHost);
							}
						}
						logger.info("[EXECUTION] removing broker with brokerID:" + brokerID);
						rebalancer.launchRemovingBrokerOperation(brokerID);
						break;
					}
				}
			case "stop-action": {
			}
			}
			break;
		}
		return 0;
	}

	public static void main(String[] args) throws IOException, ParseException {
		new CommandLine(new KafkaRebalanceMain()).execute(args);

	}
}
