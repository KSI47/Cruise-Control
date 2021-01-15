package com.dassault_systemes.kafkarebalance;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Http requests class that implements methods to send GET, POST, DELETE
 * requests useful when using REST Proxy to produce and consume messages
 * 
 * @author SKI44
 *
 */
public class HttpRestAPIRequest {
	/**
	 * response code corresponding to the last Http request sent
	 */
	private static int responseCode;

	/**
	 * gets responseCode value
	 * 
	 * @return responceCode
	 */
	public static int getResponseCode() {
		return responseCode;
	}

	/**
	 * Sends a POST request to url and returns back the response in String
	 * 
	 * @param url     http server URL
	 * @param content content-type header for the request
	 * @param accept  accept header for the response
	 * @param data    data to be sent
	 * @return the HTTP server response for the POST request
	 * @throws IOException
	 */
	public static String HttpPOSTRequest(String url, String content, String accept, String data) throws IOException {
		URL restProxyUrl = new URL(url);
		HttpURLConnection connection = (HttpURLConnection) restProxyUrl.openConnection();
		connection.setRequestMethod("POST");
		connection.setRequestProperty("Content-Type", content);
		connection.setRequestProperty("Accept", accept);
		connection.setDoOutput(true);
		OutputStream os = connection.getOutputStream();
		os.write(data.getBytes());
		os.flush();
		os.close();
		responseCode = connection.getResponseCode();
		InputStreamReader inputStream = new InputStreamReader(connection.getInputStream());
		BufferedReader in = new BufferedReader(inputStream);
		String inputLine;
		StringBuffer response = new StringBuffer();
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
		return response.toString();
	}

	/**
	 * Sends a POST request to url and returns back the response in String without
	 * Accept specifications
	 * 
	 * @param url     http server URL
	 * @param content content-type header for the request
	 * @param data    data to be sent
	 * @return the HTTP server response for the POST request
	 * @throws IOException
	 */
	public static String HttpPOSTRequest(String url, String content, String data) throws IOException {
		URL restProxyUrl = new URL(url);
		// Open URL connection
		HttpURLConnection connection = (HttpURLConnection) restProxyUrl.openConnection();
		
		// Setup URL connection
		connection.setRequestMethod("POST");
		connection.setRequestProperty("Content-Type", content);
		connection.setDoOutput(true);
		
		// Send DATA through OutputStream
		OutputStream os = connection.getOutputStream();
		os.write(data.getBytes());
		os.flush();
		os.close();
		
		// Set response code for the HTTP connection
		responseCode = connection.getResponseCode();
		
		// Read HTTP answer 
		InputStreamReader inputStream = new InputStreamReader(connection.getInputStream());
		BufferedReader in = new BufferedReader(inputStream);
		String inputLine;
		StringBuffer response = new StringBuffer();
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
		return response.toString();
	}
	public static String HttpPOSTRequest(String url) throws IOException {
		URL restProxyUrl = new URL(url);
		// Open URL connection
		HttpURLConnection connection = (HttpURLConnection) restProxyUrl.openConnection();
		
		// Setup URL connection
		connection.setRequestMethod("POST");
		connection.setDoOutput(true);		
		// Send DATA through OutputStream
		OutputStream os = connection.getOutputStream();
		os.flush();
		os.close();
		
		// Set response code for the HTTP connection
		responseCode = connection.getResponseCode();
		
		// Read HTTP answer 
		InputStreamReader inputStream = new InputStreamReader(connection.getInputStream());
		BufferedReader in = new BufferedReader(inputStream);
		String inputLine;
		StringBuffer response = new StringBuffer();
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
		return response.toString();
	}
	public static Map<String, String> HttpPOSTRequestWithHeaders(String url) throws IOException {
		URL restProxyUrl = new URL(url);
		// Open URL connection
		HttpURLConnection connection = (HttpURLConnection) restProxyUrl.openConnection();
		
		// Setup URL connection
		connection.setRequestMethod("POST");
		connection.setDoOutput(true);
		
		// Send DATA through OutputStream
		OutputStream os = connection.getOutputStream();
		os.flush();
		os.close();
		
		// Set response code for the HTTP connection
		responseCode = connection.getResponseCode();
		
		// Read HTTP answer 
		InputStreamReader inputStream = new InputStreamReader(connection.getInputStream());
		String TaskID = connection.getHeaderField("User-Task-ID");
		BufferedReader in = new BufferedReader(inputStream);
		String inputLine;
		StringBuffer response = new StringBuffer();
		Map <String, String> ResponceMap = new HashMap <String, String>(); 
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
		ResponceMap.put("body",response.toString());
		ResponceMap.put("TaskID",TaskID);
		return ResponceMap;
	}
	/**
	 * Sends a GET request to url and returns back the response in String
	 * @param url     http server URL
	 * @param accept  accept header for the response
	 * @return the HTTP server response for the GET request
	 * @throws IOException
	 */
	public static String HttpGETRequest(String url, String accept) throws IOException {
		URL restProxyUrl = new URL(url);
		// Open URL connection
		HttpURLConnection connection = (HttpURLConnection) restProxyUrl.openConnection();
		
		// Setup URL connection
		connection.setRequestMethod("GET");
		connection.setRequestProperty("Accept", accept);
		
		// Set response code for the HTTP connection
		responseCode = connection.getResponseCode();
		String readLine;
		
		// Read HTTP answer
		BufferedReader inputBuffer = new BufferedReader(new InputStreamReader(connection.getInputStream()));
		StringBuffer response = new StringBuffer();
		while ((readLine = inputBuffer.readLine()) != null) {
			response.append(readLine);
		}
		inputBuffer.close();
		return response.toString();

	}

	/**
	 * Sends a DELETE request on a URL resource
	 * @param urlResource to delete 
	 * @param content content-type
	 * @throws IOException
	 */
	public static void HttpDELETERequest(String urlResource, String content) throws IOException {
		URL urlRessource = new URL(urlResource);
		// Open URL connection
		HttpURLConnection httpURLConnection = (HttpURLConnection) urlRessource.openConnection();
		
		// Setup URL connection
		httpURLConnection.setRequestProperty("Content-Type", content);
		httpURLConnection.setRequestMethod("DELETE");

		if (httpURLConnection != null) {
			httpURLConnection.disconnect();
		}

	}

}
