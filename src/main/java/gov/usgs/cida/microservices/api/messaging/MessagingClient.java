package gov.usgs.cida.microservices.api.messaging;

import java.util.Map;

public interface MessagingClient {
	
	/**
	 * 
	 * @param requestId
	 * @param serviceRequestId
	 * @param headers
	 * @param message
	 */
	public void sendMessage(String requestId, String serviceRequestId, Map<String, Object> headers, byte[] message);
	
	/**
	 * 
	 * @param queue the queue to pull a message from
	 * @param consumeMessage true to consume message, false to leave message in queue
	 * @param timeoutMillis number of millis to wait for a message to be available
	 * @return
	 */
	byte[] getMessage(String queue, boolean consumeMessage, int timeoutMillis);

	/**
	 * Get the next message in the queue. Will wait 5 seconds for a message to show.
	 * @param queue the queue to pull a message from
	 * @param consumeMessage true to consume message, false to leave message in queue
	 * @return
	 */
	byte[] getMessage(String queue, boolean consumeMessage);
}
