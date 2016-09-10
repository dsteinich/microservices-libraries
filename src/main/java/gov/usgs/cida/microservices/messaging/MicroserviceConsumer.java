package gov.usgs.cida.microservices.messaging;

import gov.usgs.cida.microservices.api.messaging.MicroserviceHandler;
import gov.usgs.cida.microservices.util.MessageUtils;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dmsibley
 * @author thongsav
 */
public class MicroserviceConsumer extends DefaultConsumer  {
	private static final Logger log = LoggerFactory.getLogger(MicroserviceConsumer.class);

	private final Channel channel;
	private final MicroserviceHandler handler;
	private final MicroserviceMsgservice msgService;

	public MicroserviceConsumer(Channel channel, MicroserviceHandler handler, MicroserviceMsgservice msgService) {
		super(channel);
		this.channel = channel;
		this.handler = handler;
		this.msgService = msgService;
	}

	@Override
	public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
		log.trace("Handling Delivery");
		long deliveryTag = envelope.getDeliveryTag();
		boolean isRedeliver = envelope.isRedeliver();
		log.trace("Consumer {} on Channel {} received message {}, isRedeliver: {}", 
				this.getClass().getSimpleName(), this.channel.getChannelNumber(), deliveryTag, isRedeliver);
		
		log.trace("Message headers: {}", new Gson().toJson(properties.getHeaders(), Map.class));
		
		//Specifically call out requestId and serviceRequestId, this should be included in all of our microservice calls
		Map<String, Object> params = properties.getHeaders();
		String requestId = MessageUtils.getStringFromHeaders(params, "requestId");
		String serviceRequestId = MessageUtils.getStringFromHeaders(params, "serviceRequestId");
		try {
			this.handler.handle(requestId, serviceRequestId, params, body, this.msgService);

			//TODO turn off autoack?
			//For error handlers, we probably want autoack off so that there can be multiple error handlers
//			channel.basicAck(deliveryTag, false);
		} catch (Exception e) {
			log.error("Error while handling message", e);
			sendErrorMessage(
					getStringFromHeaders(properties.getHeaders(), "requestId"),
					getStringFromHeaders(properties.getHeaders(), "serviceRequestId"), 
					e);
		}
	}
	
	private void sendErrorMessage(String requestId, String serviceRequestId, Exception sourceException) {
		HashMap<String, Object> errorParams = new HashMap<>();
		errorParams.put("eventType", "error");
		errorParams.put("errorType", sourceException.getClass().getName());
		errorParams.put("errorSource", msgService.getServiceName());
		String msg = sourceException.getMessage();
		if(StringUtils.isBlank(msg)) {
			msg = "";
		}
		msgService.sendMessage(requestId, serviceRequestId, errorParams, msg.getBytes());
	}

	private String getStringFromHeaders(Map<String, Object> params, String key) {
		return params.get(key) != null ? params.get(key).toString() : null;
	}
}
