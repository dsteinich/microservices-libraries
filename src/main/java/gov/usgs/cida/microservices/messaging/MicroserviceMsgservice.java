package gov.usgs.cida.microservices.messaging;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;

import gov.usgs.cida.microservices.api.messaging.MessagingClient;
import gov.usgs.cida.microservices.api.messaging.MicroserviceHandler;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;

import java.io.IOException;
import java.util.Map;

import net.jodah.lyra.Connections;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.RecoveryPolicies;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.GetResponse;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Set;

/**
 *
 * @author thongsav
 */
public final class MicroserviceMsgservice implements Closeable, MessagingClient {

	private static final Logger log = LoggerFactory.getLogger(MicroserviceMsgservice.class);

	private final String host;
	private final String exchange;
	private final String username;
	private final String password;

	private final ConnectionFactory conFactory;
	private final Connection conn;

	private final String serviceName;
	private final Set<Class<? extends MicroserviceHandler>> microserviceHandlers;
	
	private Integer numberOfConsumers;
	
	public MicroserviceMsgservice(String host, String exchange, String inServiceName, Set<Class<? extends MicroserviceHandler>> inHandlers, Integer numberOfConsumers, String username, String password) throws IOException {
		this.host = host;
		this.exchange = exchange;
		this.username = username;
		this.password = password;
		this.numberOfConsumers = numberOfConsumers;

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(this.host);
		factory.setUsername(this.username);
		factory.setPassword(this.password);
		
		factory.setExceptionHandler(new MicroserviceExceptionHandler());

		this.conFactory = factory;

		Config config = new Config().withRecoveryPolicy(RecoveryPolicies.recoverAlways());
		Connection connection = Connections.create(conFactory, config);
		conn = connection;

		this.serviceName = inServiceName;
		this.microserviceHandlers = inHandlers;

		for (Class<? extends MicroserviceHandler> clazz : inHandlers) {
			String queueName = null;
			Channel channel = null;
			try {
				channel = getChannel();
				DeclareOk ack = channel.queueDeclare(serviceName + "." + clazz.getSimpleName(), false, false, true, null);
				queueName = ack.getQueue();
			} catch (Exception e) {
				log.error("Could not declare queue", e);
			} finally {
				quietClose(channel);
			}
			if (null != queueName) {
				autoBindConsumer(queueName, clazz);
			}
		}

		log.debug("Service initialized with name {} and {} handlers", this.serviceName, this.microserviceHandlers.size());
	}

	public Channel getChannel() throws IOException {
		Channel channel = conn.createChannel();
		log.trace("Init Channel {} of {} for service {}", channel.getChannelNumber(), conn.getChannelMax(), this.serviceName);
		return channel;
	}
	
	public static void quietClose(Channel c) {
		try {
			c.close();
		} catch (Exception e) {}
	}

	@Override
	public void close() throws IOException {
		log.debug("Service {} closing...", this.serviceName);
		this.conn.close(3000);
	}
	
	public String getServiceName() {
		return this.serviceName;
	}
	
	private void autoBindConsumer(String queueName, Class<? extends MicroserviceHandler> clazz) {
		Channel bindingChannel = null;
		try {
			MicroserviceHandler bindingHandler = clazz.newInstance();
			bindingChannel = getChannel();
			Map<String, Object> defaultBinding = new HashMap<>();
			defaultBinding.put("x-match", "all");
			defaultBinding.put("msrvServiceName", this.serviceName);
			defaultBinding.put("msrvHandlerType", bindingHandler.getClass().getSimpleName());
			bindingChannel.queueBind(queueName, this.exchange, "", defaultBinding);
			for (Map<String, Object> bindingOptions : bindingHandler.getBindings(serviceName)) {
				bindingChannel.queueBind(queueName, this.exchange, "", bindingOptions);
			}

			for (int i = 0; i < numberOfConsumers; i++) {
				//new instances just in case someone makes a non-threadsafe handler
				MicroserviceHandler handler = clazz.newInstance();
				Channel channel = getChannel();
				Consumer consumer = new MicroserviceConsumer(channel, handler, this);
				channel.basicConsume(queueName, true, consumer);
				log.debug("Channel {} now listening for {} messages, handled by {} on service {}", 
						channel.getChannelNumber(), queueName, clazz.getSimpleName(), this.serviceName);
			}
		} catch (Exception e) {
			log.error("Could not register consumers", e);
		} finally {
			quietClose(bindingChannel);
		}
	}
	
	public void bindConsumer(String queueName, MicroserviceHandler bindingHandler) {
		try {
			Channel bindingChannel = getChannel();
			Map<String, Object> defaultBinding = new HashMap<>();
			defaultBinding.put("x-match", "all");
			defaultBinding.put("msrvServiceName", this.serviceName);
			defaultBinding.put("msrvHandlerType", bindingHandler.getClass().getSimpleName());
			bindingChannel.queueBind(queueName, this.exchange, "", defaultBinding);
			for (Map<String, Object> bindingOptions : bindingHandler.getBindings(serviceName)) {
				bindingChannel.queueBind(queueName, this.exchange, "", bindingOptions);
			}
			bindingChannel.close();
			Channel channel = getChannel();
			Consumer consumer = new MicroserviceConsumer(channel, bindingHandler, this);
			channel.basicConsume(queueName, true, consumer);
			log.debug("Channel {} now listening for {} messages on service {}", 
					channel.getChannelNumber(), queueName, this.serviceName);
		} catch (Exception e) {
			log.error("Could not register consumers", e);
		}
	}
	
	@Override
	public void sendMessage(String requestId, String serviceRequestId, Map<String, Object> headers, byte[] message) {
		Channel channel = null;
		try {
			channel = getChannel();
			
			Map<String, Object> modHeaders = new HashMap<>();
			if (null != headers) {
				modHeaders.putAll(headers);
			}
			iffPut(modHeaders, "requestId", requestId);
			iffPut(modHeaders, "serviceRequestId", serviceRequestId);
			iffPut(modHeaders, "msrvLoggable", Boolean.TRUE);
			iffPut(modHeaders, "msrvPublishedBy", this.getServiceName());
			AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
				.headers(modHeaders)
				.expiration("300000") //5 minute expiration time on all sent messages
				.build();
			
			log.trace("Sending message with Headers {} through service {}", 
					new Gson().toJson(modHeaders, Map.class), this.serviceName);
			channel.basicPublish(exchange, "", props, message);
		} catch (Exception e) {
			log.error("Could not send message {}", message);
		} finally {
			quietClose(channel);
		}
	}
	
	private static boolean iffPut(Map<String, Object> headers, String key, Object val) {
		boolean result = false;
		if (null != headers && null != key) {
			if (!headers.containsKey(key)) {
				headers.put(key, val);
				result = true;
			}
		}
		return result;
	}
	
	/**
	 * Declare a queue to hold a serviceName/eventType combo
	 * 
	 * @param serviceName
	 * @param eventType
	 */
	public void declareQueueForType(String serviceName, String eventType, int messageExpiry, int queueExpiry) {
		Channel channel = null;
		try {
			channel = getChannel();
			Map<String, Object> queueOptions = new HashMap<>();
			queueOptions.put("x-message-ttl", messageExpiry);
			queueOptions.put("x-expires", queueExpiry); 
			DeclareOk ack = channel.queueDeclare(serviceName, false, false, true, queueOptions);
			ack.getQueue();

			Map<String, Object> bindingOptions = new HashMap<>();
			bindingOptions.put("x-match", "all");
			bindingOptions.put("serviceName", serviceName);
			bindingOptions.put("eventType", eventType);

			channel.queueBind(serviceName, this.exchange, "", bindingOptions);
		} catch (Exception e) {
			log.error("Could not declare queue", e);
		} finally {
			quietClose(channel);
		}
	}
	
	/**
	 * 
	 * @param queue the queue to pull a message from
	 * @param consumeMessage true to consume message, false to leave message in queue
	 * @param timeoutMillis number of millis to wait for a message to be available
	 * @return
	 */
	public byte[] getMessage(String queue, boolean consumeMessage, int timeoutMillis) {
		byte[] result = null;
		Channel channel = null;
		try {
			channel = getChannel();
			GetResponse resp = null;
			long endTime = System.currentTimeMillis() + timeoutMillis;
			
			while(resp == null) {
				if(System.currentTimeMillis() > endTime) {
					throw new RuntimeException("Timeout waiting for message");
				}
				
				resp = channel.basicGet(queue, false);
			}
			
			result = resp.getBody();
		} catch (Exception e) {
		} finally {
			quietClose(channel);
		}
		
		return result;
	}
	
	/**
	 * Get the next message in the queue. Will wait 5 seconds for a message to show.
	 * @param queue the queue to pull a message from
	 * @param consumeMessage true to consume message, false to leave message in queue
	 * @return
	 */
	public byte[] getMessage(String queue, boolean consumeMessage) {
		return getMessage(queue, consumeMessage, 5000);
	}
}
