package gov.usgs.cida.microservices.messaging;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;

import gov.usgs.cida.microservices.api.messaging.MessageBasedMicroservice;
import gov.usgs.cida.microservices.api.messaging.MessagingClient;
import gov.usgs.cida.microservices.api.messaging.MicroserviceHandler;
import gov.usgs.cida.microservices.api.messaging.exception.MqConnectionException;

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
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Set;

/**
 *
 * @author thongsav
 */
public final class MicroserviceMsgservice implements Closeable, MessagingClient, MessageBasedMicroservice {

	private static final Logger log = LoggerFactory.getLogger(MicroserviceMsgservice.class);

	private final String host;
	private final String exchange;
	private final String username;
	private final byte[] password;

	private final ConnectionFactory conFactory;

	private final String serviceName;
	private final Set<Class<? extends MicroserviceHandler>> microserviceHandlers;
	private final ShutdownListener reconnectHandler;
	private Integer numberOfConsumers;
	private Connection conn;
	
	private long connectionRetryTimeMs;
	private boolean waitForConnection;
	private boolean reconnecting;

	public MicroserviceMsgservice(String host, String exchange, String inServiceName, Set<Class<? extends MicroserviceHandler>> inHandlers, Integer numberOfConsumers, String username, String password) {
		this(host, exchange, inServiceName, inHandlers, numberOfConsumers, username, password, 0l, false, false);
	}
	
	public MicroserviceMsgservice(String host, String exchange, String inServiceName, Set<Class<? extends MicroserviceHandler>> inHandlers, Integer numberOfConsumers, String username, String password, long connectionRetryTimeMs, boolean waitForConnection, boolean reconnecting) {
		this.host = host;
		this.exchange = exchange;
		this.username = username;
		this.password = password.getBytes();
		this.numberOfConsumers = numberOfConsumers;

		this.serviceName = inServiceName;
		this.microserviceHandlers = inHandlers;
		
		this.connectionRetryTimeMs = connectionRetryTimeMs;
		this.waitForConnection = waitForConnection;
		this.reconnecting = reconnecting;
		
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(this.host);
		factory.setUsername(this.username);
		factory.setPassword(new String(this.password));
		
		factory.setExceptionHandler(new MicroserviceExceptionHandler());

		this.conFactory = factory;
		
		this.reconnectHandler = new ShutdownListener() {
			public void shutdownCompleted(ShutdownSignalException cause)
			{
				log.info("Lost connection to MQ, reconnection process starting.");
				try { conn.close(); } catch (Exception e) {} //try to close once more just in case
				conn.removeShutdownListener(reconnectHandler);
				conn = null;
				try {
					initialize();
				} catch (MqConnectionException e) {
					log.error("Could not reestablish a connection for {}", serviceName, e);
				}
			}
		};
	}

	/* (non-Javadoc)
	 * @see gov.usgs.cida.microservices.messaging.IMesageBasedMicroservice#initialize()
	 */
	@Override
	public void initialize() throws MqConnectionException {
		Config config = new Config().withRecoveryPolicy(RecoveryPolicies.recoverAlways());
		
		while(conn == null) {
			try {
				conn = Connections.create(conFactory, config);
			} catch(IOException e) {
				if(waitForConnection) {
					log.warn("Could not establish a connection for service {}, retrying again in {}ms", 
							serviceName, connectionRetryTimeMs);
					try {
						Thread.sleep(connectionRetryTimeMs);
					} catch (InterruptedException e1) {
						throw new MqConnectionException("Thread interrupted waiting for MQ connection", e1);
					}
				} else {
					throw new MqConnectionException("Unable to get create MQ connection", e);
				}
			}
		}
		
		if(reconnecting) {
			conn.addShutdownListener(this.reconnectHandler);
		}
		
		for (Class<? extends MicroserviceHandler> clazz : this.microserviceHandlers) {
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
		
		if(this.microserviceHandlers.size() > 0) { //only log info if this service has handlers, otherwise too chatty
			log.info("Service initialized with name {} and {} handlers", this.serviceName, this.microserviceHandlers.size());
		} else {
			log.debug("Service initialized with name {} and no handlers", this.serviceName);
		}
	}
	

	public Channel getChannel() throws IOException {
		if(conn == null) {
			throw new IllegalStateException("No connection exists, has initialize() been called?");
		}
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
	
	/* (non-Javadoc)
	 * @see gov.usgs.cida.microservices.messaging.IMesageBasedMicroservice#getServiceName()
	 */
	@Override
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
	
	/* (non-Javadoc)
	 * @see gov.usgs.cida.microservices.messaging.IMesageBasedMicroservice#bindConsumer(java.lang.String, gov.usgs.cida.microservices.api.messaging.MicroserviceHandler)
	 */
	@Override
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
	
	/* (non-Javadoc)
	 * @see gov.usgs.cida.microservices.messaging.IMesageBasedMicroservice#getMessage(java.lang.String, boolean, int)
	 */
	@Override
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
			log.warn("Error trying to retrieve message from {}", queue, e);
		} finally {
			quietClose(channel);
		}
		
		return result;
	}
	
	/* (non-Javadoc)
	 * @see gov.usgs.cida.microservices.messaging.IMesageBasedMicroservice#getMessage(java.lang.String, boolean)
	 */
	@Override
	public byte[] getMessage(String queue, boolean consumeMessage) {
		return getMessage(queue, consumeMessage, 5000);
	}
}
