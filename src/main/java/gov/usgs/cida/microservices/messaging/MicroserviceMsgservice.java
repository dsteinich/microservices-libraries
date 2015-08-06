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
import com.rabbitmq.client.MessageProperties;

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
	
	public MicroserviceMsgservice(String host, String exchange, String inServiceName, Set<Class<? extends MicroserviceHandler>> inHandlers, String username, String password) throws IOException {
		this.host = host;
		this.exchange = exchange;
		this.username = username;
		this.password = password;

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(this.host);
		factory.setUsername(this.username);
		factory.setPassword(this.password);
		factory.setExceptionHandler(new MicroserviceExceptionHandler());

		this.conFactory = factory;
		log.debug("initialized ConnectionFactory");

		Config config = new Config().withRecoveryPolicy(RecoveryPolicies.recoverAlways());
		Connection connection = Connections.create(conFactory, config);
		conn = connection;

		this.serviceName = inServiceName;
		log.info("THIS IS MY SERVICE NAME: " + this.serviceName);

		this.microserviceHandlers = inHandlers;
		log.info("I'VE GOT {} HANDLERS", this.microserviceHandlers.size());

		for (Class<? extends MicroserviceHandler> clazz : inHandlers) {
			String queueName = null;
			try {
				Channel channel = getChannel();
				DeclareOk ack = channel.queueDeclare(serviceName + "." + clazz.getSimpleName(), false, false, true, null);
				queueName = ack.getQueue();
				channel.close();
			} catch (Exception e) {
				log.error("Could not declare queue", e);
			}
			if (null != queueName) {
				autoBindConsumer(queueName, clazz);
			}
		}

		log.debug("instantiated msg service");
	}

	public Channel getChannel() throws IOException {
		Channel channel = conn.createChannel();
		log.trace("init Channel {} of {}", channel.getChannelNumber(), conn.getChannelMax());
		return channel;
	}

	@Override
	public void close() throws IOException {
		log.info("Cleaning Up Message Service");
		this.conn.close(3000);
	}
	
	public String getServiceName() {
		return this.serviceName;
	}
	
	private void autoBindConsumer(String queueName, Class<? extends MicroserviceHandler> clazz) {
		try {
			MicroserviceHandler bindingHandler = clazz.newInstance();
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

			int numberOfConsumers = 3;
			for (int i = 0; i < numberOfConsumers; i++) {
				//new instances just in case someone makes a non-threadsafe handler
				MicroserviceHandler handler = clazz.newInstance();
				Channel channel = getChannel();
				Consumer consumer = new MicroserviceConsumer(channel, handler, this);
				channel.basicConsume(queueName, true, consumer);
				log.info("Channel {} now listening for {} messages, handled by {}", channel.getChannelNumber(), queueName, clazz.getSimpleName());
			}
		} catch (Exception e) {
			log.error("Could not register consumers", e);
		}
	}
	
	public void bindConsumer(String queueName, MicroserviceHandler bindingHandler) {
		try {
			Channel bindingChannel = getChannel();
			Map<String, Object> defaultBinding = new HashMap<>();
			defaultBinding.put("x-match", "all");
			defaultBinding.put("msrvServiceName", this.serviceName);
			defaultBinding.put("msrvHandlerType", bindingHandler.getClass().getSimpleName());
			//@thongsav
			//defaultBinding might not always have a HandlerType (arg[2] for defaultBinding) might be causing the error.
			//Would cause queueBind to potentially fail which means no consumers registered.
			bindingChannel.queueBind(queueName, this.exchange, "", defaultBinding);
			for (Map<String, Object> bindingOptions : bindingHandler.getBindings(serviceName)) {
				bindingChannel.queueBind(queueName, this.exchange, "", bindingOptions);
			}
			bindingChannel.close();
			Channel channel = getChannel();
			Consumer consumer = new MicroserviceConsumer(channel, bindingHandler, this);
			channel.basicConsume(queueName, true, consumer);
			log.info("Channel {} now listening for {} messages", channel.getChannelNumber(), queueName);
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
			log.trace("Sending message with Headers {}", new Gson().toJson(modHeaders, Map.class));
			AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
				.headers(modHeaders)
				.build();
			
			//MsgProps.P_T_P should make it so that the messages aren't deleted from the queue.
			//Use this to test if messages are sent.
			//Msgs don't see to make it to the queues.
			channel.basicPublish(exchange, "", props, message);
		} catch (Exception e) {
			log.error("Could not send message {}", message);
		} finally {
			try {
				if (null != channel) {
					channel.close();
				}
			} catch (Exception e) {
				log.error("Could not close sending channel");
			}
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
	
	public void declareQueueForType(String serviceName, String eventType) {
		try {
			Channel channel = getChannel();
			DeclareOk ack = channel.queueDeclare(serviceName, true, false, true, null);
			ack.getQueue();

			Map<String, Object> bindingOptions = new HashMap<>();
			bindingOptions.put("x-match", "all");
			bindingOptions.put("serviceName", serviceName);
			bindingOptions.put("eventType", eventType);

			channel.queueBind(serviceName, this.exchange, "", bindingOptions);
			channel.close();
		} catch (Exception e) {
			log.error("Could not declare queue", e);
		}
	}
}
