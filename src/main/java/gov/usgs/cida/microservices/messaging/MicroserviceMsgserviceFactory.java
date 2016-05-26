package gov.usgs.cida.microservices.messaging;

import gov.usgs.cida.config.DynamicReadOnlyProperties;
import gov.usgs.cida.microservices.api.messaging.MessageBasedMicroservice;
import gov.usgs.cida.microservices.api.messaging.MicroserviceHandler;
import gov.usgs.cida.microservices.api.messaging.exception.MqConnectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Set;

import javax.naming.NamingException;

/**
 *
 * @author thongsav
 */
public final class MicroserviceMsgserviceFactory {
	private static final Logger log = LoggerFactory.getLogger(MicroserviceMsgserviceFactory.class);

	public final static String MQ_HOST_JNDI_NAME = "messaging.service.host";
	public final static String MQ_USER_JNDI_NAME = "messaging.service.user";
	public final static String MQ_PASS_JNDI_NAME = "messaging.service.password";
	public final static String MQ_CONSUMERS_JNDI_NAME = "messaging.consumers.per.service";
	private final static Integer DEFAULT_NUMBER_OF_CONSUMERS_PER_SERVICE = 10;
	
	public final static String MQ_RETRY_RATE_JNDI_NAME = "messaging.connection.retry.rate.ms";
	private final static Integer DEFAULT_RETRY_RATE = 15000; //15 seconds
	
	public static HashMap<String, MicroserviceMsgserviceFactory> FACTORY_INSTANCES = new HashMap<>();

	private String serviceName;
	private MicroserviceMsgservice serviceInstance;
	
	public MicroserviceMsgserviceFactory(String serviceName) {
		this.serviceName = serviceName;
	}
	
	private static DynamicReadOnlyProperties props = null;
	
	private static DynamicReadOnlyProperties getPropInstance() {
		if (null == props) {
			try {
				props = new DynamicReadOnlyProperties().addJNDIContexts();
			} catch (NamingException e) {
				log.warn("Error occured during initing property reader", e);
			}
		}
		return props;
	}
	
	public static MicroserviceMsgserviceFactory getInstance(String serviceName) {
		MicroserviceMsgserviceFactory result = null;

		if (null == FACTORY_INSTANCES.get(serviceName)) {
			FACTORY_INSTANCES.put(serviceName, new MicroserviceMsgserviceFactory(serviceName));
		}
		
		result = FACTORY_INSTANCES.get(serviceName);

		return result;
	}
	
	/**
	 * This will build a service assuming MQ is available.
	 * 
	 * @param inHandlers
	 * @return
	 * @throws MqConnectionException failure to connect
	 */
	public MicroserviceMsgservice buildMicroserviceMsgservice(Set<Class<? extends MicroserviceHandler>> inHandlers) 
			throws MqConnectionException {
		return buildMicroserviceMsgservice(inHandlers, false);
	}
	
	/**
	 * This is will build a MessageBasedMicroservice that will maintain a connection (always trying to reconnect) indefinitely.
	 * 
	 * @param inHandlers
	 * @return
	 * @throws MqConnectionException 
	 */
	public MessageBasedMicroservice buildPersistentMicroserviceMsgservice(Set<Class<? extends MicroserviceHandler>> inHandlers) 
			throws MqConnectionException {
		return buildMicroserviceMsgservice(inHandlers, true);
	}
	
	/**
	 * Build a MicroserviceMsgservice which will create queue and bind handlers.
	 * 
	 * @param inHandlers
	 * @param persistent true to wait for a connection, false to spawn a new thread which waits for the connection
	 * 
	 * @return
	 * @throws MqConnectionException 
	 */
	private MicroserviceMsgservice buildMicroserviceMsgservice(Set<Class<? extends MicroserviceHandler>> inHandlers, 
			boolean persistent) throws MqConnectionException {
		log.debug("Instantiating new MicroserviceMsgservice for {}", serviceName);
		
		Integer consumers = DEFAULT_NUMBER_OF_CONSUMERS_PER_SERVICE;
		String consumersString = getPropInstance().getProperty(MQ_CONSUMERS_JNDI_NAME);
		if(consumers != null) {
			try {
				consumers = Integer.parseInt(consumersString);
			} catch (Exception e) {
				log.warn("Invalid integer specified for {}", MQ_CONSUMERS_JNDI_NAME);
			}
		}
		
		long retryRate = DEFAULT_RETRY_RATE;
		String retryRateRaw = getPropInstance().getProperty(MQ_RETRY_RATE_JNDI_NAME);
		if(retryRateRaw != null) {
			try {
				retryRate = Long.parseLong(retryRateRaw);
			} catch (Exception e) {
				log.warn("Invalid long specified for {}", MQ_RETRY_RATE_JNDI_NAME);
			}
		}

		if(!persistent) {
			serviceInstance = new MicroserviceMsgservice(getPropInstance().getProperty(MQ_HOST_JNDI_NAME), "amq.headers", serviceName, 
					inHandlers, consumers, getPropInstance().getProperty(MQ_USER_JNDI_NAME), getPropInstance().getProperty(MQ_PASS_JNDI_NAME));
			serviceInstance.initialize();
		} else {
			serviceInstance = new MicroserviceMsgservice(getPropInstance().getProperty(MQ_HOST_JNDI_NAME), "amq.headers", serviceName, 
					inHandlers, consumers, getPropInstance().getProperty(MQ_USER_JNDI_NAME), getPropInstance().getProperty(MQ_PASS_JNDI_NAME),
					retryRate, true, true);
			(new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						serviceInstance.initialize();
					} catch (MqConnectionException e) {
						throw new RuntimeException("Unable to establish persistant MQ connection", e);
					}
				}
				
			})).start();
		}
		

		return serviceInstance;
	}
	
	public MicroserviceMsgservice getMicroserviceMsgservice() {
		return serviceInstance;
	}
}
