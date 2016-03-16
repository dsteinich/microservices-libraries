package gov.usgs.cida.microservices.messaging;

import gov.usgs.cida.config.DynamicReadOnlyProperties;
import gov.usgs.cida.microservices.api.messaging.MicroserviceHandler;

import java.io.IOException;

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
	
	public MicroserviceMsgservice buildMicroserviceMsgservice(Set<Class<? extends MicroserviceHandler>> inHandlers) {
		log.debug("Instantiating new MicroserviceMsgservice for {}", serviceName);
		try {
			Integer consumers = DEFAULT_NUMBER_OF_CONSUMERS_PER_SERVICE;
			String consumersString = getPropInstance().getProperty(MQ_CONSUMERS_JNDI_NAME);
			if(consumers != null) {
				try {
					consumers = Integer.parseInt(consumersString);
				} catch (Exception e) {
					log.warn("Invalid integer specified for {}", MQ_CONSUMERS_JNDI_NAME);
				}
			}
			serviceInstance = new MicroserviceMsgservice(getPropInstance().getProperty(MQ_HOST_JNDI_NAME), "amq.headers", serviceName, 
					inHandlers, consumers, getPropInstance().getProperty(MQ_USER_JNDI_NAME), getPropInstance().getProperty(MQ_PASS_JNDI_NAME));
		} catch (IOException e) {
			throw new RuntimeException("Unable to instantiate MicroserviceMsgservice", e);
		}
		return serviceInstance;
	}
	
	public MicroserviceMsgservice getMicroserviceMsgservice() {
		return serviceInstance;
	}
}
