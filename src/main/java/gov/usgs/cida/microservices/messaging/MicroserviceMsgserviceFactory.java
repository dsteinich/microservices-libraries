package gov.usgs.cida.microservices.messaging;

import gov.usgs.cida.microservices.api.messaging.MicroserviceHandler;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Set;

import javax.naming.Context;
import javax.naming.InitialContext;
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
	
	public static HashMap<String, MicroserviceMsgserviceFactory> FACTORY_INSTANCES = new HashMap<>();

	private String serviceName;
	private MicroserviceMsgservice serviceInstance;
	
	public MicroserviceMsgserviceFactory(String serviceName) {
		this.serviceName = serviceName;
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
			serviceInstance = new MicroserviceMsgservice(getJNDIValue(MQ_HOST_JNDI_NAME), "amq.headers", serviceName, inHandlers, getJNDIValue(MQ_USER_JNDI_NAME), getJNDIValue(MQ_PASS_JNDI_NAME));
		} catch (IOException e) {
			throw new RuntimeException("Unable to instantiate MicroserviceMsgservice", e);
		}
		return serviceInstance;
	}
	
	public MicroserviceMsgservice getMicroserviceMsgservice() {
		return serviceInstance;
	}
	
	private static String getJNDIValue(String var) {
		String result;
		try {
			Context ctx = new InitialContext();
			result =  (String) ctx.lookup("java:comp/env/" + var);
		} catch (NamingException ex) {
			result = "";
		}
		return result;
	}
}
