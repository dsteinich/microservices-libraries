package gov.usgs.cida.microservices.api.messaging;

import java.io.IOException;
import java.util.Map;

import gov.usgs.cida.microservices.messaging.MicroserviceMsgservice;

/**
 *
 * @author dmsibley
 */
public interface MicroserviceHandler {
	public void handle(String requestId, String serviceRequestId, Map<String, Object> params, byte[] body, MicroserviceMsgservice msgService) throws IOException;
	public Iterable<Map<String, Object>> getBindings(String serviceName); 
}
