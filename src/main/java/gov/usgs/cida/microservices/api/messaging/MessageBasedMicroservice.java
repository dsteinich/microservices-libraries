package gov.usgs.cida.microservices.api.messaging;

import gov.usgs.cida.microservices.api.messaging.exception.MqConnectionException;

public interface MessageBasedMicroservice {

	/**
	 * Connect to MQ, initialize queues, bind handlers
	 * 
	 * @throws MqConnectionException
	 */
	void initialize() throws MqConnectionException;

	String getServiceName();

	void bindConsumer(String queueName, MicroserviceHandler bindingHandler);
}