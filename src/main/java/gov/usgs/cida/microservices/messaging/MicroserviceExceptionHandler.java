package gov.usgs.cida.microservices.messaging;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.TopologyRecoveryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dmsibley
 */
public class MicroserviceExceptionHandler implements ExceptionHandler {
	private static final Logger log = LoggerFactory.getLogger(MicroserviceExceptionHandler.class);

	@Override
	public void handleUnexpectedConnectionDriverException(Connection conn, Throwable exception) {
		log.error("handleUnexpectedConnectionDriverException", exception);
	}

	@Override
	public void handleReturnListenerException(Channel channel, Throwable exception) {
		log.error("handleReturnListenerException", exception);
	}

	@Override
	public void handleConfirmListenerException(Channel channel, Throwable exception) {
		log.error("handleConfirmListenerException", exception);
	}

	@Override
	public void handleBlockedListenerException(Connection connection, Throwable exception) {
		log.error("handleBlockedListenerException", exception);
	}

	@Override
	public void handleConsumerException(Channel channel, Throwable exception, Consumer consumer, String consumerTag, String methodName) {
		log.error("handleConsumerException", exception);
	}

	@Override
	public void handleConnectionRecoveryException(Connection conn, Throwable exception) {
		log.error("handleConnectionRecoveryException", exception);
	}

	@Override
	public void handleChannelRecoveryException(Channel ch, Throwable exception) {
		log.error("handleChannelRecoveryException", exception);
	}

	@Override
	public void handleTopologyRecoveryException(Connection conn, Channel ch, TopologyRecoveryException exception) {
		log.error("handleTopologyRecoveryException", exception);
	}

}
