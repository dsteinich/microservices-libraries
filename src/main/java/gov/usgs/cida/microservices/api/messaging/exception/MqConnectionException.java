package gov.usgs.cida.microservices.api.messaging.exception;

public class MqConnectionException extends Exception {
	private static final long serialVersionUID = 1L;

	public MqConnectionException(String msg) {
		super(msg);
	}
	
	public MqConnectionException(String msg, Exception e) {
		super(msg);
	}
	
	public MqConnectionException(Exception e) {
		super(e);
	}
}
