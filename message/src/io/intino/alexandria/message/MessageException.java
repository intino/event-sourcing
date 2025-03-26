package systems.intino.eventsourcing.message;

public class MessageException extends RuntimeException {
	public MessageException(String message, Exception e) {
		super(message, e);
	}
}
