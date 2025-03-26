package systems.intino.eventsourcing.jms;


import io.intino.alexandria.logger.Logger;
import jakarta.jms.*;

import static jakarta.jms.DeliveryMode.NON_PERSISTENT;


public abstract class JmsProducer {
	protected final Session session;
	private final Destination destination;
	private final int messageExpirationSeconds;
	private MessageProducer producer = null;

	public JmsProducer(Session session, Destination destination) {
		this(session, destination, 0);
	}

	public JmsProducer(Session session, Destination destination, int messageExpirationSeconds) {
		this.session = session;
		this.destination = destination;
		this.messageExpirationSeconds = messageExpirationSeconds;
		instanceProducer();
	}

	public boolean produce(Message message) {
		try {
			if (isClosed()) instanceProducer();
			producer.send(message);
			return true;
		} catch (Exception e) {
			Logger.error(e);
			return false;
		}
	}

	public boolean produce(Message message, int messageExpirationSeconds) {
		try {
			producer.send(message, NON_PERSISTENT, 4, messageExpirationSeconds * 1000L);
			return true;
		} catch (Exception e) {
			Logger.error(e);
			return false;
		}
	}

	public void close() {
		if (producer != null) try {
			producer.close();
			producer = null;
		} catch (JMSException e) {
			Logger.error(e);
		}
	}

	private void instanceProducer() {
		try {
			this.producer = session.createProducer(destination);
			this.producer.setTimeToLive(messageExpirationSeconds * 1000L);
			this.producer.setDeliveryMode(NON_PERSISTENT);
		} catch (JMSException e) {
			Logger.error(e);
		}
	}

	public boolean isClosed() {
		return producer == null;
	}
}
