package systems.intino.eventsourcing.jms;

import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Session;

public class QueueProducer extends JmsProducer {

	public QueueProducer(Session session, String path) throws JMSException {
		super(session, session.createQueue(path));
	}

	public QueueProducer(Session session, String channel, int messageExpirationSeconds) throws JMSException {
		super(session, session.createQueue(channel), messageExpirationSeconds);
	}

	public QueueProducer(Session session, Destination destination) {
		super(session, destination);
	}

	public QueueProducer(Session session, Destination destination, int messageExpirationSeconds) {
		super(session, destination, messageExpirationSeconds);
	}
}
