package systems.intino.eventsourcing.jms;

import io.intino.alexandria.logger.Logger;
import jakarta.jms.JMSException;
import jakarta.jms.Session;

public class QueueConsumer extends JmsConsumer {
	public QueueConsumer(Session session, String topic) throws JMSException {
		this(session, topic, null);
	}

	public QueueConsumer(Session session, String path, String messageSelector) throws JMSException {
		super(session, session.createQueue(path));
		try {
			this.consumer = session.createConsumer(destination, messageSelector, true);
		} catch (JMSException ex) {
			Logger.error(ex);
		}
	}
}