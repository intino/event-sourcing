package systems.intino.eventsourcing.jms;

import io.intino.alexandria.logger.Logger;
import jakarta.jms.JMSException;
import jakarta.jms.Session;

public class TopicConsumer extends JmsConsumer {

	public TopicConsumer(Session session, String topic) throws JMSException {
		this(session, topic, null);
	}

	public TopicConsumer(Session session, String topic, String messageSelector) throws JMSException {
		super(session, session.createTopic(topic));
		try {
			this.consumer = session.createConsumer(destination, messageSelector, true);
		} catch (JMSException ex) {
			Logger.error(ex);
		}
	}
}