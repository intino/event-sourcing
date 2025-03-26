package systems.intino.eventsourcing.jms;

import io.intino.alexandria.logger.Logger;
import jakarta.jms.*;
import org.apache.activemq.ActiveMQSession;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class DurableTopicConsumer extends JmsConsumer {
	protected List<Consumer<Message>> listeners;
	private final String subscriberId;

	public DurableTopicConsumer(Session session, String topic, String subscriberId) throws JMSException {
		this(session, topic, null, subscriberId);
	}

	public DurableTopicConsumer(Session session, String topic, String messageSelector, String subscriberId) throws JMSException {
		super(session, session.createTopic(topic));
		this.subscriberId = subscriberId;
		this.listeners = new ArrayList<>();
		try {
			this.consumer = session.createDurableSubscriber((Topic) destination, subscriberId, messageSelector, true);
		} catch (JMSException ex) {
			Logger.error(ex);
		}
	}

	public String subscriberId() {
		return subscriberId;
	}

	public void destroy() {
		try {
			if (consumer == null) return;
			consumer.close();
			if (subscriberId != null && !((ActiveMQSession) session).isClosed()) session.unsubscribe(subscriberId);
		} catch (InvalidDestinationException ignored) {
		} catch (JMSException e) {
			Logger.error(e);
		}
	}


}
