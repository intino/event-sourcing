package systems.intino.eventsourcing.jms;

import io.intino.alexandria.logger.Logger;
import jakarta.jms.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public abstract class JmsConsumer {
	protected Session session;
	protected Destination destination;
	protected List<Consumer<Message>> listeners;
	protected MessageConsumer consumer;

	JmsConsumer(Session session, Destination destination) {
		this.session = session;
		this.destination = destination;
		this.listeners = new ArrayList<>();
	}

	public void listen(Consumer<Message> listener) {
		try {
			this.listeners.add(listener);
			if (this.consumer != null) consumer.setMessageListener(m -> listeners.forEach(l -> l.accept(m)));
		} catch (JMSException e) {
			Logger.error(e);
		}
	}

	public List<Consumer<Message>> listeners() {
		return Collections.unmodifiableList(listeners);
	}

	public void removeListener(Consumer<Message> listener) {
		listeners.remove(listener);
	}

	public void close() {
		if (consumer != null) try {
			consumer.close();
		} catch (JMSException e) {
			Logger.error(e);
		}
	}
}
