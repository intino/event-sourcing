package systems.intino.eventsourcing.event;

import java.util.List;
import java.util.function.Consumer;

public interface EventHub {
	void sendEvent(String channel, Event event);

	void sendEvent(String channel, List<Event> event);

	void attachListener(String channel, Consumer<Event> onEventReceived);

	void attachListener(String channel, String subscriberId, Consumer<Event> onEventReceived);

	void detachListeners(String channel);

	void detachListeners(Consumer<Event> consumer);

	void attachRequestListener(String channel, RequestConsumer onEventReceived);

	void requestResponse(String channel, String event, Consumer<String> onResponse);

	interface RequestConsumer {
		String accept(String request);
	}
}