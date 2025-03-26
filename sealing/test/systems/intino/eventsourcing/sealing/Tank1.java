package systems.intino.eventsourcing.sealing;

import systems.intino.eventsourcing.event.message.MessageEvent;
import systems.intino.eventsourcing.message.Message;

public class Tank1 extends MessageEvent {

	public Tank1(Message message) {
		super(message);
	}

	Tank1(MessageEvent event) {
		super(event.toMessage());
	}

	Tank1(String ss) {
		super(Tank1.class.getSimpleName(), ss);
	}

	int entries() {
		return toMessage().get("entries").asInteger();
	}

	Tank1 entries(int v) {
		toMessage().set("entries", v);
		return this;
	}
}
