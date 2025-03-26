package systems.intino.eventsourcing.event.message;

import systems.intino.eventsourcing.event.Event;
import systems.intino.eventsourcing.message.Message;

import java.time.Instant;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class MessageEvent implements Event {
	private static final String TS = "ts";
	private static final String SS = "ss";

	protected final Message message;
	protected Instant ts;
	protected String ss;

	public MessageEvent(String type, String ss) {
		this.message = new Message(type);
		this.message.set(TS, this.ts = Instant.now());
		this.message.set(SS, this.ss = requireNonNull(ss, "ss cannot be null"));
	}

	public MessageEvent(Message message) {
		this.message = message;
		this.ts = requireNonNull(message.get(TS).asInstant(), "ts cannot be null");
		this.ss = requireNonNull(message.get(SS).asString(), "ss cannot be null");
	}

	@Override
	public String type() {
		return message.type();
	}

	public Instant ts() {
		return ts;
	}

	public String ss() {
		return ss;
	}

	@Override
	public Format format() {
		return Format.Message;
	}

	public MessageEvent ts(Instant ts) {
		this.message.set(TS, this.ts = requireNonNull(ts));
		return this;
	}

	public MessageEvent ss(String ss) {
		this.message.set(SS, this.ss = requireNonNull(ss));
		return this;
	}

	public Message toMessage() {
		return message;
	}

	@Override
	public String toString() {
		return message.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		MessageEvent that = (MessageEvent) o;
		return Objects.equals(message, that.message);
	}

	@Override
	public int hashCode() {
		return Objects.hash(message);
	}
}
