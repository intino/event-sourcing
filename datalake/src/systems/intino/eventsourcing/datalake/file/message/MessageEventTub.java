package systems.intino.eventsourcing.datalake.file.message;

import io.intino.alexandria.Timetag;
import io.intino.alexandria.logger.Logger;
import systems.intino.eventsourcing.datalake.Datalake;
import systems.intino.eventsourcing.datalake.FileTub;
import systems.intino.eventsourcing.event.EventStream;
import systems.intino.eventsourcing.event.message.MessageEvent;

import java.io.File;
import java.io.IOException;
import java.util.stream.Stream;

import static systems.intino.eventsourcing.event.Event.Format.Message;

public class MessageEventTub implements Datalake.Store.Tub<MessageEvent>, FileTub {
	private final File zim;

	public MessageEventTub(File zim) {
		this.zim = zim;
	}

	public String name() {
		return zim.getName().replace(Message.extension(), "");
	}

	@Override
	public Timetag timetag() {
		return new Timetag(name());
	}

	@Override
	public Stream<MessageEvent> events() {
		try {
			return EventStream.of(zim);
		} catch (IOException e) {
			Logger.error(e);
			return Stream.empty();
		}
	}

	@Override
	public String fileExtension() {
		return Message.extension();
	}

	public File file() {
		return zim;
	}
}