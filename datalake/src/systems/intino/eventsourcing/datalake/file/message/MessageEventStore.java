package systems.intino.eventsourcing.datalake.file.message;

import io.intino.alexandria.FS;
import systems.intino.eventsourcing.datalake.Datalake;
import systems.intino.eventsourcing.datalake.file.FileStore;
import systems.intino.eventsourcing.event.Event;
import systems.intino.eventsourcing.event.message.MessageEvent;

import java.io.File;
import java.util.stream.Stream;

public class MessageEventStore implements Datalake.Store<MessageEvent>, FileStore {
	private final File root;

	public MessageEventStore(File root) {
		this.root = root;
	}

	@Override
	public Stream<Tank<MessageEvent>> tanks() {
		return FS.directoriesIn(root).map(MessageEventTank::new);
	}

	public File directory() {
		return root;
	}

	@Override
	public MessageEventTank tank(String name) {
		return new MessageEventTank(new File(root, name));
	}

	@Override
	public String fileExtension() {
		return Event.Format.Message.extension();
	}
}
