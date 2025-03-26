package systems.intino.eventsourcing.datalake.file.message;

import io.intino.alexandria.FS;
import systems.intino.eventsourcing.datalake.Datalake;
import systems.intino.eventsourcing.event.message.MessageEvent;

import java.io.File;
import java.util.stream.Stream;

public class MessageEventTank implements Datalake.Store.Tank<MessageEvent> {
	private final File root;

	MessageEventTank(File root) {
		this.root = root;
	}

	@Override
	public String name() {
		return root.getName();
	}

	public Datalake.Store.Source<MessageEvent> source(String name) {
		return new MessageEventSource(new File(root, name));
	}

	@Override
	public Stream<Datalake.Store.Source<MessageEvent>> sources() {
		return FS.directoriesIn(root).map(MessageEventSource::new);
	}

	public File root() {
		return root;
	}


}