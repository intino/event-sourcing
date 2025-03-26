package systems.intino.eventsourcing.datalake.file.message;

import io.intino.alexandria.FS;
import io.intino.alexandria.Scale;
import io.intino.alexandria.Timetag;
import systems.intino.eventsourcing.datalake.Datalake;
import systems.intino.eventsourcing.datalake.Datalake.Store.Tub;
import systems.intino.eventsourcing.event.message.MessageEvent;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static systems.intino.eventsourcing.event.Event.Format.Message;

public class MessageEventSource implements Datalake.Store.Source<MessageEvent> {
	private final File root;

	public MessageEventSource(File file) {
		this.root = file;
	}

	@Override
	public String name() {
		return root.getName();
	}

	@Override
	public Tub<MessageEvent> tub(Timetag timetag) {
		File file = new File(root, timetag.value() + Message.extension());
		return file.exists() ? new MessageEventTub(file) : null;
	}

	@Override
	public Scale scale() {
		return Optional.ofNullable(first()).map(Tub::scale).orElse(null);
	}

	@Override
	public Tub<MessageEvent> first() {
		return tubs().findFirst().orElse(null);
	}

	@Override
	public Tub<MessageEvent> last() {
		List<File> files = tubFiles().toList();
		return files.isEmpty() ? null : new MessageEventTub(files.getLast());
	}

	@Override
	public Stream<Tub<MessageEvent>> tubs() {
		return tubFiles().map(MessageEventTub::new);
	}

	@Override
	public Tub<MessageEvent> on(Timetag tag) {
		return new MessageEventTub(new File(root, tag.value() + Message.extension()));
	}

	private Stream<File> tubFiles() {
		return FS.filesIn(root, pathname -> pathname.getName().endsWith(Message.extension()));
	}
}
