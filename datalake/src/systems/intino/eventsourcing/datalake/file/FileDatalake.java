package systems.intino.eventsourcing.datalake.file;

import systems.intino.eventsourcing.datalake.Datalake;
import systems.intino.eventsourcing.datalake.file.message.MessageEventStore;
import systems.intino.eventsourcing.datalake.file.resource.ResourceEventStore;
import systems.intino.eventsourcing.event.message.MessageEvent;

import java.io.File;

public record FileDatalake(File root) implements Datalake {
	public FileDatalake(File root) {
		this.root = root;
		checkStore();
	}

	private void checkStore() {
		messageStoreFolder().mkdirs();
		resourceStoreFolder().mkdirs();
	}

	@Override
	public Store<MessageEvent> messageStore() {
		return new MessageEventStore(messageStoreFolder());
	}

	@Override
	public ResourceStore resourceStore() {
		return new ResourceEventStore(resourceStoreFolder());
	}

	public File messageStoreFolder() {
		return new File(root, MessageStoreFolder);
	}

	private File resourceStoreFolder() {
		return new File(root, ResourceStoreFolder);
	}
}