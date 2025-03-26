package systems.intino.eventsoucing.sealing;

import java.io.File;
import java.io.IOException;

public abstract class EventSorter {

	protected final File file;
	protected final File temp;

	public EventSorter(File file, File temp) {
		this.file = file;
		this.temp = temp;
	}

	public void sort() throws IOException {
		sort(file);
	}

	public abstract void sort(File destination) throws IOException;

	@FunctionalInterface
	public interface Factory {
		EventSorter of(File file, File temp) throws IOException;
	}
}
