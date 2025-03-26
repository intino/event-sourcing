package systems.intino.eventsourcing.event;

import java.io.File;
import java.time.Instant;
import java.util.Arrays;

public interface Event extends Comparable<Event> {

	String type();

	Instant ts();

	String ss();

	Format format();

	@Override
	default int compareTo(Event o) {
		return ts().compareTo(o.ts());
	}

	enum Format {
		Unknown(""),
		Message(".zim"),
		Resource(".zip");

		private final String extension;

		Format(String extension) {
			this.extension = extension;
		}

		public String extension() {
			return extension;
		}

		public static Format byExtension(String extension) {
			return Arrays.stream(values()).filter(f -> f.extension.equalsIgnoreCase(extension)).findFirst().orElse(Unknown);
		}

		public static Format of(File file) {
			String name = file.getName();
			int index = name.lastIndexOf('.');
			return Format.byExtension(index < 0 ? "" : name.substring(index));
		}
	}
}
