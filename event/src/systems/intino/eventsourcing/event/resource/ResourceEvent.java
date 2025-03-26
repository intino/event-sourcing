package systems.intino.eventsourcing.event.resource;

import io.intino.alexandria.Resource;
import systems.intino.eventsourcing.event.Event;
import systems.intino.eventsourcing.message.Message;

import java.io.File;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

import static systems.intino.eventsourcing.event.resource.ResourceHelper.*;
import static java.util.Objects.requireNonNull;

public class ResourceEvent implements Event {
	public static final String METADATA = ".metadata";
	static final String ENTRY_NAME_SEP = "$";

	private final String type;
	private final String ss;
	private Instant ts = Instant.now();
	private final Resource resource;

	public ResourceEvent(String type, String ss, Resource resource) {
		this.type = requireNonNull(type, "type cannot be null");
		this.ss = requireNonNull(ss, "ss cannot be null");
		this.resource = requireNonNull(resource, "resource cannot be null");
		if (resource.name().isBlank()) throw new IllegalArgumentException("Resource name cannot be blank");
	}

	public ResourceEvent(String type, String ss, File file) {
		this(type, ss, new Resource(file));
	}

	public Resource resource() {
		return resource;
	}

	@Override
	public String type() {
		return type;
	}

	@Override
	public Instant ts() {
		return ts;
	}

	public REI getREI() {
		return new REI(type, ss, ts, resource.name());
	}

	public ResourceEvent ts(Instant ts) {
		this.ts = requireNonNull(ts, "ts cannot be null");
		return this;
	}

	@Override
	public String ss() {
		return ss;
	}

	@Override
	public Format format() {
		return Format.Resource;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ResourceEvent that = (ResourceEvent) o;
		return Objects.equals(type, that.type) && Objects.equals(ss, that.ss) && Objects.equals(ts, that.ts);
	}

	@Override
	public int hashCode() {
		return Objects.hash(type, ss, ts);
	}

	@Override
	public String toString() {
		Message message = new Message(getClass().getSimpleName());
		message.set("ss", ss());
		message.set("ts", ts());
		message.set("resource", resource().name());
		message.set("REI", getREI().toString());
		return message.toString();
	}

	public static ResourceEvent of(Resource resource) {
		Resource.Metadata metadata = resource.metadata();
		String type = metadata.getProperty(METADATA_TYPE).orElseThrow(() -> new MissingResourceMetadataException("Resource '" + resource.name() + "' does not declare the type in its metadata."));
		String ss = metadata.getProperty(METADATA_SS).orElseThrow(() -> new MissingResourceMetadataException("Resource '" + resource.name() + "' does not declare the ss in its metadata."));
		String ts = metadata.getProperty(METADATA_TS).orElseThrow(() -> new MissingResourceMetadataException("Resource '" + resource.name() + "' does not declare the ts in its metadata."));
		return new ResourceEvent(type, ss, resource).ts(Instant.parse(ts));
	}

	/**
	 * <p>Represents an id that uniquely identifies a resource event in a datalake. It has the form:</p>
	 *
	 * <p><b>tank</b>/<b>ss</b>/<b>ts-epoch-millis</b>/<b>resource-name</b></p>
	 */
	public static class REI {

		public static final String SEP = "/";
		public static final String ID_SEP = "#";
		public static final String NAME_SEP = "$";
		public static final int SIZE = 4;

		public static REI of(String rei) {
			try {
				return of(rei.split(SEP, SIZE));
			} catch (Exception e) {
				return of(rei.split(ID_SEP, SIZE)); // Compatibility with old versions
			}
		}

		public static REI of(String[] components) {
			if (components.length != 4)
				throw new MalformedREIException("REI must have 4 components: type, ss, ts and resource name");
			if (components[0] == null || components[0].isBlank())
				throw new MalformedREIException("type in REI cannot be null nor blank");
			if (components[1] == null || components[1].isBlank())
				throw new MalformedREIException("ss in REI cannot be null nor blank");
			if (components[2] == null || components[2].isBlank() || isNotAValidTs(components[2]))
				throw new MalformedREIException("ts in REI is not a valid ts");
			if (components[3] == null || components[3].isBlank())
				throw new MalformedREIException("resource name in REI cannot be null nor blank");
			return new REI(components);
		}

		private final String[] components;

		private REI(String[] components) {
			this.components = components;
		}

		public REI(String type, String ss, Instant ts, String resourceName) {
			this.components = new String[]{type, ss, String.valueOf(ts.toEpochMilli()), resourceName};
		}

		public String type() {
			return components[0];
		}

		public String ss() {
			return components[1];
		}

		public Instant ts() {
			return Instant.ofEpochMilli(Long.parseLong(components[2]));
		}

		public String tsRaw() {
			return components[2];
		}

		public String resourceName() {
			return components[3];
		}

		/**
		 * The id of the resource inside the tub
		 */
		public String resourceId() {
			return tsRaw() + ID_SEP + resourceName().replace("/", NAME_SEP);
		}

		private static boolean isNotAValidTs(String ts) {
			try {
				Instant.ofEpochMilli(Long.parseLong(ts));
				return false;
			} catch (Exception ignored) {
				return true;
			}
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			REI rei = (REI) o;
			return Arrays.equals(components, rei.components);
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(components);
		}

		@Override
		public String toString() {
			return String.join(SEP, components);
		}
	}

	public static class MissingResourceMetadataException extends RuntimeException {
		public MissingResourceMetadataException(String message) {
			super(message);
		}
	}

	public static class MalformedREIException extends RuntimeException {
		public MalformedREIException(String message) {
			super(message);
		}
	}
}
