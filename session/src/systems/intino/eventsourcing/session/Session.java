package systems.intino.eventsourcing.session;

import systems.intino.eventsourcing.event.Event.Format;

import java.io.InputStream;

public interface Session {
	String SessionExtension = ".session";

	String name();

	Format format();

	InputStream inputStream();
}
