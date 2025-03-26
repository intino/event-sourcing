package systems.intino.eventsourcing.jms;

import org.apache.activemq.transport.TransportListener;

import java.io.IOException;

public interface ConnectionListener extends TransportListener {
	@Override
	default void onCommand(Object o) {

	}

	@Override
	default void onException(IOException e) {

	}
}
