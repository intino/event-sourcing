import systems.intino.eventsourcing.jms.BrokerConnector;
import systems.intino.eventsourcing.jms.ConnectionConfig;
import systems.intino.eventsourcing.jms.ConnectionListener;
import systems.intino.eventsourcing.jms.DurableTopicConsumer;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Session;

public class ConnectionTest {
	public static void main(String[] args) throws JMSException, InterruptedException {
		Connection connection = BrokerConnector.createConnection(new ConnectionConfig("failover:(tcp://localhost:63000)", "digestor", "digestor", "digestor"), connectionListener());
		if (connection == null) return;
		connection.setClientID("clientId"); //different from subscription_id
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		DurableTopicConsumer topicConsumer = new DurableTopicConsumer(session, "bla.bla.example", "subscriber-id");
		Thread.sleep(10000);
		topicConsumer.close();
		session.unsubscribe("subscriber-id");
		session.close();
		connection.close();
	}

	private static ConnectionListener connectionListener() {
		return new ConnectionListener() {
			@Override
			public void transportInterupted() {
			}

			@Override
			public void transportResumed() {
			}
		};
	}
}
