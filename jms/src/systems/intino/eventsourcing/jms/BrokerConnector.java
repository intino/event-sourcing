package systems.intino.eventsourcing.jms;

import io.intino.alexandria.logger.Logger;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;

public class BrokerConnector {

	public static Connection createConnection(ConnectionConfig config, ConnectionListener connectionListener) {
		if (config.hasSSlCredentials()) return createSSLConnection(config, connectionListener);
		return createPlainConnection(config, connectionListener);
	}

	private static Connection createPlainConnection(ConnectionConfig config, ConnectionListener listener) {
		try {
			ActiveMQConnection connection = (ActiveMQConnection) new ActiveMQConnectionFactory(config.user(), config.password(), config.url()).createConnection();
			connection.addTransportListener(listener);
			return connection;
		} catch (JMSException e) {
			Logger.error(e);
			return null;
		}
	}

	private static Connection createSSLConnection(ConnectionConfig config, ConnectionListener listener) {
		try {
			ActiveMQSslConnectionFactory factory = new ActiveMQSslConnectionFactory(config.url());
			factory.setKeyStore(config.keyStore().getAbsolutePath());
			factory.setTrustStore(config.trustStore().getAbsolutePath());
			factory.setKeyStorePassword(config.keyStorePassword());
			factory.setTrustStorePassword(config.trustStorePassword());
			factory.setUserName(config.user());
			factory.setPassword(config.password());
			ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
			connection.addTransportListener(listener);
			return connection;
		} catch (Exception e) {
			Logger.error(e);
			return null;
		}
	}
}
