package systems.intino.eventsourcing.jms;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class ConnectionConfig {
	private final String url;
	private final String user;
	private final String password;
	private final String clientId;
	private final File keyStore;
	private final File trustStore;
	private final String keyStorePassword;
	private final String trustStorePassword;
	private final long defaultTimeoutAmount;
	private final TimeUnit defaultTimeoutUnit;

	public ConnectionConfig(String url, String user, String password, String clientId) {
		this(url, user, password, clientId, null, null, null, null, -1, TimeUnit.MINUTES);
	}

	public ConnectionConfig(String url, String user, String password, String clientId, File keyStore, File trustStore, String keyStorePassword, String trustStorePassword) {
		this(url, user, password, clientId, keyStore, trustStore, keyStorePassword, trustStorePassword, -1, TimeUnit.MINUTES);
	}

	public ConnectionConfig(String url, String user, String password, String clientId, File keyStore, File trustStore, String keyStorePassword, String trustStorePassword, long defaultTimeoutAmount, TimeUnit defaultTimeoutUnit) {
		this.url = url;
		this.user = user;
		this.password = password;
		this.clientId = clientId;
		this.keyStore = keyStore;
		this.trustStore = trustStore;
		this.keyStorePassword = keyStorePassword;
		this.trustStorePassword = trustStorePassword;
		this.defaultTimeoutAmount = defaultTimeoutAmount;
		this.defaultTimeoutUnit = defaultTimeoutUnit;
	}

	public boolean hasSSlCredentials() {
		return keyStore != null;
	}

	public String url() {
		return url;
	}

	public String user() {
		return user;
	}

	public String password() {
		return password;
	}

	public String clientId() {
		return clientId;
	}

	public File keyStore() {
		return keyStore;
	}

	public File trustStore() {
		return trustStore;
	}

	public String keyStorePassword() {
		return keyStorePassword;
	}

	public String trustStorePassword() {
		return trustStorePassword;
	}

	public long defaultTimeoutAmount() {
		return defaultTimeoutAmount;
	}

	public TimeUnit defaultTimeoutUnit() {
		return defaultTimeoutUnit;
	}
}
