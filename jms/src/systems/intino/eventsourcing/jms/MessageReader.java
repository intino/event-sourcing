package systems.intino.eventsourcing.jms;

import io.intino.alexandria.logger.Logger;
import jakarta.jms.BytesMessage;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;

public class MessageReader {

	public static String textFrom(Message message) {
		try {
			if (message instanceof BytesMessage) {
				byte[] data = new byte[(int) ((BytesMessage) message).getBodyLength()];
				((BytesMessage) message).readBytes(data);
				return new String(data);
			} else return ((TextMessage) message).getText();
		} catch (JMSException e) {
			Logger.error(e);
			return "";
		}
	}
}
