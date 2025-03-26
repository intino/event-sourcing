package systems.intino.eventsourcing.jms;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import org.apache.activemq.command.ActiveMQTextMessage;

public class MessageWriter {
	public static Message write(String message) throws JMSException {
		TextMessage textMessage = new ActiveMQTextMessage();
		textMessage.setText(message);
		return textMessage;
	}
}
