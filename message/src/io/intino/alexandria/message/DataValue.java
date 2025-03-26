package systems.intino.eventsourcing.message;

class DataValue implements Message.Value {

	private final String data;

	DataValue(String data) {
		this.data = data;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public String data() {
		return data;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T as(Class<T> type) {
		return (T) Parser.of(type).parse(data);
	}

	@Override
	public String toString() {
		return data;
	}
}
