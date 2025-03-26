package systems.intino.eventsourcing.message;

class NullValue implements Message.Value {

	@Override
	public boolean isEmpty() {
		return true;
	}

	@Override
	public String data() {
		return null;
	}

	@Override
	public <T> T as(Class<T> type) {
		return isBoolean(type) ? (T) Boolean.FALSE : null;
	}

	private <T> boolean isBoolean(Class<T> type) {
		return type.equals(boolean.class) || type.equals(Boolean.class);
	}
}
