package systems.intino.eventsourcing.message;

interface Parser {

	Object parse(String text);

	static Parser of(Class<?> aClass) {
		return ParserFactory.get(aClass);
	}

	class ParseException extends RuntimeException {
		public ParseException(String message, Throwable cause) {
			super(message, cause);
		}
	}
}
