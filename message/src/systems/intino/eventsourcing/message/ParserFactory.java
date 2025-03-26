package systems.intino.eventsourcing.message;


import systems.intino.eventsourcing.message.Parser.ParseException;

import java.lang.reflect.Array;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static systems.intino.eventsourcing.message.Message.*;

class ParserFactory {
	private static final Map<Class<?>, Parser> Parsers;

	static {
		Parsers = new HashMap<>();
		primitives();
		boxed();
		text();
		datetime();
	}

	private static void primitives() {
		register(boolean.class, Boolean::parseBoolean);
		register(byte.class, Byte::parseByte);
		register(char.class, s -> (char)Integer.parseInt(s));
		register(short.class, Short::parseShort);
		register(int.class, Integer::parseInt);
		register(long.class, Long::parseLong);
		register(float.class, Float::parseFloat);
		register(double.class, Double::parseDouble);
	}

	private static void boxed() {
		register(Boolean.class, Parsers.get(boolean.class));
		register(Byte.class, Parsers.get(byte.class));
		register(Character.class, Parsers.get(char.class));
		register(Short.class, Parsers.get(short.class));
		register(Integer.class, Parsers.get(int.class));
		register(Long.class, Parsers.get(long.class));
		register(Float.class, Parsers.get(float.class));
		register(Double.class, Parsers.get(double.class));
		register(Number.class, ParserFactory::parseNumber);
	}

	private static void text() {
		register(String.class, text -> text);
		register(CharSequence.class, text -> text);
	}

	private static void datetime() {
		register(Instant.class, Instant::parse);
		register(LocalTime.class, LocalTime::parse);
		register(LocalDate.class, LocalDate::parse);
		register(LocalDateTime.class, LocalDateTime::parse);
	}

	static boolean contains(Class<?> type) {
		return Parsers.containsKey(type);
	}

	static Parser get(Class<?> type) {
		if(!contains(type)) throw new NoSuchParserException("No parser found for type " + type);
		return Parsers.get(type);
	}

	private static void register(Class<?> clazz, Parser parser) {
		Parsers.put(clazz, safe(clazz, parser));
		if(!clazz.isArray()) registerArrayOf(clazz);
	}

	private static void registerArrayOf(Class<?> clazz) {
		register(Array.newInstance(clazz, 0).getClass(), ArrayParser.of(clazz));
	}

	private static Number parseNumber(String str) {
		Number[] result = new Number[1];
		for(Class<?> numberType : List.of(Long.class, Double.class)) {
			if(tryParseNumber(numberType, str, result)) return result[0];
		}
		throw new NumberFormatException(str);
	}

	private static boolean tryParseNumber(Class<?> numberType, String str, Number[] result) {
		try {
			result[0] = (Number) get(numberType).parse(str);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	private static Parser safe(Class<?> targetType, Parser parser) {
		return textToParse -> {
			try {
				return textToParse == null || Null.equals(textToParse) ? null : parser.parse(textToParse);
			} catch (Exception e) {
				throw new ParseException("Could not parse " + textToParse + " to " + targetType + ": " + e.getMessage(), e);
			}
		};
	}

	@SuppressWarnings("unchecked")
	static <T> Class<T[]> arrayTypeOf(Class<?> elementType) {
		return (Class<T[]>) Array.newInstance(elementType, 0).getClass();
	}

	private static class ArrayParser implements Parser {
		private final Class<?> elementType;

		private ArrayParser(Class<?> elementType) {
			this.elementType = elementType;
		}

		@Override
		public Object parse(String text) {
			Parser parser = Parser.of(elementType);
			String[] items = text.isEmpty() ? new String[0] : text.split(ListSepStr);
			Object result = Array.newInstance(elementType, items.length);
			for (int i = 0; i < items.length; i++) {
				String item = items[i];
				Array.set(result, i, (Null.equals(item) ? null : parser.parse(item)));
			}
			return result;
		}

		private static ArrayParser of(Class<?> type) {
			return new ArrayParser(type);
		}
	}

	private static class NoSuchParserException extends RuntimeException {
		public NoSuchParserException(String message) {
			super(message);
		}
	}
}
