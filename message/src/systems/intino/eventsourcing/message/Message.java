package systems.intino.eventsourcing.message;

import java.lang.reflect.Array;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static systems.intino.eventsourcing.message.ParserFactory.arrayTypeOf;
import static java.util.Objects.requireNonNull;

public class Message {
	static final char ListSep = '\u0001';
	static final String ListSepStr = String.valueOf(ListSep);
	static final char MultilineSep = '\u0002';
	static final String MultilineSepStr = String.valueOf(MultilineSep);
	static final String Null = "\0";

	private final Map<String, String> attributes;
	private final String type;
	private Message owner;
	private List<Message> components;

	public Message(String type) {
		this.type = requireNonNull(type, "Message type cannot be null");
		this.attributes = new LinkedHashMap<>();
	}

	public String type() {
		return type;
	}

	public boolean is(String type) {
		return type.equalsIgnoreCase(this.type);
	}

	public boolean isComponent() {
		return type.indexOf('.') >= 0;
	}

	public boolean isComponentOf(String type) {
		return isComponent() && this.type.startsWith(type);
	}

	public List<String> attributes() {
		return new ArrayList<>(attributes.keySet());
	}

	public boolean contains(String attribute) {
		return attributes.containsKey(attribute);
	}

	public Value get(String attribute) {
		return contains(attribute) ? new DataValue(deserialize(attributes.get(attribute))) : Value.Null;
	}

	public Message set(String attribute, Object value) {
		if(value == null) return set(attribute, Null);
		if(isIterable(value.getClass())) return setIterable(attribute, value);
		checkElementTypeIsSupported(value);
		return set(attribute, str(value));
	}

	private Message setIterable(String attribute, Object value) {
		attributes.put(attribute, serializedListFromIterable(value));
		return this;
	}

	public Message set(String attribute, String value) {
		attributes.put(attribute, serialize(value));
		return this;
	}

	void setUnsafe(String attribute, String value) {
		attributes.put(attribute, value);
	}

	private static final Pattern MultilinePattern = Pattern.compile("\r?\n");
	private String serialize(String value) {
		if(value == null) return Null;
		return MultilinePattern.matcher(value).replaceAll(MultilineSepStr);
	}

	private String deserialize(String value) {
		if(Null.equals(value)) return null;
		return value.replace(MultilineSep, '\n');
	}

	public Message append(String attribute, Object value) {
		if(value == null) return append(attribute, Null);
		if(isIterable(value.getClass())) return appendIterable(attribute, value);
		checkElementTypeIsSupported(value);
		return append(attribute, str(value));
	}

	private Message appendIterable(String attribute, Object value) {
		return append(attribute, serializedListFromIterable(value));
	}

	public Message append(String attribute, String newValue) {
		if(!contains(attribute)) return set(attribute, newValue);
		newValue = serialize(newValue);
		String oldValue = attributes.putIfAbsent(attribute, newValue);
		if(oldValue == null || oldValue.isEmpty() || oldValue.equals(ListSepStr)) {
			attributes.put(attribute, newValue);
		} else {
			attributes.put(attribute, oldValue + ListSepStr + newValue);
		}
		return this;
	}

	public Message rename(String attribute, String newName) {
		attributes.put(newName, attributes.remove(attribute));
		return this;
	}

	public Message remove(String attribute) {
		attributes.remove(attribute);
		return this;
	}

	private static final String ListSepStr2 = ListSepStr + ListSepStr;
	public Message remove(String attribute, Object value) {
		String str = value == null ? Null : str(value);
		attributes.computeIfPresent(attribute, (k, v) -> v.replace(str, "").replace(ListSepStr2, ListSepStr));
		return this;
	}

	public List<Message> components() {
		return components == null ? new ArrayList<>(0) : new ArrayList<>(components);
	}

	public List<Message> components(String type) {
		return components == null
				? new ArrayList<>(0)
				: components.stream().filter(c -> c.is(type)).collect(Collectors.toList());
	}

	public void add(Message component) {
		if(component == null) throw new NullPointerException("Component cannot be null");
		if (components == null) components = new ArrayList<>();
		components.add(component);
		component.owner = this;
	}

	public void add(List<Message> components) {
		if (components == null) return;
		if(this.components == null) this.components = new ArrayList<>(components.size());
		components.forEach(this::add);
	}

	public void remove(Message component) {
		if(component == null) return;
		components.remove(component);
	}

	public void remove(List<Message> components) {
		if(components == null) return;
		this.components.removeAll(components);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("[").append(qualifiedType()).append("]\n");
		for (Map.Entry<String, String> attribute : attributes.entrySet()) sb.append(stringOf(attribute)).append("\n");
		for (Message component : components()) sb.append("\n").append(component.toString());
		return sb.toString();
	}

	@Override
	public boolean equals(Object object) {
		if (this == object) return true;
		if (object == null || getClass() != object.getClass()) return false;
		Message message = (Message) object;
		return Objects.equals(type, message.type) &&
				attributes.keySet().stream().allMatch(k -> attributeEquals(message, k)) &&
				Objects.equals(components, message.components);
	}

	@Override
	public int hashCode() {
		return Objects.hash(type, attributes, components);
	}

	private String stringOf(Map.Entry<String, String> attribute) {
		return attribute.getKey() + ":" + (isMultiline(attribute.getValue()) ? indent(attribute.getValue()) : " " + attribute.getValue());
	}

	private boolean isMultiline(String value) {
		return value != null && value.contains("\n");
	}

	private String qualifiedType() {
		return owner != null ? owner.qualifiedType() + "." + type : type;
	}

	private boolean isIterable(Class<?> type) {
		return Iterable.class.isAssignableFrom(type) || type.isArray();
	}

	private Iterator<?> iteratorOf(Object value) {
		if(value instanceof Iterator) return (Iterator<?>) value;
		if(value instanceof Iterable) return ((Iterable<?>) value).iterator();
		return iteratorFromArray(value);
	}

	private Iterator<?> iteratorFromArray(Object array) {
		return new Iterator<>() {
			private final int length = Array.getLength(array);
			private int index = 0;

			@Override
			public boolean hasNext() {
				return index < length;
			}

			@Override
			public Object next() {
				return Array.get(array, index++);
			}
		};
	}

	private String serializedListFromIterable(Object value) {
		StringBuilder list = new StringBuilder();
		Iterator<?> iterator = iteratorOf(value);
		while(iterator.hasNext()) {
			Object item = iterator.next();
			checkElementTypeIsSupported(item);
			list.append(item == null ? Null : item).append(ListSep);
		}
		int length = list.length();
		if(length > 1) list.setLength(length - 1);
		return list.toString();
	}

	private void checkElementTypeIsSupported(Object value) {
		if(value == null) return;
		Class<?> clazz = value.getClass();
		if(clazz.isArray()) throw new IllegalArgumentException("Message does not support multidimensional arrays values");
		if(Parser.of(clazz) == null) throw new IllegalArgumentException("Message does not support values of type " + clazz);
	}

	private String str(Object value) {
		return String.valueOf(value);
	}

	private boolean attributeEquals(Message message, String key) {
		return message.contains(key) && Objects.equals(message.get(key).data(), get(key).data());
	}

	private static String indent(String text) {
		return "\n\t" + text.replaceAll("\\n", "\n\t");
	}

	public static Set<String> invalidCharacters() {
		return Set.of(Null, ListSepStr, MultilineSepStr);
	}

	/**<p>Wraps the value of a Message's attribute</p>*/
	public interface Value {

		Value Null = new NullValue();

		/**
		 * <p>Return true if the attribute was NOT found in the message, false otherwise.</p>
		 * */
		boolean isEmpty();
		/**
		 * <p>Returns true if the value of the attribute is null, false otherwise.</p>
		 * */
		default boolean isNull() { return data() == null;}
		/**
		 * <p>Returns the raw, underlying data value of this attribute.</p>
		 * */
		String data();

		/**
		 * <p>Returns true if this value can be parsed to the specified type, false otherwise.</p>
		 * */
		default <T> boolean is(Class<T> type) {return asOptional(type).isPresent();}

		/**
		 * <p>Parse the value to the specified type. Throws a ParseException if it fails.</p>
		 * */
		<T> T as(Class<T> type);

		default Boolean asBoolean() {Boolean v = as(Boolean.class); return v != null && v;}
		default Byte asByte() {return as(Byte.class);}
		default Character asCharacter() {return as(Character.class);}
		default Short asShort() {return as(Short.class);}
		default Integer asInteger() {return as(Integer.class);}
		default Long asLong() {return as(Long.class);}
		default Float asFloat() {return as(Float.class);}
		default Double asDouble() {return as(Double.class);}
		default String asString() {return as(String.class);}
		default Instant asInstant() {return as(Instant.class);}
		default LocalTime asLocalTime() {return as(LocalTime.class);}
		default LocalDate asLocalDate() {return as(LocalDate.class);}
		default LocalDateTime asLocalDateTime() {return as(LocalDateTime.class);}
		default String[] asMultiline() {return data().split("\n", -1);}

		/**
		 * <p>Returns an Optional wrapping this value's raw data. If the data is null, empty string or is not set, then an empty Optional is returned.</p>
		 * */
		default Optional<String> asOptional() {
			return (isEmpty() || isNull() || data().isEmpty()) ? Optional.empty() : Optional.ofNullable(data());
		}

		/**
		 * <p>Returns an Optional wrapping the result of parsing this value to the specified type. If it fails to parse, an empty Optional is returned.</p>
		 * */
		default <T> Optional<T> asOptional(Class<T> type) {
			try {return (isEmpty() || isNull() || data().isEmpty()) ? Optional.empty() : Optional.ofNullable(as(type));} catch (Exception ignored) {return Optional.empty();}
		}

		/**
		 * <p>Returns the value parsed to the specified type. If it fails, then it will return the specified default value.</p>
		 * */
		default <T> T orElse(Class<T> type, T valueIfFail) {
			return asOptional(type).orElse(valueIfFail);
		}

		/**
		 * <p>Returns a Stream consisting only in the raw data of this value. If the data is null or this.isEmpty(), then an empty Stream is returned.</p`>
		 * */
		default Stream<String> stream() {return asOptional().stream();}
		/**
		 * <p>Returns a Stream consisting only in this value parsed to the specified type. If it fails to parse then an empty Stream is returned.</p`>
		 * */
		default <T> Stream<T> stream(Class<T> type) {return asOptional(type).stream();}
		default <T> Stream<T> flatMap(Class<T> type) {
			if(type.isPrimitive()) throw new IllegalArgumentException("Cannot flatMap primitive type: " + type);
			return type.isArray() ? stream(type) : this.<T[]>stream(arrayTypeOf(type)).flatMap(Arrays::stream);
		}
		default <T> List<T> asList(Class<T> elementType) {return collect(elementType, Collectors.toList());}
		default <T> Set<T> asSet(Class<T> elementType) {return collect(elementType, Collectors.toSet());}
		default <T, R> R collect(Class<T> elementType, Collector<T, ?, R> collector) {
			if(elementType.isPrimitive()) throw new IllegalArgumentException("Cannot collect primitive type: " + elementType);
			return this.<T[]>stream(arrayTypeOf(elementType)).flatMap(Arrays::stream).collect(collector);
		}
	}
}