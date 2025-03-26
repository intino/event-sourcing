package systems.intino.eventsourcing.event;


import io.intino.alexandria.iteratorstream.ResourceIteratorStream;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Arrays.stream;
import static java.util.stream.IntStream.range;

public class EventStream<T extends Event> extends ResourceIteratorStream<T> {

	public static <T extends Event> Stream<T> sequence(List<Supplier<Stream<T>>> streams) {
		return new EventStream<>(new SequenceIterator<>(streams));
	}

	public static <T extends Event> Stream<T> merge(Stream<Stream<T>> streams) {
		return new EventStream<>(new MergeIterator<>(streams));
	}

	public static <T extends Event> Stream<T> of(File file) throws IOException {
		return new EventStream<>(EventReader.of(file));
	}

	public EventStream(Iterator<T> iterator) {
		super(iterator);
	}

	private static Exception tryClose(Iterator<?> iterator) {
		if (iterator instanceof AutoCloseable) {
			try {
				((AutoCloseable) iterator).close();
			} catch (Exception e) {
				return e;
			}
		}
		return null;
	}

	@SuppressWarnings({"unchecked", "unused"})
	private static class MergeIterator<T extends Event> implements Iterator<T>, AutoCloseable {
		private final Iterator<T>[] inputs;
		private final Event[] current;

		public MergeIterator(Stream<Stream<T>> streams) {
			this.inputs = streams.map(Stream::iterator).toArray(Iterator[]::new);
			this.current = stream(inputs).map(this::next).toArray(Event[]::new);
		}

		@Override
		public boolean hasNext() {
			return !stream(current).allMatch(Objects::isNull);
		}

		@Override
		public T next() {
			return next(minIndex());
		}

		private T next(int index) {
			T message = (T) current[index];
			current[index] = next(inputs[index]);
			return message;
		}

		private T next(Iterator<T> input) {
			if(input.hasNext()) return input.next();
			tryClose(input);
			return null;
		}

		private int minIndex() {
			return range(0, current.length).boxed().min(this::comparingTimestamp).orElse(-1);
		}

		private int comparingTimestamp(int a, int b) {
			return tsOf(current[a]).compareTo(tsOf(current[b]));
		}

		private Instant tsOf(Event event) {
			return event != null ? event.ts() : Instant.MAX;
		}

		@Override
		public void close() throws Exception {
			Exception e = null;
			for (Iterator<T> iterator : inputs) {
				e = tryClose(iterator);
			}
			if (e != null) throw e;
		}
	}

	private static class SequenceIterator<T> implements Iterator<T> {

		private final Iterator<Supplier<Stream<T>>> sources;
		private Iterator<T> currentIterator;

		public SequenceIterator(List<Supplier<Stream<T>>> streams) {
			this.sources = streams.iterator();
			advanceToNextIterator();
		}

		@Override
		public boolean hasNext() {
			return currentIterator != null && currentIterator.hasNext();
		}

		@Override
		public T next() {
			T next = currentIterator.next();
			advanceToNextIteratorIfNecessary();
			return next;
		}

		private void advanceToNextIteratorIfNecessary() {
			if(!currentIterator.hasNext()) advanceToNextIterator();
		}

		private void advanceToNextIterator() {
			if(currentIterator != null) tryClose(currentIterator);
			currentIterator = null;
			currentIterator = sources.hasNext() ? sources.next().get().iterator() : null;
		}
	}
}
