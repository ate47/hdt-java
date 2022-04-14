package org.rdfhdt.hdt.iterator.utils;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * alternative iterator with exception throwing
 * @param <T> the iterator type
 * @param <E> the allowed exception
 * @author Antoine Willerval
 */
public interface ExceptionIterator<T, E extends Exception> {
	/**
	 * create an exception iterator from a basic iterator
	 * @param it the iterator the wrap
	 * @param <T> the iterator type
	 * @param <E> the exception to allow
	 * @return exception iterator
	 */
	static <T, E extends Exception> ExceptionIterator<T, E> of(final Iterator<T> it) {
		return new ExceptionIterator<>() {
			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public T next() {
				return it.next();
			}

			@Override
			public void remove() {
				it.remove();
			}

			@Override
			public void forEachRemaining(Consumer<? super T> action) {
				it.forEachRemaining(action);
			}
		};
	}

	/**
	 * @return if the iterator has a next element
	 * @throws E exception triggered by the implementation
	 */
	boolean hasNext() throws E;

	/**
	 * @return the next iterator element
	 * @throws E exception triggered by the implementation
	 */
	T next() throws E;

	/**
	 * remove the last element returned by the iterator
	 * @throws E exception triggered by the implementation
	 */
	default void remove() throws E {
		throw new UnsupportedOperationException("remove");
	}

	/**
	 * loop over all the elements
	 * @param action the action to handle the element
	 * @throws E exception triggered by the implementation
	 */
	default void forEachRemaining(Consumer<? super T> action) throws E {
		Objects.requireNonNull(action);
		while (hasNext())
			action.accept(next());
	}
}
