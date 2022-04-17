package org.rdfhdt.hdt.iterator.utils;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * alternative iterator with exception throwing
 * @param <T> the iterator type
 * @param <E> the allowed exception
 * @author Antoine Willerval
 */
public interface ExceptionIterator<T, E extends Exception> {
	@FunctionalInterface
	interface ExceptionConsumer<T, E extends Exception> {
		void consume(T element) throws E;
	}
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
		};
	}	/**
	 * create an empty iterator
	 * @param <T> the iterator type
	 * @param <E> the exception to allow
	 * @return exception iterator
	 */
	static <T, E extends Exception> ExceptionIterator<T, E> empty() {
		return of(new Iterator<>() {
			@Override
			public boolean hasNext() {
				return false;
			}

			@Override
			public T next() {
				return null;
			}
		});
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
	default void forEachRemaining(ExceptionConsumer<? super T, E> action) throws E {
		Objects.requireNonNull(action);
		while (hasNext())
			action.consume(next());
	}

	/**
	 * map this iterator with a function
	 * @param mappingFunc the mapping function
	 * @param <M> the new iterator type
	 * @return iterator
	 */
	default <M> ExceptionIterator<M, E> map(Function<T, M> mappingFunc) {
		return new ExceptionIterator<>() {
			@Override
			public boolean hasNext() throws E {
				return ExceptionIterator.this.hasNext();
			}

			@Override
			public M next() throws E {
				return mappingFunc.apply(ExceptionIterator.this.next());
			}

			@Override
			public void remove() throws E {
				ExceptionIterator.this.remove();
			}

			@Override
			public void forEachRemaining(ExceptionConsumer<? super M, E> action) throws E {
				ExceptionIterator.this.forEachRemaining(e -> action.consume(mappingFunc.apply(e)));
			}
		};
	}

	/**
	 * convert this exception iterator to a base iterator and convert the exception to RuntimeException
	 * @return iterator
	 */
	default Iterator<T> asIterator() {
		return new Iterator<>() {
			@Override
			public boolean hasNext() {
				try {
					return ExceptionIterator.this.hasNext();
				} catch (Exception e) {
					if (e instanceof RuntimeException) {
						throw (RuntimeException) e;
					}
					throw new RuntimeException(e);
				}
			}

			@Override
			public T next() {
				try {
					return ExceptionIterator.this.next();
				} catch (Exception e) {
					if (e instanceof RuntimeException) {
						throw (RuntimeException) e;
					}
					throw new RuntimeException(e);
				}
			}

			@Override
			public void forEachRemaining(Consumer<? super T> action) {
				try {
					ExceptionIterator.this.forEachRemaining(action::accept);
				} catch (Exception e) {
					if (e instanceof RuntimeException) {
						throw (RuntimeException) e;
					}
					throw new RuntimeException(e);
				}
			}

			@Override
			public void remove() {
				try {
					ExceptionIterator.this.remove();
				} catch (Exception e) {
					if (e instanceof RuntimeException) {
						throw (RuntimeException) e;
					}
					throw new RuntimeException(e);
				}
			}
		};
	}
}
