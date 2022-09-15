package org.rdfhdt.hdt.iterator.utils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

public class MergeExceptionIterator<T, E extends Exception> implements ExceptionIterator<T, E> {

	/**
	 * Create a tree of merge iterators from an array of element
	 * @param itFunction a function to create an iterator from an element
	 * @param comp comparator for the merge iterator
	 * @param array the elements
	 * @param length the number of elements
	 * @param <I> input of the element
	 * @param <T> type of the element in the iterator
	 * @param <E> exception returned by the iterator
	 * @return the iterator
	 */
	public static <I, T, E extends Exception> ExceptionIterator<T, E> buildOfTree(
			Function<I, ExceptionIterator<T, E>> itFunction, Comparator<T> comp, I[] array, int length) {
		return buildOfTree(itFunction, comp, array, 0, length);
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 * @param itFunction a function to create an iterator from an element
	 * @param comp comparator for the merge iterator
	 * @param array the elements
	 * @param start the start of the array (inclusive)
	 * @param end the end of the array (exclusive)
	 * @param <T> type of the element
	 * @param <E> exception returned by the iterator
	 * @return the iterator
	 */
	public static <I, T, E extends Exception> ExceptionIterator<T, E> buildOfTree(
			Function<I, ExceptionIterator<T, E>> itFunction, Comparator<T> comp, I[] array, int start, int end) {
		return buildOfTree(itFunction, comp, Arrays.asList(array), start, end);
	}

	/**
	 * Create a tree of merge iterators from an array of element
	 * @param itFunction a function to create an iterator from an element
	 * @param comp comparator for the merge iterator
	 * @param array the elements
	 * @param start the start of the array (inclusive)
	 * @param end the end of the array (exclusive)
	 * @param <T> type of the element
	 * @param <E> exception returned by the iterator
	 * @return the iterator
	 */
	public static <I, T, E extends Exception> ExceptionIterator<T, E> buildOfTree(
			Function<I, ExceptionIterator<T, E>> itFunction, Comparator<T> comp, List<I> array, int start, int end) {
		int length = end - start;
		if (length <= 0) {
			return ExceptionIterator.empty();
		}
		if (length == 1) {
			return itFunction.apply(array.get(start));
		}
		int mid = (start + end) / 2;
		return new MergeExceptionIterator<>(
				buildOfTree(itFunction, comp, array, start, mid),
				buildOfTree(itFunction, comp, array, mid, end),
				comp
		);
	}

	private final ExceptionIterator<T, E> in1, in2;
	private final Comparator<T> comp;
	private T next;
	private T prevE1;
	private T prevE2;

	public MergeExceptionIterator(ExceptionIterator<T, E> in1, ExceptionIterator<T, E> in2, Comparator<T> comp) {
		this.in1 = in1;
		this.in2 = in2;
		this.comp = comp;
	}

	@Override
	public boolean hasNext() throws E {
		if (next != null) {
			return true;
		}

		// read next element 1 if required
		if (prevE1 == null && in1.hasNext()) {
			prevE1 = in1.next();
		}
		// read next element 2 if required
		if (prevE2 == null && in2.hasNext()) {
			prevE2 = in2.next();
		}

		if (prevE1 != null && prevE2 != null) {
			// we have an element from both stream, compare them
			if (comp.compare(prevE1, prevE2) < 0) {
				// element 1 lower, return it
				next = prevE1;
				prevE1 = null;
			} else {
				// element 2 lower, return it
				next = prevE2;
				prevE2 = null;
			}
			return true;
		}
		// we have at most one element
		if (prevE1 != null) {
			// return element 1
			next = prevE1;
			prevE1 = null;
			return true;
		}
		if (prevE2 != null) {
			// return element 2
			next = prevE2;
			prevE2 = null;
			return true;
		}
		// nothing else
		return false;
	}

	@Override
	public T next() throws E {
		if (!hasNext()) {
			return null;
		}
		T next = this.next;
		this.next = null;
		return next;
	}
}
