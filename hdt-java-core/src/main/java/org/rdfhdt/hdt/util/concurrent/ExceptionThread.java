package org.rdfhdt.hdt.util.concurrent;

import java.io.IOException;
import java.util.Objects;

/**
 * Thread allowing exception and returning it when joining it with {@link #joinAndCrashIfRequired()} or by using
 * {@link #getException()}, can be attached to other threads to crash the others if an exception occurs in one of
 * them with {@link #attach(ExceptionThread...)}.
 *
 * @author Antoine Willerval
 */
public class ExceptionThread extends Thread {
	/**
	 * create an exception threads of multiple runnable
	 *
	 * @param name      common name
	 * @param runnables the runnable list, can't be empty
	 * @return exception thread attached with other runnables
	 */
	public static ExceptionThread async(String name, ExceptionRunnable... runnables) {
		if (runnables.length == 0) {
			throw new IllegalArgumentException("empty runnable list");
		}

		ExceptionThread thread = new ExceptionThread(runnables[0], name + "#" + 0);

		for (int i = 1; i < runnables.length; i++) {
			thread.attach(new ExceptionThread(runnables[i], name + "#" + i));
		}

		return thread;
	}


	/**
	 * Version of {@link java.lang.Runnable} with an exception
	 */
	@FunctionalInterface
	public interface ExceptionRunnable {
		void run() throws Exception;
	}

	private Throwable exception = null;
	private final ExceptionRunnable target;
	private ExceptionThread next;
	private ExceptionThread prev;

	public ExceptionThread(ExceptionRunnable target, String name) {
		super(name);
		this.target = target;
	}

	/**
	 * attach another threads to wait with this one
	 *
	 * @param threads others
	 * @return this
	 */
	public ExceptionThread attach(ExceptionThread... threads) {
		Objects.requireNonNull(threads, "can't attach null thread");
		for (ExceptionThread thread : threads) {
			if (thread.prev != null) {
				throw new IllegalArgumentException("Thread " + thread.getName() + " already attached");
			}
			if (this.next != null) {
				this.next.attach(thread);
				continue;
			}
			this.next = thread;
			thread.prev = this;
		}
		return this;
	}

	/**
	 * start this thread and all attached thread
	 *
	 * @return this
	 */
	public ExceptionThread startAll() {
		ExceptionThread prev = this.prev;
		while (prev != null) {
			prev.start();
			prev = prev.prev;
		}
		start();
		ExceptionThread next = this.next;
		while (next != null) {
			next.start();
			next = next.next;
		}
		return this;
	}

	@Override
	public final void run() {
		try {
			target.run();
		} catch (Throwable t) {
			if (exception != null) {
				exception.addSuppressed(t);
				return; // another attached thread crashed, probably interruption exception
			}
			exception = t;
			if (this.next != null) {
				this.next.interruptForward(t);
			}
			if (this.prev != null) {
				this.prev.interruptBackward(t);
			}
		}
	}

	private void interruptBackward(Throwable t) {
		exception = t;
		if (this.prev != null) {
			this.prev.interruptBackward(t);
		}
		interrupt();
	}

	private void interruptForward(Throwable t) {
		exception = t;
		if (this.next != null) {
			this.next.interruptForward(t);
		}
		interrupt();
	}

	/**
	 * @return the exception returned by this thread, another attached thread or null if no exception occurred
	 */
	public Throwable getException() {
		return exception;
	}

	/**
	 * join this thread and create an exception if required, will convert it to a runtime exception if it can't be
	 * created
	 *
	 * @throws InterruptedException interruption while joining the thread
	 */
	public void joinAndCrashIfRequired() throws InterruptedException {
		join();
		ExceptionThread next = this.next;
		while (next != null) {
			next.join();
			next = next.next;
		}
		ExceptionThread prev = this.prev;
		while (prev != null) {
			prev.join();
			prev = prev.prev;
		}
		if (exception == null) {
			return;
		}
		throw new ExceptionThreadException(exception);
	}

	public static class ExceptionThreadException extends RuntimeException {
		public ExceptionThreadException(Throwable cause) {
			super(cause);
		}
	}

}
