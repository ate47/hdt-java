package org.rdfhdt.hdt.iterator.utils;

import org.junit.Assert;
import org.junit.Test;
import org.rdfhdt.hdt.util.concurrent.ExceptionThread;

public class SinglePipedIteratorTest {

	@Test
	public void singlePipeIteratorReadSyncTest() throws InterruptedException {
		TestReference r = new TestReference();
		TestReference r2 = new TestReference();

		SinglePipedIterator<TestReference> pipe = new SinglePipedIterator<>();
		SinglePipedIterator<TestReference> pipe2 = new SinglePipedIterator<>();
		new ExceptionThread(() -> {
			for (int i = 0; i < 10; i++) {
				pipe.addElement(() -> {
					r.update();
					return r;
				});
				pipe2.addElement(() -> {
					r2.update();
					return r2;
				});
			}
			pipe.closePipe();
			pipe2.closePipe();
		}, "SinglePipeTest").attach(
						new ExceptionThread(() -> {
							Thread.sleep(25);
							for (int i = 1; i <= 10; i++) {
								Assert.assertTrue(pipe.hasNext());
								TestReference rr = pipe.next();
								Thread.sleep(25);
								Assert.assertEquals(i, rr.read());

								Assert.assertTrue(pipe2.hasNext());
								TestReference rr2 = pipe2.next();
								Thread.sleep(20);
								Assert.assertEquals(i, rr2.read());
							}
							Assert.assertFalse(pipe.hasNext());
						}, "Pipe1"))
				.startAll()
				.joinAndCrashIfRequired();
	}

	@Test
	public void singlePipeIteratorReadAsyncTest() throws InterruptedException {
		TestReference r = new TestReference();
		TestReference r2 = new TestReference();

		SinglePipedIterator<TestReference> pipe = new SinglePipedIterator<>();
		SinglePipedIterator<TestReference> pipe2 = new SinglePipedIterator<>();
		new ExceptionThread(() -> {
			for (int i = 0; i < 10; i++) {
				pipe.addElement(() -> {
					r.update();
					return r;
				});
				pipe2.addElement(() -> {
					r2.update();
					return r2;
				});
			}
			pipe.closePipe();
			pipe2.closePipe();
		}, "SinglePipeTest").attach(
						new ExceptionThread(() -> {
							Thread.sleep(25);
							for (int i = 1; i <= 10; i++) {
								Assert.assertTrue(pipe.hasNext());
								TestReference rr = pipe.next();
								Thread.sleep(25);
								Assert.assertEquals(i, rr.read());
							}
							Assert.assertFalse(pipe.hasNext());
						}, "Pipe1"),
						new ExceptionThread(() -> {
							Thread.sleep(15);
							for (int i = 1; i <= 10; i++) {
								Assert.assertTrue(pipe2.hasNext());
								TestReference rr = pipe2.next();
								Thread.sleep(20);
								Assert.assertEquals(i, rr.read());
							}
							Assert.assertFalse(pipe2.hasNext());
						}, "Pipe2"))
				.startAll()
				.joinAndCrashIfRequired();
	}

	static class TestReference {
		int value;

		void update() {
			value++;
		}

		int read() {
			return value;
		}
	}
}
