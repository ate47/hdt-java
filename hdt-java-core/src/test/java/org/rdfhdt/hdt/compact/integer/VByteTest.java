package org.rdfhdt.hdt.compact.integer;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;

import org.junit.Test;

public class VByteTest {

	@Test 
	public void testMaxSize() throws IOException {

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		VByte.encode(out, Long.MAX_VALUE);
//		System.out.println("Size of "+Long.MAX_VALUE+" => "+out.size());

		long val = VByte.decode(new ByteArrayInputStream(out.toByteArray()));
//		System.out.println("Value back: "+val);
		assertEquals(Long.MAX_VALUE, val);

		out = new ByteArrayOutputStream();
		VByte.encode(out, Integer.MAX_VALUE);
//		System.out.println("Size of "+Integer.MAX_VALUE+" => "+out.size());
		val = VByte.decode(new ByteArrayInputStream(out.toByteArray()));
		assertEquals(Integer.MAX_VALUE, val);
//		System.out.println("Value back: "+val);

	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testMoreCharactersThanNeeded() throws IOException {
		byte [] arr = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		long val = VByte.decode(new ByteArrayInputStream(arr));
		System.out.println("VAL: "+val);
		fail("Exception not thrown");
	}
	
	@Test(expected=EOFException.class)
	public void testEOF() throws IOException {
		byte [] arr = { 0, 0 };
		long val = VByte.decode(new ByteArrayInputStream(arr));
		System.out.println("VAL: "+val);
		fail("Exception not thrown");
	}

	@Test
	public void paddedTest() throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		long[] values = {
				42,
				256,
				4096,
				12345,
				1234567
		};

		for (long v : values) {
			VByte.encodePadded(out, v);
		}

		byte[] buf = out.toByteArray();
		assertEquals("variable lenght!", 0, buf.length % values.length);
		assertEquals("not the right amount of bits!", Long.BYTES + 1, buf.length / values.length);
		ByteArrayInputStream in = new ByteArrayInputStream(buf);

		for (long v : values) {
			assertEquals(v, VByte.decode(in));
		}
	}
}
