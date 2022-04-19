package org.rdfhdt.hdt.hdt;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.dictionary.DictionarySection;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.impl.diskimport.CompressionResult;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.triples.impl.utils.HDTTestUtils;
import org.rdfhdt.hdt.util.LargeFakeDataSetStreamSupplier;
import org.rdfhdt.hdt.util.StopWatch;
import org.rdfhdt.hdt.util.io.compress.CompressTest;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public class HDTManagerTest {
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();
	private HDTSpecification spec;

	@Before
	public void setup() throws IOException {
		spec = new HDTSpecification();
		spec.set("loader.disk.location", tempDir.newFolder().getAbsolutePath());
		spec.setInt("loader.disk.chunkSize", 1024L);
	}

	private void generateDiskTest() throws IOException, ParserException, NotFoundException {
		LargeFakeDataSetStreamSupplier supplier =
				LargeFakeDataSetStreamSupplier
						.createSupplierWithMaxSize(1024L * 8, 34)
						.withMaxElementSplit(20);

		// create DISK HDT
		HDT actual = HDTManager.generateHDTDisk(
				supplier.createTripleStringStream(),
				HDTTestUtils.BASE_URI,
				spec,
				(level, message) -> System.out.println("[" + level + "] " + message)
		);

		supplier.reset();

		// create MEMORY HDT
		HDT expected = HDTManager.generateHDT(
				supplier.createTripleStringStream(),
				HDTTestUtils.BASE_URI,
				spec,
				null
		);

		try {
			assertEquals(expected, actual);
		} finally {
			try {
				expected.close();
			} finally {
				actual.close();
			}
		}
	}

	private void assertEquals(HDT expected, HDT actual) throws NotFoundException {

		// test dictionary
		Dictionary ed = expected.getDictionary();
		Dictionary ad = actual.getDictionary();
		assertEquals("Subjects", ed.getSubjects(), ad.getSubjects());
		assertEquals("Predicates", ed.getPredicates(), ad.getPredicates());
		assertEquals("Objects", ed.getObjects(), ad.getObjects());
		assertEquals("Shared", ed.getShared(), ad.getShared());
		Assert.assertEquals(ed.getType(), ad.getType());

		// test triples
		IteratorTripleString actualIt = actual.search("", "", "");
		IteratorTripleString expectedIt = expected.search("", "", "");

		while (expectedIt.hasNext()) {
			Assert.assertTrue(actualIt.hasNext());

			TripleString expectedTriple = expectedIt.next();
			TripleString actualTriple = actualIt.next();
			Assert.assertEquals(expectedIt.getLastTriplePosition(), actualIt.getLastTriplePosition());
			Assert.assertEquals(expectedTriple, actualTriple);
		}
		Assert.assertFalse(actualIt.hasNext());

		// test header
		Assert.assertEquals(actual.getHeader().getBaseURI(), expected.getHeader().getBaseURI());
		Assert.assertEquals(actual.getHeader().getNumberOfElements(), expected.getHeader().getNumberOfElements());
	}

	private void assertEquals(String section, DictionarySection excepted, DictionarySection actual) {
		Assert.assertEquals(excepted.getNumberOfElements(), actual.getNumberOfElements());
		Iterator<? extends CharSequence> itEx = excepted.getSortedEntries();
		Iterator<? extends CharSequence> itAc = actual.getSortedEntries();

		while (itEx.hasNext()) {
			Assert.assertTrue(itAc.hasNext());
			CharSequence expectedTriple = itEx.next();
			CharSequence actualTriple = itAc.next();
			CompressTest.assertCharSequenceEquals(section + " section strings", expectedTriple, actualTriple);
		}
		Assert.assertFalse(itAc.hasNext());
	}

	@Test
	public void generateDiskCompleteTest() throws IOException, ParserException, NotFoundException {
		spec.set("loader.disk.compressMode", CompressionResult.COMPRESSION_MODE_COMPLETE);
		generateDiskTest();
	}

	@Test
	public void generateDiskPartialTest() throws IOException, ParserException, NotFoundException {
		spec.set("loader.disk.compressMode", CompressionResult.COMPRESSION_MODE_PARTIAL);
		generateDiskTest();
	}
}
