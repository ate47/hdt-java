package org.rdfhdt.hdt.hdt;

import org.apache.commons.io.file.PathUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;
import org.rdfhdt.hdt.compact.bitmap.BitmapFactory;
import org.rdfhdt.hdt.compact.bitmap.ModifiableBitmap;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.dictionary.DictionarySection;
import org.rdfhdt.hdt.dictionary.impl.MultipleBaseDictionary;
import org.rdfhdt.hdt.enums.CompressionType;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.impl.diskimport.CompressionResult;
import org.rdfhdt.hdt.iterator.utils.PipedCopyIterator;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.options.HDTOptionsKeys;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.rdf.RDFFluxStop;
import org.rdfhdt.hdt.rdf.RDFParserFactory;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.triples.impl.utils.HDTTestUtils;
import org.rdfhdt.hdt.util.LargeFakeDataSetStreamSupplier;
import org.rdfhdt.hdt.util.StopWatch;
import org.rdfhdt.hdt.util.concurrent.ExceptionThread;
import org.rdfhdt.hdt.util.io.AbstractMapMemoryTest;
import org.rdfhdt.hdt.util.io.IOUtil;
import org.rdfhdt.hdt.util.io.compress.CompressTest;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Suite.class)
@Suite.SuiteClasses({
		HDTManagerTest.DynamicDiskTest.class,
		HDTManagerTest.DynamicCatTreeTest.class,
		HDTManagerTest.FileDynamicTest.class,
		HDTManagerTest.StaticTest.class
})
public class HDTManagerTest {
	private static class HDTManagerTestBase extends AbstractMapMemoryTest implements ProgressListener {
		protected static String[][] diskDict() {
			return new String[][]{
//					{HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS, HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_MULT_HASH},
					{HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_SECTION, HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_HASH}
			};
		}

		protected static final long SIZE_VALUE = 1L << 16;
		protected static final int SEED = 67;
		@Rule
		public TemporaryFolder tempDir = new TemporaryFolder();
		protected HDTSpecification spec;

		@Before
		public void setupManager() throws IOException {
			spec = new HDTSpecification();
			spec.set(HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, tempDir.newFolder().getAbsolutePath());
			ExceptionThread.startDebug();
		}

		@After
		public void closeManager() {
			ExceptionThread.endDebug();
		}

		@Override
		public void notifyProgress(float level, String message) {
			//		System.out.println("[" + level + "] " + message);
		}

		protected void assertEqualsHDT(HDT expected, HDT actual) throws NotFoundException {
			// test dictionary
			Dictionary ed = expected.getDictionary();
			Dictionary ad = actual.getDictionary();
			assertEqualsHDT("Subjects", ed.getSubjects(), ad.getSubjects());
			assertEqualsHDT("Predicates", ed.getPredicates(), ad.getPredicates());
			if (ed instanceof MultipleBaseDictionary) {
				assertTrue(ad instanceof MultipleBaseDictionary);
				MultipleBaseDictionary edm = (MultipleBaseDictionary) ed;
				MultipleBaseDictionary adm = (MultipleBaseDictionary) ad;
				Map<? extends CharSequence, DictionarySection> keysE = edm.getAllObjects();
				Map<? extends CharSequence, DictionarySection> keysA = adm.getAllObjects();
				assertEquals(keysE.keySet(), keysA.keySet());
				keysE.forEach((key, dictE) -> {
					DictionarySection dictA = keysA.get(key);

					assertEqualsHDT(key.toString(), dictE, dictA);
				});
			} else {
				assertFalse(ad instanceof MultipleBaseDictionary);
				assertEqualsHDT("Objects", ed.getObjects(), ad.getObjects());
			}
			assertEqualsHDT("Shared", ed.getShared(), ad.getShared());
			assertEquals(ed.getType(), ad.getType());

			// test triples
			IteratorTripleString actualIt = actual.search("", "", "");
			IteratorTripleString expectedIt = expected.search("", "", "");

			while (expectedIt.hasNext()) {
				assertTrue(actualIt.hasNext());

				TripleString expectedTriple = expectedIt.next();
				TripleString actualTriple = actualIt.next();
				assertEquals(expectedIt.getLastTriplePosition(), actualIt.getLastTriplePosition());
				assertEquals(expectedTriple, actualTriple);
			}
			assertFalse(actualIt.hasNext());

			// test header
			assertEquals(actual.getHeader().getBaseURI(), expected.getHeader().getBaseURI());
			if (expected.getHeader().getNumberOfElements() != actual.getHeader().getNumberOfElements()) {
				StringBuilder bld = new StringBuilder();

				bld.append("-------- Header excepted:");
				expected.getHeader().search("", "", "").forEachRemaining(l -> bld.append(l).append('\n'));
				bld.append("-------- Header actual:");
				actual.getHeader().search("", "", "").forEachRemaining(l -> bld.append(l).append('\n'));

				fail("Size of the header doesn't match " + bld + expected.getHeader().getNumberOfElements() + "!=" + actual.getHeader().getNumberOfElements());
			}
		}

		protected void assertEqualsHDT(String section, DictionarySection excepted, DictionarySection actual) {
			Iterator<? extends CharSequence> itEx = excepted.getSortedEntries();
			Iterator<? extends CharSequence> itAc = actual.getSortedEntries();
			assertEquals("dictionary section sizes don't match", excepted.getNumberOfElements(), actual.getNumberOfElements());

			while (itEx.hasNext()) {
				assertTrue("dictionary section " + section + " is less big than excepted", itAc.hasNext());
				CharSequence expectedTriple = itEx.next();
				CharSequence actualTriple = itAc.next();
				CompressTest.assertCharSequenceEquals(section + " section strings", expectedTriple, actualTriple);
			}
			assertFalse("dictionary section " + section + " is bigger than excepted", itAc.hasNext());
		}
	}

	@RunWith(Parameterized.class)
	public static class DynamicDiskTest extends HDTManagerTestBase {

		@Parameterized.Parameters(name = "{7} - {0}")
		public static Collection<Object[]> params() {
			List<Object[]> params = new ArrayList<>();
			for (String[] dict : diskDict()) {
				params.addAll(List.of(
						new Object[]{"slow-str1", 10, 2, 4, 2, HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE, false, dict[0], dict[1], 2, "debug.disk.slow.stream=true"},
						new Object[]{"slow-str2", 10, 2, 4, 2, HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE, false, dict[0], dict[1], 2, "debug.disk.slow.stream2=true"},
						new Object[]{"slow-cfsd", 10, 2, 4, 2, HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE, false, dict[0], dict[1], 2, "debug.disk.slow.pfsd=true"},
						new Object[]{"slow-kw-d", 10, 2, 4, 2, HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE, false, dict[0], dict[1], 2, "debug.disk.slow.kway.dict=true"},
						new Object[]{"slow-kw-t", 10, 2, 4, 2, HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE, false, dict[0], dict[1], 2, "debug.disk.slow.kway.triple=true"}
				));
				for (int threads : new int[]{
						// sync
						1,
						// async, low thread count
						2,
						// async, large thread count
						8
				}) {
					List<String> modes;
					if (threads > 1) {
						// async, no need for partial
						modes = List.of(
								HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE
						);
					} else {
						modes = List.of(
								HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_VALUE_PARTIAL,
								HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE
						);
					}
					for (String mode : modes) {
						params.addAll(List.of(
								new Object[]{"base-w" + threads + "-" + mode, SIZE_VALUE * 8, 20, 50, threads, mode, false, dict[0], dict[1], SIZE_VALUE, ""},
						new Object[]{"duplicates-w" + threads + "-" + mode, SIZE_VALUE * 8, 10, 50, threads, mode, false, dict[0], dict[1], SIZE_VALUE, ""},
						new Object[]{"large-literals-w" + threads + "-" + mode, SIZE_VALUE * 2, 20, 250, threads, mode, false, dict[0], dict[1], SIZE_VALUE, ""},
						new Object[]{"quiet-w" + threads + "-" + mode, SIZE_VALUE * 8, 10, 50, threads, mode, false, dict[0], dict[1], SIZE_VALUE, ""}
						));
					}
				}
			}

			return params;
		}

		@Parameterized.Parameter
		public String name;
		@Parameterized.Parameter(1)
		public long maxSize;
		@Parameterized.Parameter(2)
		public int maxElementSplit;
		@Parameterized.Parameter(3)
		public int maxLiteralSize;
		@Parameterized.Parameter(4)
		public int threads;
		@Parameterized.Parameter(5)
		public String compressMode;
		@Parameterized.Parameter(6)
		public boolean quiet;
		@Parameterized.Parameter(7)
		public String dictionaryType;
		@Parameterized.Parameter(8)
		public String tempDictionaryType;
		@Parameterized.Parameter(9)
		public long size;
		@Parameterized.Parameter(10)
		public String addedSpecs;

		@Before
		public void setupSpecs() {
			spec.setOptions(addedSpecs);
			spec.set(HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY, String.valueOf(threads));
			spec.set(HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_KEY, compressMode);
			spec.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, dictionaryType);
			spec.set(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY, tempDictionaryType);
			spec.set(HDTOptionsKeys.LOADER_DISK_NO_COPY_ITERATOR_KEY, true);
		}

		private void generateDiskTest() throws IOException, ParserException, NotFoundException, InterruptedException {
			LargeFakeDataSetStreamSupplier supplier =
					LargeFakeDataSetStreamSupplier
							.createSupplierWithMaxSize(maxSize, SEED)
							.withMaxElementSplit(maxElementSplit)
							.withMaxLiteralSize(maxLiteralSize)
							.withSameTripleString(true);

			if (spec.getBoolean("debug.disk.slow.stream")) {
				supplier.withSlowStream(25);
			}

			// create DISK HDT
			LargeFakeDataSetStreamSupplier.ThreadedStream genActual = supplier.createNTInputStream(CompressionType.GZIP);
			HDT actual = null;
			try {
				actual = HDTManager.generateHDTDisk(
						genActual.getStream(),
						HDTTestUtils.BASE_URI,
						RDFNotation.NTRIPLES,
						CompressionType.GZIP,
						spec,
						quiet ? null : this
				);
			} finally {
				if (actual == null) {
					genActual.getThread().interrupt();
				}
			}
			genActual.getThread().joinAndCrashIfRequired();

			supplier.reset();

			LargeFakeDataSetStreamSupplier.ThreadedStream genExpected = supplier.createNTInputStream(CompressionType.GZIP);
			// create MEMORY HDT
			HDT expected = null;
			try {
				expected = HDTManager.generateHDT(
						genExpected.getStream(),
						HDTTestUtils.BASE_URI,
						RDFNotation.NTRIPLES,
						CompressionType.GZIP,
						spec,
						null
				);
			} finally {
				if (expected == null) {
					genExpected.getThread().interrupt();
				}
			}
			genExpected.getThread().joinAndCrashIfRequired();

			// happy compiler, should throw before
			assertNotNull(expected);
			assertNotNull(actual);
			try {
				assertEqualsHDT(expected, actual);
			} catch (Throwable t) {
				HDTTestUtils.printCoDictionary(expected, actual);
				throw t;
			} finally {
				IOUtil.closeAll(expected, actual);
			}
		}

		@Test
		public void generateSaveLoadMapTest() throws IOException, ParserException, NotFoundException {
			LargeFakeDataSetStreamSupplier supplier =
					LargeFakeDataSetStreamSupplier
							.createSupplierWithMaxSize(maxSize, SEED)
							.withMaxElementSplit(maxElementSplit)
							.withMaxLiteralSize(maxLiteralSize);

			// create MEMORY HDT

			try (HDT expected = HDTManager.generateHDT(
					supplier.createTripleStringStream(),
					HDTTestUtils.BASE_URI,
					spec,
					quiet ? null : this
			)) {
				String tmp = tempDir.newFile().getAbsolutePath();
				expected.saveToHDT(tmp, null);

				try (HDT mapExcepted = HDTManager.mapHDT(tmp, quiet ? null : this)) {
					assertEqualsHDT(expected, mapExcepted);
				}

				try (HDT loadExcepted = HDTManager.loadHDT(tmp, quiet ? null : this)) {
					assertEqualsHDT(expected, loadExcepted);
				}
			}

		}

		@Test
		public void generateDiskMemTest() throws IOException, ParserException, NotFoundException, InterruptedException {
			spec.set(HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, size);
			spec.set("debug.disk.build", true);
			generateDiskTest();
		}

		@Test
		public void generateDiskMapTest() throws IOException, ParserException, NotFoundException, InterruptedException {
			spec.set(HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, size);
			spec.set("debug.disk.build", true);
			File mapHDT = tempDir.newFile("mapHDTTest.hdt");
			spec.set(HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, mapHDT.getAbsolutePath());
			generateDiskTest();
			Files.deleteIfExists(mapHDT.toPath());
		}

		@Test
		public void catTreeTest() throws IOException, ParserException, NotFoundException, InterruptedException {
			LargeFakeDataSetStreamSupplier supplier =
					LargeFakeDataSetStreamSupplier
							.createSupplierWithMaxSize(maxSize, SEED)
							.withMaxElementSplit(maxElementSplit)
							.withMaxLiteralSize(maxLiteralSize);

			// create DISK HDT
			LargeFakeDataSetStreamSupplier.ThreadedStream genActual = supplier.createNTInputStream(CompressionType.NONE);
			HDT actual = null;
			try {
				actual = HDTManager.catTree(
						RDFFluxStop.sizeLimit(size),
						HDTSupplier.memory(),
						genActual.getStream(),
						HDTTestUtils.BASE_URI,
						RDFNotation.NTRIPLES,
						spec,
						quiet ? null : this
				);
			} finally {
				if (actual == null) {
					genActual.getThread().interrupt();
				}
			}
			genActual.getThread().joinAndCrashIfRequired();

			supplier.reset();

			Iterator<TripleString> genExpected = supplier.createTripleStringStream();
			// create MEMORY HDT
			HDT expected = HDTManager.generateHDT(
					genExpected,
					HDTTestUtils.BASE_URI,
					spec,
					null
			);

			// happy compiler, should throw before
			assertNotNull(expected);
			assertNotNull(actual);
			try {
				assertEqualsHDT(expected, actual); // -1 for the original size ignored by hdtcat
			} finally {
				IOUtil.closeAll(expected, actual);
			}
		}

		@Test
		public void catTreeDiskTest() throws IOException, ParserException, NotFoundException, InterruptedException {
			LargeFakeDataSetStreamSupplier supplier =
					LargeFakeDataSetStreamSupplier
							.createSupplierWithMaxSize(maxSize, SEED)
							.withMaxElementSplit(maxElementSplit)
							.withMaxLiteralSize(maxLiteralSize);

			spec.set("debug.disk.build", true);

			// create DISK HDT
			LargeFakeDataSetStreamSupplier.ThreadedStream genActual = supplier.createNTInputStream(CompressionType.NONE);
			HDT actual = null;
			try {
				actual = HDTManager.catTree(
						RDFFluxStop.sizeLimit(size),
						HDTSupplier.disk(),
						genActual.getStream(),
						HDTTestUtils.BASE_URI,
						RDFNotation.NTRIPLES,
						spec,
						quiet ? null : this
				);
			} finally {
				if (actual == null) {
					genActual.getThread().interrupt();
				}
			}
			genActual.getThread().joinAndCrashIfRequired();

			supplier.reset();

			Iterator<TripleString> genExpected = supplier.createTripleStringStream();
			// create MEMORY HDT
			HDT expected = HDTManager.generateHDT(
					genExpected,
					HDTTestUtils.BASE_URI,
					spec,
					null
			);

			// happy compiler, should throw before
			assertNotNull(expected);
			assertNotNull(actual);
			try {
				assertEqualsHDT(expected, actual); // -1 for the original size ignored by hdtcat
			} finally {
				IOUtil.closeAll(expected, actual);
			}
		}
	}

	@RunWith(Parameterized.class)
	public static class DynamicCatTreeTest extends HDTManagerTestBase {

		@Parameterized.Parameters(name = "{5} - {0}")
		public static Collection<Object[]> params() {
			List<Object[]> params = new ArrayList<>();
			for (String[] dict : diskDict()) {
				params.add(new Object[]{"base", SIZE_VALUE * 16, 20, 50, false, dict[0], dict[1], SIZE_VALUE});
				params.add(new Object[]{"duplicates", SIZE_VALUE * 16, 10, 50, false, dict[0], dict[1], SIZE_VALUE});
				params.add(new Object[]{"large-literals", SIZE_VALUE * 4, 20, 250, false, dict[0], dict[1], SIZE_VALUE});
				params.add(new Object[]{"quiet", SIZE_VALUE * 16, 10, 50, false, dict[0], dict[1], SIZE_VALUE});
			}
			return params;
		}

		@Parameterized.Parameter
		public String name;
		@Parameterized.Parameter(1)
		public long maxSize;
		@Parameterized.Parameter(2)
		public int maxElementSplit;
		@Parameterized.Parameter(3)
		public int maxLiteralSize;
		@Parameterized.Parameter(4)
		public boolean quiet;
		@Parameterized.Parameter(5)
		public String dictionaryType;
		@Parameterized.Parameter(6)
		public String tempDictionaryType;
		@Parameterized.Parameter(7)
		public long size;

		@Before
		public void setupSpecs() {
			spec.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, dictionaryType);
			spec.set(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY, tempDictionaryType);
		}

		@Test
		public void catTreeTest() throws IOException, ParserException, NotFoundException, InterruptedException {
			LargeFakeDataSetStreamSupplier supplier =
					LargeFakeDataSetStreamSupplier
							.createSupplierWithMaxSize(maxSize, SEED)
							.withMaxElementSplit(maxElementSplit)
							.withMaxLiteralSize(maxLiteralSize);

			// create DISK HDT
			LargeFakeDataSetStreamSupplier.ThreadedStream genActual = supplier.createNTInputStream(CompressionType.NONE);
			HDT actual = null;
			HDT expected = null;
			try {
				try {
					actual = HDTManager.catTree(
							RDFFluxStop.sizeLimit(size),
							HDTSupplier.memory(),
							genActual.getStream(),
							HDTTestUtils.BASE_URI,
							RDFNotation.NTRIPLES,
							spec,
							quiet ? null : this
					);
				} finally {
					if (actual == null) {
						genActual.getThread().interrupt();
					}
				}
				genActual.getThread().joinAndCrashIfRequired();

				supplier.reset();

				Iterator<TripleString> genExpected = supplier.createTripleStringStream();
				// create MEMORY HDT
				expected = HDTManager.generateHDT(
						genExpected,
						HDTTestUtils.BASE_URI,
						spec,
						null
				);

				// happy compiler, should throw before
				assertNotNull(expected);
				assertNotNull(actual);
				assertEqualsHDT(expected, actual); // -1 for the original size ignored by hdtcat
			} finally {
				IOUtil.closeAll(expected, actual);
			}
		}

		@Test
		public void catTreeDiskTest() throws IOException, ParserException, NotFoundException, InterruptedException {
			LargeFakeDataSetStreamSupplier supplier =
					LargeFakeDataSetStreamSupplier
							.createSupplierWithMaxSize(maxSize, SEED)
							.withMaxElementSplit(maxElementSplit)
							.withMaxLiteralSize(maxLiteralSize);

			// create DISK HDT
			LargeFakeDataSetStreamSupplier.ThreadedStream genActual = supplier.createNTInputStream(CompressionType.NONE);
			HDT actual = null;
			try {
				actual = HDTManager.catTree(
						RDFFluxStop.sizeLimit(size),
						HDTSupplier.disk(),
						genActual.getStream(),
						HDTTestUtils.BASE_URI,
						RDFNotation.NTRIPLES,
						spec,
						quiet ? null : this
				);
			} finally {
				if (actual == null) {
					genActual.getThread().interrupt();
				}
			}
			genActual.getThread().joinAndCrashIfRequired();

			supplier.reset();

			Iterator<TripleString> genExpected = supplier.createTripleStringStream();
			// create MEMORY HDT
			HDT expected = HDTManager.generateHDT(
					genExpected,
					HDTTestUtils.BASE_URI,
					spec,
					null
			);

			// happy compiler, should throw before
			assertNotNull(expected);
			assertNotNull(actual);
			try {
				assertEqualsHDT(expected, actual); // -1 for the original size ignored by hdtcat
			} finally {
				IOUtil.closeAll(expected, actual);
			}
		}
	}

	@RunWith(Parameterized.class)
	public static class FileDynamicTest extends HDTManagerTestBase {
		@Parameterized.Parameters(name = "{0}")
		public static Collection<Object[]> params() {
			return List.<Object[]>of(
					new Object[]{"hdtGenDisk/unicode_disk_encode.nt", true, SIZE_VALUE}
			);
		}

		@Parameterized.Parameter
		public String file;
		@Parameterized.Parameter(1)
		public boolean quiet;
		@Parameterized.Parameter(2)
		public long size;


		private void generateDiskTest() throws IOException, ParserException, NotFoundException {
			String ntFile = Objects.requireNonNull(getClass().getClassLoader().getResource(file), "Can't find " + file).getFile();
			// create DISK HDT
			HDT actual = HDTManager.generateHDTDisk(
					ntFile,
					HDTTestUtils.BASE_URI,
					RDFNotation.NTRIPLES,
					spec,
					quiet ? null : this
			);

			// create MEMORY HDT
			HDT expected = HDTManager.generateHDT(
					ntFile,
					HDTTestUtils.BASE_URI,
					RDFNotation.NTRIPLES,
					spec,
					null
			);

			try {
				assertEqualsHDT(expected, actual);
			} finally {
				IOUtil.closeAll(expected, actual);
			}
		}

		@Test
		public void generateDiskCompleteTest() throws IOException, ParserException, NotFoundException {
			spec.set(HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_KEY, CompressionResult.COMPRESSION_MODE_COMPLETE);
			spec.set(HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, size);
			spec.set("debug.disk.build", true);
			generateDiskTest();
		}

		@Test
		public void generateDiskPartialTest() throws IOException, ParserException, NotFoundException {
			spec.set(HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_KEY, CompressionResult.COMPRESSION_MODE_PARTIAL);
			spec.set(HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, size);
			spec.set("debug.disk.build", true);
			generateDiskTest();
		}

		@Test
		public void generateDiskCompleteMapTest() throws IOException, ParserException, NotFoundException {
			spec.set(HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_KEY, CompressionResult.COMPRESSION_MODE_COMPLETE);
			spec.set(HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, size);
			File mapHDT = tempDir.newFile("mapHDTTest.hdt");
			spec.set(HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, mapHDT.getAbsolutePath());
			spec.set("debug.disk.build", true);
			generateDiskTest();
			Files.deleteIfExists(mapHDT.toPath());
		}

		@Test
		public void generateDiskPartialMapTest() throws IOException, ParserException, NotFoundException {
			spec.set(HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_KEY, CompressionResult.COMPRESSION_MODE_PARTIAL);
			spec.set(HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY, size);
			File mapHDT = tempDir.newFile("mapHDTTest.hdt");
			spec.set(HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, mapHDT.getAbsolutePath());
			generateDiskTest();
			Files.deleteIfExists(mapHDT.toPath());
		}

		@Test
		public void generateTest() throws IOException, ParserException, NotFoundException {
			String ntFile = Objects.requireNonNull(getClass().getClassLoader().getResource(file), "Can't find " + file).getFile();
			// create DISK HDT
			try (InputStream in = IOUtil.getFileInputStream(ntFile)) {
				try (PipedCopyIterator<TripleString> it = RDFParserFactory.readAsIterator(
						RDFParserFactory.getParserCallback(RDFNotation.NTRIPLES, true),
						in, HDTTestUtils.BASE_URI, true, RDFNotation.NTRIPLES
				)) {
					try (HDT expected = HDTManager.generateHDT(
							it,
							HDTTestUtils.BASE_URI,
							spec,
							quiet ? null : this
					)) {
						String testCopy = tempDir.newFile().getAbsolutePath();
						expected.saveToHDT(testCopy, null);

						// create MEMORY HDT
						try (HDT actual = HDTManager.loadHDT(testCopy)) {
							assertEqualsHDT(expected, actual);
						}
					}
				}
			}
		}
	}

	public static class StaticTest extends HDTManagerTestBase {
		@Test
		public void multiSectionTest() throws ParserException, IOException, NotFoundException {
			Path root = tempDir.getRoot().toPath();
			Path hdtFile = root.resolve("testhdt.hdt");
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxTriples(10_000, 32)
					.withMaxLiteralSize(30)
					.withUnicode(true);

			// set MultiSectionDictionary type
			spec.set(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY, HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_MULT_HASH);
			spec.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS);

			try (HDT hdt = HDTManager.generateHDT(
					supplier.createTripleStringStream(),
					HDTTestUtils.BASE_URI, spec,
					null
			)) {
				assertTrue(hdt.getDictionary() instanceof MultipleBaseDictionary);
				String testHdt = hdtFile.toString();
				hdt.saveToHDT(
						testHdt,
						null
				);

				// test mapping
				try (HDT hdt2 = HDTManager.mapHDT(testHdt)) {
					assertTrue(hdt2.getDictionary() instanceof MultipleBaseDictionary);
					assertEqualsHDT(hdt, hdt2);
				}
				// test loading
				try (HDT hdt2 = HDTManager.loadHDT(testHdt)) {
					assertTrue(hdt2.getDictionary() instanceof MultipleBaseDictionary);
					assertEqualsHDT(hdt, hdt2);
				}
			} finally {
				Files.deleteIfExists(hdtFile);
			}
			Path fakeNt = root.resolve("fake.nt");
			try {
				supplier.createNTFile(fakeNt);
				try (HDT hdt = HDTManager.generateHDT(
						fakeNt.toString(),
						HDTTestUtils.BASE_URI,
						RDFNotation.NTRIPLES,
						spec,
						null
				)) {
					String testHdt = hdtFile.toString();

					hdt.saveToHDT(testHdt, null);

					// test mapping
					try (HDT hdt2 = HDTManager.mapHDT(testHdt)) {
						assertTrue(hdt2.getDictionary() instanceof MultipleBaseDictionary);
						assertEqualsHDT(hdt, hdt2);
					}
					// test loading
					try (HDT hdt2 = HDTManager.loadHDT(testHdt)) {
						assertTrue(hdt2.getDictionary() instanceof MultipleBaseDictionary);
						assertEqualsHDT(hdt, hdt2);
					}
				}
			} finally {
				try {
					Files.deleteIfExists(fakeNt);
				} finally {
					Files.deleteIfExists(hdtFile);
				}
			}
		}

		@Test
		public void diffMultiSectTest() throws ParserException, IOException, NotFoundException {
			Path root = tempDir.getRoot().toPath();
			Path hdtFile = root.resolve("testhdt.hdt");
			Path diffLocation = root.resolve("diff");
			Files.createDirectories(diffLocation);
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxTriples(10_000, 32)
					.withMaxLiteralSize(30)
					.withUnicode(true);

			// set MultiSectionDictionary type
			spec.set(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY, "multHash");
			spec.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, "dictionaryMultiObj");

			try (HDT hdt = HDTManager.generateHDT(
					supplier.createTripleStringStream(),
					HDTTestUtils.BASE_URI, spec,
					null
			)) {
				assertTrue(hdt.getDictionary() instanceof MultipleBaseDictionary);
				String testHdt = hdtFile.toString();
				hdt.saveToHDT(
						testHdt,
						null
				);

				ModifiableBitmap bitmap;

				// test mapping
				long n;
				try (HDT hdt2 = HDTManager.mapHDT(testHdt)) {
					bitmap = BitmapFactory.createRWBitmap(hdt2.getTriples().getNumberOfElements());
					assertTrue(hdt2.getDictionary() instanceof MultipleBaseDictionary);
					assertEqualsHDT(hdt, hdt2);

					n = hdt2.getTriples().getNumberOfElements();
				}

				Random rnd = new Random(SEED);
				for (long i = 0; i < n / 24; i++) {
					bitmap.set(Math.abs(rnd.nextLong()) % n, true);
				}

				try (HDT hdtDiff = HDTManager.diffHDTBit(
						diffLocation.toAbsolutePath().toString(),
						testHdt,
						bitmap,
						spec,
						null
				)) {
					assertEquals(hdt.getTriples().getNumberOfElements() - bitmap.countOnes(), hdtDiff.getTriples().getNumberOfElements());
				}
			} finally {
				try {
					Files.deleteIfExists(hdtFile);
				} finally {
					PathUtils.deleteDirectory(diffLocation);
				}
			}
		}

	}

	@Ignore("handTests")
	public static class HandTest extends HDTManagerTestBase {
		@Test
		public void qzdqzdTest() throws ParserException, IOException {
			String path = "/Users/ate/workspace/qacompany/hdt-java-ate47/hdt-java-package/target/hdt-java-package-3.0.5-distribution/hdt-java-package-3.0.5/bin/shit.nt.gz";

			HDTSpecification spec = new HDTSpecification();
			spec.load("/Users/ate/workspace/qacompany/hdt-java-ate47/hdt-java-package/target/hdt-java-package-3.0.5-distribution/hdt-java-package-3.0.5/bin/option.hdtspec");


			try (HDT hdt = HDTManager.generateHDTDisk(path, "http://ex.ogr/#", spec,
					(level, message) -> System.out.println("[" + level + "] " + message)
			)) {
				System.out.println(hdt.getTriples().getNumberOfElements());
			}

		}
		@Test
		public void bigDiskTest() throws ParserException, IOException {
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxSize(100_000_000L, 94);

			Path output = tempDir.newFolder().toPath();

			HDTOptions spec = new HDTSpecification();
			spec.set(HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, output.resolve("future.hdt").toAbsolutePath().toString());
			spec.set(HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, output.resolve("gen_dir").toAbsolutePath().toString());
			spec.set(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, "true");
			spec.set(HDTOptionsKeys.PROFILER_KEY, "true");
			StopWatch watch = new StopWatch();
			watch.reset();
			try (HDT hdt = HDTManager.generateHDTDisk(supplier.createTripleStringStream(), "http://ex.ogr/#", spec,
					(level, message) -> System.out.println("[" + level + "] " + message)
			)) {
				System.out.println(watch.stopAndShow());
				System.out.println(hdt.getTriples().getNumberOfElements());
			}
		}

		@Test
		public void zqdz() {
			System.out.println("\255".getBytes(StandardCharsets.UTF_8)[0] & 0xFF);
		}

		@Test
		public void bigCatTreeDiskTest() throws ParserException, IOException {
			HDTOptions spec = new HDTSpecification();
			StopWatch watch = new StopWatch();
			spec.set(HDTOptionsKeys.LOADER_CATTREE_LOCATION_KEY, "C:\\WIKI\\CATTREE\\WORKING");
			spec.set(HDTOptionsKeys.LOADER_CATTREE_FUTURE_HDT_LOCATION_KEY, "C:\\WIKI\\CATTREE\\future.hdt");
			spec.set(HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, "C:\\WIKI\\CATTREE\\WORKING_HDTDISK");
			spec.set(HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY, "12");
			spec.set(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, "true");
			spec.set(HDTOptionsKeys.PROFILER_KEY, "true");
			watch.reset();
			try (HDT hdt = HDTManager.catTree(
					RDFFluxStop.sizeLimit(100_000_000_000L) // 300GB free
							.and(RDFFluxStop.countLimit(700_000_000L) // ~9GB maps
							), HDTSupplier.disk(),
					"M:\\WIKI\\latest-all.nt.bz2", HDTTestUtils.BASE_URI, RDFNotation.NTRIPLES, spec,
					(level, message) -> System.out.println("[" + level + "] " + message)
			)) {
				System.out.println(watch.stopAndShow());
				System.out.println(hdt.getTriples().getNumberOfElements());
			}
		}

		@Test
		public void bigGenCatTreeDiskTest() throws ParserException, IOException {
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxSize(10_000_000_000L, 94);
			HDTOptions spec = new HDTSpecification();
			StopWatch watch = new StopWatch();
			spec.set(HDTOptionsKeys.LOADER_CATTREE_LOCATION_KEY, "C:\\WIKI\\CATTREE\\WORKING");
			spec.set(HDTOptionsKeys.LOADER_CATTREE_FUTURE_HDT_LOCATION_KEY, "C:\\WIKI\\CATTREE\\future.hdt");
			spec.set(HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, "C:\\WIKI\\CATTREE\\WORKING_HDTDISK");
			spec.set(HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY, "12");
			spec.set(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, "true");
			spec.set(HDTOptionsKeys.PROFILER_KEY, "true");
			watch.reset();
			try (HDT hdt = HDTManager.catTree(
					RDFFluxStop.sizeLimit(100_000_000_000L) // 300GB free
							.and(RDFFluxStop.countLimit(700_000_000L) // ~9GB maps
							), HDTSupplier.disk(),
					supplier.createTripleStringStream(), HDTTestUtils.BASE_URI, spec,
					(level, message) -> System.out.println("[" + level + "] " + message)
			)) {
				System.out.println(watch.stopAndShow());
				System.out.println(hdt.getTriples().getNumberOfElements());
			}
		}
	}

}
