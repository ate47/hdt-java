package org.rdfhdt.hdt.util.io;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rdfhdt.hdt.util.Profiler;
import org.rdfhdt.hdt.util.io.impl.ProfileInputStream;
import org.rdfhdt.hdt.util.io.impl.ProfileOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;

public class CloseSuppressPathTest {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void createDelTest() throws IOException {
        Path path = tempDir.getRoot().toPath();

        CloseSuppressPath test = CloseSuppressPath.of(path.resolve("test"));

        Files.writeString(test, "test");
        assertTrue(Files.exists(test));
        test.close();
        assertFalse(Files.exists(test));
    }

    @Test
    public void createDelRecTest() throws IOException {
        Path path = tempDir.getRoot().toPath();

        CloseSuppressPath test = CloseSuppressPath.of(path.resolve("test"));
        test.closeWithDeleteRecurse();
        Files.createDirectories(test);

        CloseSuppressPath test2 = test.resolve("test2");
        Files.writeString(test2, "test");
        assertTrue(Files.exists(test));
        assertTrue(Files.exists(test2));
        test.close();
        assertFalse(Files.exists(test2));
        assertFalse(Files.exists(test));
    }

    @Test
    public void pathTest() {
        Path path = tempDir.getRoot().toPath();

        assertEquals(CloseSuppressPath.of(path), path);
        // known unresolvable issue
        assertNotEquals(path, CloseSuppressPath.of(path));
    }

    @Test
    public void profilerTest() throws IOException {
        Profiler t = new Profiler("prof");
        t.setIoProfiling(true);
        t.setDisabled(false);
        t.setGlobal();
        t.pushSection("test sec");
        try {
            Path root = tempDir.getRoot().toPath();
            int size = "test".getBytes(StandardCharsets.UTF_8).length;
            try (CloseSuppressPath test = CloseSuppressPath.of(root).resolve("test.txt")) {
                Files.writeString(test, "test");
                try (InputStream stream = Files.newInputStream(test)) {
                    assertTrue(stream instanceof ProfileInputStream);
                    assertEquals(t, ((ProfileInputStream) stream).getProfiler());

                    byte[] bytes = stream.readAllBytes();
                    assertEquals(size, bytes.length);
                    assertEquals(size * Byte.SIZE, t.getBytesIOReadNano());
                }
                try (InputStream stream = Files.newInputStream(test)) {
                    assertTrue(stream instanceof ProfileInputStream);
                    assertEquals(t, ((ProfileInputStream) stream).getProfiler());

                    byte[] bytes = stream.readAllBytes();
                    assertEquals(size, bytes.length);
                    assertEquals(size * Byte.SIZE * 2, t.getBytesIOReadNano());
                }
                byte[] str2 = "foo".getBytes(StandardCharsets.UTF_8);
                try (OutputStream stream = Files.newOutputStream(test)) {
                    assertTrue(stream instanceof ProfileOutputStream);
                    assertEquals(t, ((ProfileOutputStream) stream).getProfiler());

                    stream.write(str2);
                    assertEquals((str2.length + size) * Byte.SIZE, t.getBytesIOWriteNano());
                }
            }

        } finally {
            t.popSection();
            t.stop();
            t.writeProfiling();
            t.unsetGlobal();
        }
    }
}
