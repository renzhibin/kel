package org.csits.kel.manager.filesystem;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalFileSystemManagerTest {

    private LocalFileSystemManager manager;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        manager = new LocalFileSystemManager();
    }

    @Test
    void ensureDirectory_createsMultiLevelDirectory() throws IOException {
        Path deep = tempDir.resolve("a").resolve("b").resolve("c");
        assertThat(Files.exists(deep)).isFalse();

        Path result = manager.ensureDirectory(deep);

        assertThat(result).isEqualTo(deep);
        assertThat(Files.isDirectory(deep)).isTrue();
    }

    @Test
    void ensureDirectory_returnsExistingDir() throws IOException {
        Path dir = tempDir.resolve("existing");
        Files.createDirectories(dir);
        Path result = manager.ensureDirectory(dir);
        assertThat(result).isEqualTo(dir);
        assertThat(Files.isDirectory(dir)).isTrue();
    }

    @Test
    void copyFile_copiesContentToTarget() throws IOException {
        Path source = tempDir.resolve("source.txt");
        Files.write(source, "hello".getBytes());
        Path target = tempDir.resolve("sub").resolve("target.txt");

        manager.copyFile(source, target);

        assertThat(Files.exists(target)).isTrue();
        assertThat(Files.readAllBytes(target)).isEqualTo("hello".getBytes());
        assertThat(Files.readAllBytes(source)).isEqualTo("hello".getBytes());
    }

    @Test
    void moveFile_movesFileToTarget() throws IOException {
        Path source = tempDir.resolve("moveMe.txt");
        Files.write(source, "moved".getBytes());
        Path target = tempDir.resolve("sub").resolve("moved.txt");

        manager.moveFile(source, target);

        assertThat(Files.exists(source)).isFalse();
        assertThat(Files.exists(target)).isTrue();
        assertThat(Files.readAllBytes(target)).isEqualTo("moved".getBytes());
    }

    @Test
    void scanFiles_matchesGlobPattern() throws IOException {
        Path root = tempDir.resolve("scanRoot");
        Files.createDirectories(root);
        Path a = root.resolve("a.txt");
        Path b = root.resolve("b.dat");
        Path c = root.resolve("c.txt");
        Path sub = root.resolve("sub");
        Files.createDirectories(sub);
        Path subTxt = sub.resolve("d.txt");
        Files.write(a, "a".getBytes());
        Files.write(b, "b".getBytes());
        Files.write(c, "c".getBytes());
        Files.write(subTxt, "d".getBytes());

        List<Path> txtFiles = manager.scanFiles(root, "*.txt");

        assertThat(txtFiles).hasSize(3);
        assertThat(txtFiles).extracting(Path::getFileName).extracting(Path::toString)
            .containsExactlyInAnyOrder("a.txt", "c.txt", "d.txt");
    }

    @Test
    void scanFiles_returnsEmptyWhenRootNotExists() throws IOException {
        Path notExists = tempDir.resolve("notExists");
        List<Path> result = manager.scanFiles(notExists, "*.txt");
        assertThat(result).isEmpty();
    }

    @Test
    void scanFiles_nullOrEmptyPattern_matchesAll() throws IOException {
        Path root = tempDir.resolve("allRoot");
        Files.createDirectories(root);
        Files.write(root.resolve("f1.txt"), "1".getBytes());
        Files.write(root.resolve("f2.dat"), "2".getBytes());

        List<Path> all = manager.scanFiles(root, null);
        assertThat(all).hasSize(2);

        List<Path> allEmpty = manager.scanFiles(root, "");
        assertThat(allEmpty).hasSize(2);
    }
}
