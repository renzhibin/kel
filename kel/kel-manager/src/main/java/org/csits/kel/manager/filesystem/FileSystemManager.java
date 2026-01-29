package org.csits.kel.manager.filesystem;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * 文件系统操作抽象。
 */
public interface FileSystemManager {

    Path ensureDirectory(Path dir) throws IOException;

    void copyFile(Path source, Path target) throws IOException;

    void moveFile(Path source, Path target) throws IOException;

    List<Path> scanFiles(Path root, String pattern) throws IOException;
}

