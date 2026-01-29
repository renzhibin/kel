package org.csits.kel.manager.filesystem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.stereotype.Component;

/**
 * 本地文件系统实现。
 */
@Component
public class LocalFileSystemManager implements FileSystemManager {

    @Override
    public Path ensureDirectory(Path dir) throws IOException {
        if (Files.notExists(dir)) {
            Files.createDirectories(dir);
        }
        return dir;
    }

    @Override
    public void copyFile(Path source, Path target) throws IOException {
        Path parent = target.getParent();
        if (parent != null && Files.notExists(parent)) {
            Files.createDirectories(parent);
        }
        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void moveFile(Path source, Path target) throws IOException {
        Path parent = target.getParent();
        if (parent != null && Files.notExists(parent)) {
            Files.createDirectories(parent);
        }
        Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public List<Path> scanFiles(Path root, String pattern) throws IOException {
        if (Files.notExists(root)) {
            return new ArrayList<>();
        }
        try (Stream<Path> stream = Files.walk(root)) {
            return stream
                .filter(Files::isRegularFile)
                .filter(p -> p.getFileName().toString().matches(convertGlobToRegex(pattern)))
                .collect(Collectors.toList());
        }
    }

    private String convertGlobToRegex(String glob) {
        if (glob == null || glob.isEmpty()) {
            return ".*";
        }
        StringBuilder sb = new StringBuilder(glob.length());
        int inGroup = 0;
        int inClass = 0;
        int firstIndexInClass = -1;
        char[] arr = glob.toCharArray();
        for (int i = 0; i < arr.length; i++) {
            char ch = arr[i];
            switch (ch) {
                case '\\':
                    if (++i >= arr.length) {
                        sb.append('\\');
                    } else {
                        char next = arr[i];
                        switch (next) {
                            case ',':
                                // escape not needed
                                break;
                            case 'Q':
                            case 'E':
                                // extra escape needed
                                sb.append('\\');
                            default:
                                sb.append('\\');
                        }
                        sb.append(next);
                    }
                    break;
                case '*':
                    if (inClass == 0) {
                        sb.append(".*");
                    } else {
                        sb.append('*');
                    }
                    break;
                case '?':
                    if (inClass == 0) {
                        sb.append('.');
                    } else {
                        sb.append('?');
                    }
                    break;
                case '[':
                    inClass++;
                    firstIndexInClass = i + 1;
                    sb.append('[');
                    break;
                case ']':
                    inClass--;
                    sb.append(']');
                    break;
                case '.':
                case '(':
                case ')':
                case '+':
                case '|':
                case '^':
                case '$':
                case '@':
                case '%':
                    if (inClass == 0 || (firstIndexInClass == i && arr[i] == '^')) {
                        sb.append('\\');
                    }
                    sb.append(ch);
                    break;
                case '!':
                    if (firstIndexInClass == i) {
                        sb.append('^');
                    } else {
                        sb.append('!');
                    }
                    break;
                case '{':
                    inGroup++;
                    sb.append('(');
                    break;
                case '}':
                    inGroup--;
                    sb.append(')');
                    break;
                case ',':
                    if (inGroup > 0) {
                        sb.append('|');
                    } else {
                        sb.append(',');
                    }
                    break;
                default:
                    sb.append(ch);
            }
        }
        return sb.toString();
    }
}

