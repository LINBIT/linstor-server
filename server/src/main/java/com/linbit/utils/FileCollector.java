package com.linbit.utils;

import com.linbit.linstor.logging.LinstorFile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class FileCollector extends SimpleFileVisitor<Path>
{
    private static final Path LOG_DIR = Paths.get("./logs");
    private static final PathMatcher LOG_MATCHER = FileSystems.getDefault().getPathMatcher("glob:**.{mv.db,log}");
    private final Path parentDir;
    private final Set<LinstorFile> files = new HashSet<>();
    private final String nodeName;

    public FileCollector(String nodeNameRef, Path parentDirRef)
    {
        nodeName = nodeNameRef;
        parentDir = parentDirRef;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
    {
        if (LOG_MATCHER.matches(file))
        {
            String content = "";
            try
            {
                content = new String(Files.readAllBytes(file));
            }
            catch (IOException ignored)
            {
            }
            Path relFile = parentDir.relativize(file);
            files.add(
                new LinstorFile(
                    nodeName,
                    LOG_DIR.resolve(relFile).normalize().toString(),
                    new Date(attrs.creationTime().toMillis()),
                    content
                )
            );
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc)
    {
        Path relFile = parentDir.relativize(file);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        exc.printStackTrace(new PrintStream(baos));
        files.add(
            new LinstorFile(
                nodeName,
                LOG_DIR.toString() + relFile.getFileName().toString(),
                new Date(System.currentTimeMillis()),
                baos.toString()
            )
        );
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc)
    {
        return FileVisitResult.CONTINUE;
    }

    public Set<LinstorFile> getFiles()
    {
        return files;
    }
}
