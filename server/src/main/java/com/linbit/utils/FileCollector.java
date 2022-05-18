package com.linbit.utils;

import com.linbit.linstor.SosReportType;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Set;

public class FileCollector extends SimpleFileVisitor<Path>
{
    private static final PathMatcher LOG_MATCHER = FileSystems.getDefault().getPathMatcher("glob:**.{mv.db,log}");
    private final Set<SosReportType> files = new HashSet<>();
    private final Path relativizeTo;

    public FileCollector(Path relativizeToRef)
    {
        relativizeTo = relativizeToRef;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
    {
        if (LOG_MATCHER.matches(file))
        {
            files.add(
                new SosReportType.SosFileType(
                    "logs/" + relativizeTo.relativize(file),
                    file.toString(),
                    attrs.creationTime().toMillis()
                )
            );
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc)
    {
        files.add(
            new SosReportType.SosFileType(
                "logs/" + relativizeTo.relativize(file),
                file.toString(),
                -1
            )
        );
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc)
    {
        return FileVisitResult.CONTINUE;
    }

    public Set<SosReportType> getFiles()
    {
        return files;
    }
}
