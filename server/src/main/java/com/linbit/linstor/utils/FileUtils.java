package com.linbit.linstor.utils;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.logging.ErrorReporter;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class FileUtils
{
    private FileUtils()
    {
        // utility class, usually there is no need to create an instance of this class
    }

    /**
     * Deletes the path with all of its content (sub-directories and -files)
     */
    public static void deleteDirectoryWithContent(Path path)
        throws IOException
    {
        deleteDirectoryWithContent(path, null);
    }

    /**
     * Deletes the path with all of its content (sub-directories and -files)
     */
    public static void deleteDirectoryWithContent(Path path, @Nullable ErrorReporter errorReporterRef)
        throws IOException
    {
        Files.walkFileTree(path, new DeleteFileVisitor(errorReporterRef));
    }

    public static class DeleteFileVisitor extends SimpleFileVisitor<Path>
    {
        private final @Nullable ErrorReporter errorReporter;

        public DeleteFileVisitor(@Nullable ErrorReporter errorReporterRef)
        {
            errorReporter = errorReporterRef;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
        {
            Files.delete(file);
            logDeleted(file, true);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException
        {
            Files.delete(file);
            logDeleted(file, false);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException
        {
            if (exc != null)
            {
                throw exc;
            }
            Files.delete(dir);
            logDeleted(dir, true);
            return FileVisitResult.CONTINUE;
        }

        private void logDeleted(Path dirRef, boolean accessibleRef)
        {
            if (errorReporter != null)
            {
                errorReporter.logTrace("Deleted %s%s", accessibleRef ? "" : "inaccessible ", dirRef.toString());
            }
        }
    }

}
