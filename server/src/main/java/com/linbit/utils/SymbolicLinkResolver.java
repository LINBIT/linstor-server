package com.linbit.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

public class SymbolicLinkResolver
{
    // Maximum number of symbolic links to follow
    public static final int MAX_REDIRECTS = 20;

    public static Path resolveSymLink(final String pathStr)
        throws IOException
    {
        final Path symLink = Path.of(pathStr);
        return resolveSymLink(symLink);
    }

    public static Path resolveSymLink(final Path symLink)
        throws IOException
    {
        Path curPath = symLink.toAbsolutePath();
        boolean isLink = false;
        int redirects = 0;
        // Follow symbolic links until reaching an object that is not a symbolic link,
        // or until the maximum number of redirects is reached.
        do
        {
            final BasicFileAttributes attr = Files.readAttributes(
                curPath, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS
            );
            isLink = attr.isSymbolicLink();
            if (isLink)
            {
                final Path target = Files.readSymbolicLink(curPath);
                if (target.isAbsolute())
                {
                    curPath = target;
                }
                else
                {
                    final Path parentPath = curPath.getParent();
                    final String directory = parentPath != null ? parentPath.toString() : "/";
                    curPath = Path.of(directory, target.toString());
                    curPath = curPath.normalize();
                }
            }
            ++redirects;
        }
        while (isLink && redirects < MAX_REDIRECTS);
        if (isLink)
        {
            // Loop terminated because of too many redirects without resolving the symbolic link, fail
            throw new IOException("Unable to resolve symbolic link \"" + symLink + "\": Too many redirects");
        }
        return curPath;
    }
}
