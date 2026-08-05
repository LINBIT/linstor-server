package com.linbit.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;

public class SymbolicLinkResolver
{
    // Maximum number of symbolic links to follow
    public static final int MAX_REDIRECTS = 20;

    private SymbolicLinkResolver()
    {
    }

    public static Path resolveSymLink(final String pathStr)
        throws IOException
    {
        final Path symLink = Path.of(pathStr);
        return resolveSymLink(symLink);
    }

    /**
     * <p>Follows the chain of symbolic links starting at the specified path until an object that is not
     * a symbolic link is reached, and returns that object's path.</p>
     *
     * <p><b>Difference to {@link Path#toRealPath}:</b> {@code toRealPath} canonicalizes the <em>entire</em>
     * path against the filesystem: symbolic links are resolved in every path component (including parent
     * directories), and {@code ".."} is resolved according to the actual directory structure. This method
     * only follows the symbolic link chain of the path's final component; relative link targets are joined
     * onto the link's parent directory and folded textually via {@link Path#normalize()}. Consequently the
     * returned path is guaranteed to not be a symbolic link itself, but is not necessarily canonical:
     * parent components that are symbolic links remain unresolved, and a {@code ".."} in a link target
     * underneath a symlinked directory is folded textually instead of through the real directory tree.</p>
     *
     * <p><b>When to use which:</b> Use this method when only the final filesystem object matters and
     * canonicalizing the directory part of the path buys nothing. The prime example is
     * {@code BlockSizeInfo}: it resolves a device path (e.g. {@code /dev/mapper/<vg>-<lv>} or
     * {@code /dev/disk/by-id/...}) merely to obtain the kernel name of the underlying block device
     * special file ({@code dm-5}, {@code nvme1n1}, ...), which is then used to build the
     * {@code /sys/block/<name>/queue/...} lookup path. Only {@link Path#getFileName()} of the result is
     * consumed there, so whether the directory part is canonical is irrelevant - and since {@code ".."}
     * in a link target can only ever change the directory part, the final name stays correct even in the
     * textual-normalization edge case described above. In addition, this method fails deterministically
     * after {@link #MAX_REDIRECTS} hops with a descriptive message instead of depending on the operating
     * system's symlink loop limit ({@code ELOOP}).</p>
     *
     * <p>For comparing two device paths for identity ("does {@code /dev/disk/by-id/X} point to
     * {@code /dev/nvme1n1}?") or whenever a canonical path is required, use {@link Path#toRealPath}
     * (on both sides of the comparison) instead.</p>
     *
     * <p>Like {@code toRealPath}, this method requires every step of the chain to exist: a dangling
     * symbolic link results in an {@link IOException}.</p>
     *
     * @param symLink Path to resolve; resolved against the current working directory if relative. The path
     *     does not need to be a symbolic link — the path of a regular filesystem object is returned
     *     unchanged (as an absolute path).
     *
     * @return Absolute path of the first object in the chain that is not a symbolic link
     *
     * @throws IOException If a path in the chain does not exist or cannot be accessed, or if the chain
     *     exceeds {@link #MAX_REDIRECTS} links (e.g. cyclic symbolic links)
     */
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
                curPath,
                BasicFileAttributes.class,
                LinkOption.NOFOLLOW_LINKS
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

    /**
     * <p>Returns whether or not the two given paths point to the same file/directory.</p>
     * <p>Returns {@code false} in case of {@link IOException}</p>
     */
    public static boolean pathsEquals(String pathA, String pathB)
    {
        boolean equals;
        try
        {
            Path realPathA = Paths.get(pathA).toRealPath();
            Path realPathB = Paths.get(pathB).toRealPath();
            equals = realPathA.equals(realPathB);
        }
        catch (IOException ignored)
        {
            equals = false;
        }
        return equals;
    }
}
