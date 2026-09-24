package com.linbit.linstor.core.cfg;

import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.annotation.Nullable;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.moandjiezana.toml.Toml;

/**
 * Loads a toml configuration from more than one file.
 *
 * <p>
 * The main configuration file is read first, followed by every {@value #INCLUDE_FILE_GLOB} file of the include
 * directory, in lexicographical order of its file name. The include directory defaults to a directory next to the
 * main configuration file and can be changed with the top level {@value #INCLUDE_DIR_KEY} key; a relative directory
 * is resolved against the directory of the main configuration file.
 * </p>
 *
 * <p>
 * The caller is expected to apply the returned configurations in the given order. As the setters of
 * {@link LinstorConfig} ignore null values, a key that a later file does not mention keeps the value of an earlier
 * file, while a key it does mention overrides it. The external file whitelist is the exception: every file adds to
 * it, see {@link StltConfig#addToExternalFilesWhitelist}.
 * </p>
 */
public class TomlConfigLoader
{
    public static final String INCLUDE_DIR_KEY = "includeDir";
    public static final String INCLUDE_FILE_GLOB = "*.toml";

    /**
     * The configuration files that were read, together with their parsed content.
     *
     * @param includeDir the effective include directory
     * @param paths the configuration files that were read, in the order they have to be applied
     * @param configs the parsed configurations, index-aligned with <code>paths</code>
     */
    public record Result<T>(String includeDir, List<Path> paths, List<T> configs)
    {
    }

    private TomlConfigLoader()
    {
    }

    /**
     * Reads the main configuration file and all drop-in files of the include directory.
     *
     * @param cmdLineIncludeDirRef include directory from the command line, which outranks the main configuration
     *     file, or null
     * @param envIncludeDirRef include directory from the environment, which the main configuration file can
     *     override, or null
     * @param dfltIncludeDirRef include directory to use if none of the three configures one. A missing directory is
     *     only reported if it was explicitly configured.
     *
     * @throws LinStorRuntimeException if one of the configuration files cannot be read or parsed
     */
    public static <T> Result<T> loadAll(
        Path mainCfgPathRef,
        Class<T> tomlClassRef,
        @Nullable String cmdLineIncludeDirRef,
        @Nullable String envIncludeDirRef,
        String dfltIncludeDirRef
    )
    {
        List<Path> paths = new ArrayList<>();
        List<T> configs = new ArrayList<>();
        @Nullable String tomlIncludeDir = null;

        if (Files.exists(mainCfgPathRef))
        {
            System.out.println("Loading configuration file \"" + mainCfgPathRef + "\"");
            Toml mainToml = load(mainCfgPathRef);
            tomlIncludeDir = readIncludeDir(mainCfgPathRef, mainToml);
            paths.add(mainCfgPathRef);
            configs.add(convert(mainCfgPathRef, mainToml, tomlClassRef));
        }

        // command line beats the main configuration file, which in turn beats the environment
        @Nullable String configuredIncludeDir = cmdLineIncludeDirRef;
        if (configuredIncludeDir == null)
        {
            configuredIncludeDir = tomlIncludeDir;
        }
        if (configuredIncludeDir == null)
        {
            configuredIncludeDir = envIncludeDirRef;
        }
        boolean explicitIncludeDir = configuredIncludeDir != null;
        String effectiveIncludeDir = explicitIncludeDir ? configuredIncludeDir : dfltIncludeDirRef;

        Path includeDirPath = resolveIncludeDir(mainCfgPathRef, effectiveIncludeDir);
        for (Path includeFile : listIncludeFiles(includeDirPath, mainCfgPathRef, explicitIncludeDir))
        {
            System.out.println("Loading configuration override file \"" + includeFile + "\"");
            Toml includeToml = load(includeFile);
            if (readIncludeDir(includeFile, includeToml) != null)
            {
                System.err.printf(
                    "Ignoring '%s' of '%s': only the main configuration file can include a directory%n",
                    INCLUDE_DIR_KEY,
                    includeFile
                );
            }
            paths.add(includeFile);
            configs.add(convert(includeFile, includeToml, tomlClassRef));
        }

        return new Result<>(
            effectiveIncludeDir,
            Collections.unmodifiableList(paths),
            Collections.unmodifiableList(configs)
        );
    }

    /**
     * The absolute include directory that {@link #loadAll} would use for the given main configuration file.
     *
     * @throws LinStorRuntimeException if the main configuration file cannot be read or parsed
     */
    public static Path effectiveIncludeDir(Path mainCfgPathRef, String dfltIncludeDirRef)
    {
        @Nullable String includeDir = null;
        if (Files.exists(mainCfgPathRef))
        {
            includeDir = readIncludeDir(mainCfgPathRef, load(mainCfgPathRef));
        }
        return resolveIncludeDir(mainCfgPathRef, includeDir == null ? dfltIncludeDirRef : includeDir);
    }

    /**
     * {@link Toml#getString} casts, so a non-string value would escape as a ClassCastException instead of the usual
     * parse error.
     */
    private static @Nullable String readIncludeDir(Path pathRef, Toml tomlRef)
    {
        try
        {
            return tomlRef.getString(INCLUDE_DIR_KEY);
        }
        catch (ClassCastException ccExc)
        {
            throw new LinStorRuntimeException(
                String.format("Error parsing '%s': '%s' must be a string", pathRef, INCLUDE_DIR_KEY),
                ccExc
            );
        }
    }

    private static Path resolveIncludeDir(Path mainCfgPathRef, String includeDirRef)
    {
        @Nullable Path mainCfgDir = mainCfgPathRef.toAbsolutePath().getParent();
        return mainCfgDir == null ?
            Paths.get(includeDirRef).normalize() :
            mainCfgDir.resolve(includeDirRef).normalize();
    }

    /**
     * The include directory can be the directory of the main configuration file itself, which must not be read a
     * second time.
     */
    private static List<Path> listIncludeFiles(
        Path includeDirPathRef,
        Path mainCfgPathRef,
        boolean reportMissingRef
    )
    {
        List<Path> ret = new ArrayList<>();
        if (Files.isDirectory(includeDirPathRef))
        {
            Path mainCfgFile = mainCfgPathRef.toAbsolutePath().normalize();
            try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(includeDirPathRef, INCLUDE_FILE_GLOB))
            {
                for (Path path : dirStream)
                {
                    if (Files.isRegularFile(path) && !path.toAbsolutePath().normalize().equals(mainCfgFile))
                    {
                        ret.add(path);
                    }
                }
            }
            catch (IOException ioExc)
            {
                throw new LinStorRuntimeException(
                    String.format("Error reading '%s': %s", includeDirPathRef, ioExc.getMessage()),
                    ioExc
                );
            }
            ret.sort(Comparator.comparing(path -> path.getFileName().toString()));
        }
        else if (reportMissingRef)
        {
            System.err.printf("Configuration directory '%s' does not exist, ignoring%n", includeDirPathRef);
        }
        return ret;
    }

    private static Toml load(Path pathRef)
    {
        try
        {
            return new Toml().read(pathRef.toFile());
        }
        catch (RuntimeException tomlExc)
        {
            throw new LinStorRuntimeException(
                String.format("Error parsing '%s': %s", pathRef, tomlExc.getMessage()),
                tomlExc
            );
        }
    }

    private static <T> T convert(Path pathRef, Toml tomlRef, Class<T> tomlClassRef)
    {
        try
        {
            return tomlRef.to(tomlClassRef);
        }
        catch (RuntimeException tomlExc)
        {
            throw new LinStorRuntimeException(
                String.format("Error parsing '%s': %s", pathRef, tomlExc.getMessage()),
                tomlExc
            );
        }
    }
}
