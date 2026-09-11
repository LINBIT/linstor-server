package com.linbit.linstor.layer.drbd.resfiles;

import com.linbit.ImplementationError;
import com.linbit.PlatformStlt;
import com.linbit.drbd.DrbdVersion;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.layer.drbd.DrbdLayer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.utils.layer.DrbdLayerUtils;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class DrbdResourceFileUtils
{
    private static final String DRBD_CONFIG_SUFFIX = ".res";
    private static final String DRBD_CONFIG_TMP_SUFFIX = ".res_tmp";

    private final ErrorReporter errorReporter;
    private final WhitelistProps whitelistProps;
    private final StltConfigAccessor stltCfgAccessor;
    private final PlatformStlt platformStlt;
    private final DrbdVersion drbdVersion;

    @Inject
    public DrbdResourceFileUtils(
        ErrorReporter errorReporterRef,
        WhitelistProps whiltelistPropsRef,
        StltConfigAccessor stltCfgAccessorRef,
        DrbdVersion drbdVersionRef,
        PlatformStlt platformStltRef
    )
    {
        errorReporter = errorReporterRef;
        whitelistProps = whiltelistPropsRef;
        stltCfgAccessor = stltCfgAccessorRef;
        drbdVersion = drbdVersionRef;
        platformStlt = platformStltRef;
    }

    /**
     * Writes a new resfile if the content really changed.
     *
     * @return True if a new res file was written otherwise false.
     */
    public boolean regenerateResFile(DrbdRscData<Resource> drbdRscData)
        throws StorageException
    {
        boolean fileWritten = false;
        Path resFile = asResourceFile(drbdRscData, false, false);
        Path tmpResFile = asResourceFile(drbdRscData, true, false);

        List<DrbdRscData<Resource>> drbdPeerRscDataList = getPeerRscDataList(drbdRscData);

        String content = new ConfFileBuilder(
            errorReporter,
            drbdRscData,
            drbdPeerRscDataList,
            whitelistProps,
            stltCfgAccessor.getReadonlyProps(),
            drbdVersion
        ).build();

        String onDiskContent = "resource \"i\"{}";
        if (Files.exists(resFile))
        {
            try
            {
                onDiskContent = readResFile(resFile);
            }
            catch (NoSuchFileException nsfe)
            {
                errorReporter.logWarning("Expected resource file %s did not exist. Rewriting...", resFile.toString());
            }
            catch (IOException exc)
            {
                errorReporter.reportError(exc);
            }
        }

        if (!isResFileEqual(onDiskContent, content))
        {
            try (FileOutputStream resFileOut = new FileOutputStream(tmpResFile.toFile()))
            {
                resFileOut.write(content.getBytes(StandardCharsets.UTF_8));
            }
            catch (IOException ioExc)
            {
                String ioErrorMsg = ioExc.getMessage();
                if (ioErrorMsg == null)
                {
                    ioErrorMsg = "The runtime environment or operating system did not provide a description of " +
                        "the I/O error";
                }
                throw new StorageException(
                    "Creation of the DRBD configuration file for resource '" + drbdRscData.getSuffixedResourceName() +
                        "' failed due to an I/O error",
                    DrbdLayer.getAbortMsg(drbdRscData),
                    "Creation of the DRBD configuration file failed due to an I/O error",
                    """
                    - Check whether enough free space is available for the creation of the file
                    - Check whether the application has write access to the target directory
                    - Check whether the storage is operating flawlessly""",
                    "The error reported by the runtime environment or operating system is:\n" + ioErrorMsg,
                    ioExc
                );
            }

            try
            {
                Files.move(
                    tmpResFile,
                    resFile,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE
                );
                fileWritten = true;
            }
            catch (IOException ioExc)
            {
                String ioErrorMsg = ioExc.getMessage();
                throw new StorageException(
                    "Unable to move temporary DRBD resource file '" + tmpResFile + "' to resource directory.",
                    DrbdLayer.getAbortMsg(drbdRscData),
                    "Unable to move temporary DRBD resource file due to an I/O error",
                    """
                    - Check whether enough free space is available for moving the file
                    - Check whether the application has write access to the target directory
                    - Check whether the storage is operating flawlessly""",
                    "The error reported by the runtime environment or operating system is:\n" + ioErrorMsg,
                    ioExc
                );
            }
            errorReporter.logInfo("DRBD regenerated resource file: %s", resFile);
        }
        return fileWritten;
    }

    /**
     * Returns the list of peer {@link DrbdRscData} the given local resource is expected to connect to, i.e. the
     * peers that are also considered during res file generation.
     */
    public static List<DrbdRscData<Resource>> getPeerRscDataList(DrbdRscData<Resource> drbdRscData)
    {
        return drbdRscData.getRscDfnLayerObject()
            .getDrbdRscDataList()
            .stream()
            .filter(
                otherRscData -> !otherRscData.equals(drbdRscData) &&
                    DrbdLayerUtils.isDrbdResourceExpected(otherRscData) &&
                    !otherRscData.getAbsResource().getStateFlags().isSet(Resource.Flags.INACTIVE)
            )
            .collect(Collectors.toList());
    }

    public boolean restoreBackupResFile(DrbdRscData<Resource> drbdRscData) throws StorageException
    {
        boolean success = false;
        String rscName = drbdRscData.getSuffixedResourceName();
        errorReporter.logError("Restoring resource file from backup: %s", rscName);
        Path backupFile = asBackupResourceFile(drbdRscData);
        if (Files.exists(backupFile))
        {
            Path resFile = asResourceFile(drbdRscData, false, false);
            success = copyResFile(
                backupFile,
                resFile,
                String.format("Failed to restore resource file from backup of resource '%s'", rscName),
                DrbdLayer.getAbortMsg(drbdRscData)
            );
        }
        return success;
    }

    public void deleteResFile(DrbdRscData<Resource> drbdRscDataRef) throws IOException
    {
        Path resFile = asResourceFile(drbdRscDataRef, false, false);
        errorReporter.logDebug("Ensuring .res file is deleted: %s ", resFile);
        Files.deleteIfExists(resFile);
    }

    public void deleteBackupResFile(DrbdRscData<Resource> drbdRscDataRef) throws StorageException
    {
        Path resFile = asBackupResourceFile(drbdRscDataRef);
        errorReporter.logDebug("Deleting res file from backup: %s ", resFile);
        try
        {
            Files.deleteIfExists(resFile);
        }
        catch (IOException exc)
        {
            throw new StorageException("IOException while removing resource file from backup", exc);
        }
    }

    public boolean doesResFileExist(DrbdRscData<Resource> drbdRscDataRef)
    {
        return Files.exists(asResourceFile(drbdRscDataRef, false, false));
    }

    public void copyResFileToBackup(DrbdRscData<Resource> drbdRscData) throws StorageException
    {
        Path resFile = asResourceFile(drbdRscData, false, false);
        Path backupFile = asBackupResourceFile(drbdRscData);
        String rscName = drbdRscData.getSuffixedResourceName();
        copyResFile(
            resFile,
            backupFile,
            String.format("Failed to create a backup of the resource file of resource '%s'", rscName),
            DrbdLayer.getAbortMsg(drbdRscData)
        );
    }

    private String readResFile(Path resFilePath) throws IOException
    {
        return Files.readString(resFilePath);
    }

    private Path asResourceFile(DrbdRscData<Resource> drbdRscData, boolean temp, boolean cygwinFormat)
    {
        String prefix;

        if (cygwinFormat)
        {
            prefix = platformStlt.sysRootCygwin();
        }
        else
        {
            prefix = platformStlt.sysRoot();
        }

        return Paths.get(
            prefix + LinStor.CONFIG_PATH,
            drbdRscData.getSuffixedResourceName() + (temp ? DRBD_CONFIG_TMP_SUFFIX : DRBD_CONFIG_SUFFIX)
        );
    }

    private Path asBackupResourceFile(DrbdRscData<Resource> drbdRscData)
    {
        return Paths.get(
            platformStlt.sysRoot() + LinStor.BACKUP_PATH,
            drbdRscData.getSuffixedResourceName() + DRBD_CONFIG_SUFFIX
        );
    }

    /**
     * Compares the contents of 2 res files if they are equal
     * Starts comparing, after finding the 'resources "' section, because Linstor generates a header with date.
     *
     * @param onDiskContent String content of the existing res file on disk (may be empty or corrupt)
     * @param newContent String content of the freshly generated res file
     * @return true if real res file content is the same
     */
    // package-private and static (uses no instance state) so it can be unit tested directly
    static boolean isResFileEqual(String onDiskContent, String newContent)
    {
        boolean equal;
        int beginNew = newContent.indexOf("resource \"");
        if (beginNew < 0)
        {
            // The freshly generated content must always contain a resource section
            throw new ImplementationError("isResFileEqual should only be used for DRBD res files.");
        }

        int beginOnDisk = onDiskContent.indexOf("resource \"");
        if (beginOnDisk < 0)
        {
            // The on-disk file is empty, truncated or otherwise corrupt (no resource section).
            // Treat it as not equal so the caller rewrites the file.
            equal = false;
        }
        else
        {
            equal = onDiskContent.substring(beginOnDisk).equals(newContent.substring(beginNew));
        }
        return equal;
    }

    private boolean copyResFile(Path srcPath, Path dstPath, String errMsg, String errCause)
        throws StorageException
    {
        try
        {
            Files.copy(srcPath, dstPath, StandardCopyOption.REPLACE_EXISTING);
        }
        catch (IOException ioExc)
        {
            String ioErrorMsg = ioExc.getMessage();
            if (ioErrorMsg == null)
            {
                ioErrorMsg = "The runtime environment or operating system did not provide a description of " +
                    "the I/O error";
            }
            throw new StorageException(
                errMsg,
                errCause,
                null,
                """
                - Check whether enough free space is available for the creation of the file
                - Check whether the application has write access to the target directory
                - Check whether the storage is operating flawlessly""",
                "The error reported by the runtime environment or operating system is:\n" + ioErrorMsg,
                ioExc
            );
        }
        return true;
    }
}
