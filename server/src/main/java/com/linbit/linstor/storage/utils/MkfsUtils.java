package com.linbit.linstor.storage.utils;

import com.linbit.TimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.utils.ShellUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class MkfsUtils
{
    private static ExtCmd.OutputData makeFs(
        ExtCmd extCmd,
        String fileSystem,
        String devicePath,
        String additionalParams
    ) throws StorageException
    {
        final String cmdString = "mkfs." + fileSystem + " -q " + additionalParams + " " + devicePath;

        List<String> cmdList = ShellUtils.shellSplit(cmdString);

        return Commands.genericExecutor(extCmd,
            cmdList.toArray(new String[cmdList.size()]),
            "Failed to mkfs " + devicePath,
            "Failed to mfks " + devicePath
        );
    }

    public static ExtCmd.OutputData makeExt4(
        ExtCmd extCmd,
        String devicePath,
        String additionalParams
    ) throws StorageException
    {
        return makeFs(extCmd, "ext4", devicePath, additionalParams);
    }

    public static ExtCmd.OutputData makeXfs(
        ExtCmd extCmd,
        String devicePath,
        String additionalParams
    ) throws StorageException
    {
        return makeFs(extCmd, "xfs", devicePath, additionalParams);
    }

    public static Optional<String> hasFileSystem(
        ExtCmd extCmd,
        String devicePath
    ) throws StorageException
    {
        @Nullable String filesys = null;
        try
        {
            ExtCmd.OutputData outData = extCmd.exec("blkid", "-o", "export", devicePath);
            if (outData.exitCode == 0)
            {
                BufferedReader br = new BufferedReader(new InputStreamReader(outData.getStdoutStream()));
                filesys = br.lines()
                    .filter(line -> line.startsWith("TYPE="))
                    .map(type -> type.substring("TYPE=".length()))
                    .findFirst()
                    .orElse(null);
            }
            // else blkid couldn't determine any FS or other type, maybe null
        }
        catch (TimeoutException | IOException exc)
        {
            throw new StorageException("Unable to execute command blkid", exc);
        }
        return Optional.ofNullable(filesys);
    }

    public static boolean needsToCreateFs(Resource rsc, AccessContext wrkCtx)
        throws InvalidKeyException, AccessDeniedException
    {
        boolean ret = false;
        if (rsc.getLayerData(wrkCtx).checkFileSystem())
        {
            for (AbsVolume<Resource> vlm : rsc.streamVolumes().collect(Collectors.toList()))
            {
                VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
                ResourceDefinition rscDfn = vlm.getResourceDefinition();
                ResourceGroup rscGrp = rscDfn.getResourceGroup();
                PriorityProps prioProps = new PriorityProps(
                    rsc.getProps(wrkCtx),
                    vlmDfn.getProps(wrkCtx),
                    rscGrp.getVolumeGroupProps(wrkCtx, vlmDfn.getVolumeNumber()),
                    rscDfn.getProps(wrkCtx),
                    rscGrp.getProps(wrkCtx)
                );

                if (prioProps.getProp(ApiConsts.KEY_FS_TYPE, ApiConsts.NAMESPC_FILESYSTEM) != null)
                {
                    ret = true;
                    break;
                }
            }
        }
        return ret;
    }

    public static void makeFileSystemOnMarked(
        ErrorReporter errorReporter,
        ExtCmdFactory extCmdFactory,
        AccessContext wrkCtx,
        Resource rsc
    )
        throws StorageException, AccessDeniedException, InvalidKeyException
    {
        if (rsc.getLayerData(wrkCtx).checkFileSystem())
        {
            rsc.getLayerData(wrkCtx).disableCheckFileSystem();
            for (AbsVolume<Resource> vlm : rsc.streamVolumes().collect(Collectors.toList()))
            {
                VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
                ResourceDefinition rscDfn = vlm.getResourceDefinition();
                ResourceGroup rscGrp = rscDfn.getResourceGroup();
                PriorityProps prioProps = new PriorityProps(
                    rsc.getProps(wrkCtx),
                    vlmDfn.getProps(wrkCtx),
                    rscGrp.getVolumeGroupProps(wrkCtx, vlmDfn.getVolumeNumber()),
                    rscDfn.getProps(wrkCtx),
                    rscGrp.getProps(wrkCtx)
                );

                final @Nullable String fsType = prioProps.getProp(ApiConsts.KEY_FS_TYPE, ApiConsts.NAMESPC_FILESYSTEM);
                if (fsType != null)
                {
                    VlmProviderObject<Resource> vlmProviderObject = rsc.getLayerData(wrkCtx).getVlmProviderObject(
                        vlmDfn.getVolumeNumber()
                    );
                    final String devicePath = vlmProviderObject.getDevicePath();
                    Optional<String> optFsType = MkfsUtils.hasFileSystem(
                        extCmdFactory.create(),
                        devicePath
                    );
                    if (optFsType.isEmpty())
                    {
                        String mkfsParametes = prioProps.getProp(
                            ApiConsts.KEY_FS_MKFSPARAMETERS,
                            ApiConsts.NAMESPC_FILESYSTEM,
                            ""
                        );

                        @Nullable String mkfsUser = prioProps.getProp(
                            ApiConsts.KEY_FS_USER, ApiConsts.NAMESPC_FILESYSTEM);
                        @Nullable String mkfsGroup = prioProps.getProp(
                            ApiConsts.KEY_FS_GROUP, ApiConsts.NAMESPC_FILESYSTEM);

                        if (mkfsUser != null || mkfsGroup != null)
                        {
                            if (mkfsUser == null)
                            {
                                mkfsUser = "nobody";
                            }
                            if (mkfsGroup == null)
                            {
                                mkfsGroup = mkfsUser;
                            }
                        }

                        if (fsType.equals(ApiConsts.VAL_FS_TYPE_EXT4))
                        {
                            mkfsParametes += " -F -E nodiscard"; // -F force, -E additional options

                            if (mkfsUser != null && mkfsGroup != null)
                            {
                                long mkfsUID = getUserId(extCmdFactory, mkfsUser);
                                long mkfsGID = getGroupId(extCmdFactory, mkfsGroup);

                                mkfsParametes += " -E root_owner=" + mkfsUID + ":" + mkfsGID;
                            }

                            MkfsUtils.makeExt4(extCmdFactory.create(), devicePath, mkfsParametes);
                        }
                        else
                        if (fsType.equals(ApiConsts.VAL_FS_TYPE_XFS))
                        {
                            mkfsParametes += " -f -K"; // -f force, -K no discard

                            @Nullable File tempFile = null;
                            if (mkfsUser != null && mkfsGroup != null)
                            {
                                try
                                {
                                    tempFile = File.createTempFile(
                                        "linstor",
                                        "xfs_proto_" + rsc.getResourceDefinition().getName().displayValue
                                    );
                                    long mkfsUID = getUserId(extCmdFactory, mkfsUser);
                                    long mkfsGID = getGroupId(extCmdFactory, mkfsGroup);
                                    Files.write(
                                        tempFile.toPath(),
                                        ("/generated/by/linstor 13 42 d--777 " + mkfsUID + " " + mkfsGID + "\n")
                                            .getBytes()
                                    );

                                    mkfsParametes += " -p " + tempFile.getAbsolutePath();
                                }
                                catch (IOException exc)
                                {
                                    throw new StorageException("Failed to create proto file for XFS creation", exc);
                                }
                            }

                            MkfsUtils.makeXfs(extCmdFactory.create(), devicePath, mkfsParametes);

                            if (tempFile != null)
                            {
                                try
                                {
                                    Files.deleteIfExists(tempFile.toPath());
                                }
                                catch (IOException exc)
                                {
                                    throw new StorageException("Failed to delete proto file after XFS creation", exc);
                                }
                            }
                        }
                        else
                        {
                            errorReporter.logError(
                                String.format("Unknown file system type %s. ignoring.", fsType)
                            );
                        }
                    }
                    // else Check for mismatch?
                }
            }
        }
    }

    private static long getUserId(ExtCmdFactory extCmdFactory, String name) throws StorageException
    {
        return getUserGroupId(extCmdFactory, name, "-u", "user");
    }

    private static long getGroupId(ExtCmdFactory extCmdFactory, String name) throws StorageException
    {
        return getUserGroupId(extCmdFactory, name, "-g", "group");
    }

    private static long getUserGroupId(ExtCmdFactory extCmdFactory, String idName, String idType, String idDescr)
        throws StorageException
    {
        long ret;
        try
        {
            ret = Long.parseLong(idName);
        }
        catch (NumberFormatException nfe)
        {
            String errMsg = "Failed to find " + idDescr + " '" + idName + "'s " + idDescr + " id";
            try
            {
                OutputData outputData = Commands.genericExecutor(
                    extCmdFactory.create(),
                    new String[]
                    {
                        "id", idType, idName
                    },
                    errMsg,
                    errMsg
                );
                ret = Long.parseLong(new String(outputData.stdoutData).trim());
            }
            catch (NumberFormatException nfe2)
            {
                throw new StorageException(errMsg, nfe2);
            }
        }
        return ret;
    }

    private MkfsUtils()
    {
    }
}
