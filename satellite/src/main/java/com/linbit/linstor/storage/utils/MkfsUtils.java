package com.linbit.linstor.storage.utils;

import com.linbit.TimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.layer.provider.utils.Commands;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class MkfsUtils
{
    public static List<String> shellSplit(CharSequence string)
    {
        List<String> tokens = new ArrayList<String>();
        boolean escaping = false;
        char quoteChar = ' ';
        boolean quoting = false;
        StringBuilder current = new StringBuilder();
        for (int index = 0; index < string.length(); index++)
        {
            char chr = string.charAt(index);
            if (escaping)
            {
                current.append(chr);
                escaping = false;
            }
            else if (chr == '\\' && !(quoting && quoteChar == '\''))
            {
                escaping = true;
            }
            else if (quoting && chr == quoteChar)
            {
                quoting = false;
            }
            else if (!quoting && (chr == '\'' || chr == '"'))
            {
                quoting = true;
                quoteChar = chr;
            }
            else if (!quoting && Character.isWhitespace(chr))
            {
                if (current.length() > 0)
                {
                    tokens.add(current.toString());
                    current = new StringBuilder();
                }
            }
            else
            {
                current.append(chr);
            }
        }
        if (current.length() > 0)
        {
            tokens.add(current.toString());
        }
        return tokens;
    }

    private static ExtCmd.OutputData makeFs(
        ExtCmd extCmd,
        String fileSystem,
        String devicePath,
        String additionalParams
    ) throws StorageException
    {
        final String cmdString = "mkfs." + fileSystem + " -q " + additionalParams + " " + devicePath;

        List<String> cmdList = shellSplit(cmdString);

        return Commands.genericExecutor(
            extCmd,
            cmdList.toArray(new String[0]),
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
        String filesys = null;
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
            for (Volume vlm : rsc.streamVolumes().collect(Collectors.toList()))
            {
                PriorityProps prioProps = new PriorityProps(
                    rsc.getProps(wrkCtx),
                    vlm.getVolumeDefinition().getProps(wrkCtx),
                    vlm.getResourceDefinition().getProps(wrkCtx)
                );

                final String fsType = prioProps.getProp(ApiConsts.KEY_FS_TYPE, ApiConsts.NAMESPC_FILESYSTEM);
                if (fsType != null)
                {
                    VlmProviderObject vlmProviderObject = rsc.getLayerData(wrkCtx).getVlmProviderObject(
                        vlm.getVolumeDefinition().getVolumeNumber()
                    );
                    final String devicePath = vlmProviderObject.getDevicePath();
                    Optional<String> optFsType = MkfsUtils.hasFileSystem(
                        extCmdFactory.create(),
                        devicePath
                    );
                    if (!optFsType.isPresent())
                    {
                        String mkfsParametes = prioProps.getProp(
                            ApiConsts.KEY_FS_MKFSPARAMETERS,
                            ApiConsts.NAMESPC_FILESYSTEM,
                            ""
                        );
                        if (fsType.equals(ApiConsts.VAL_FS_TYPE_EXT4))
                        {
                            if (VolumeUtils.isVolumeThinlyBacked(vlmProviderObject, false))
                            {
                                mkfsParametes += " -E nodiscard";
                            }
                            MkfsUtils.makeExt4(extCmdFactory.create(), devicePath, mkfsParametes);
                        }
                        else if (fsType.equals(ApiConsts.VAL_FS_TYPE_XFS))
                        {
                            if (VolumeUtils.isVolumeThinlyBacked(vlmProviderObject, false))
                            {
                                mkfsParametes += " -K";
                            }
                            MkfsUtils.makeXfs(extCmdFactory.create(), devicePath, mkfsParametes);
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
}
