package com.linbit.linstor.layer.storage.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.Commands;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PmemUtils
{
    private static final String FSDAX = "fsdax";
    private static final String BLOCKDEV = "blockdev";
    private static final String MODE = "mode";
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    private PmemUtils()
    {
    }

    public static boolean supportsDax(ExtCmd extCmd, String dev) throws StorageException
    {
        return supportsDax(extCmd, Collections.singletonList(dev));
    }

    public static boolean supportsDax(ExtCmd extCmd, List<String> devicesToCheck) throws StorageException
    {
        boolean supportsDax = false;
        try
        {
            OutputData outputData = Commands.genericExecutor(
                extCmd.setSaveWithoutSharedLocks(true),
                new String[]
                {
                    "ndctl", "list"
                },
                "Failed to list non-volatile memory devices",
                "Failed to list non-volatile memory devices"
            );
            String out = new String(outputData.stdoutData).trim();
            List<String> daxDevices = getDaxSupportingDevices(out);

            boolean allDevsSupportDax = true;

            for (String dev : devicesToCheck)
            {
                boolean currentDevSupportsDax = false;
                String devName;
                if (dev.startsWith("/dev/"))
                {
                    devName = dev.substring("/dev/".length());
                }
                else
                {
                    devName = dev;
                }
                for (String daxDev : daxDevices)
                {
                    if (devName.contains(daxDev))
                    {
                        currentDevSupportsDax = true;
                        break;
                    }
                }
                if (!currentDevSupportsDax)
                {
                    allDevsSupportDax = false;
                    break;
                }
            }
            supportsDax = allDevsSupportDax;
        }
        catch (IOException exc)
        {
            throw new StorageException("Failed to parse ndctl json", exc);
        }
        catch (StorageException exc)
        {
            // ignored, not supported
        }
        return supportsDax;
    }

    static List<String> getDaxSupportingDevices(String outRef) throws IOException
    {
        List<String> devices = new ArrayList<>();

        JsonNode jsonNode = OBJ_MAPPER.readTree(outRef);
        if (jsonNode != null)
        {
            if (jsonNode.isArray())
            {
                Iterator<JsonNode> jsonIt = jsonNode.elements();
                while (jsonIt.hasNext())
                {
                    extractDevice(devices, jsonIt.next());
                }
            }
            else
            {
                extractDevice(devices, jsonNode);
            }
        }
        return devices;
    }

    private static void extractDevice(List<String> devices, JsonNode entry)
    {
        JsonNode modeNode = entry.get(MODE);
        if (modeNode != null && modeNode.asText().equalsIgnoreCase(FSDAX))
        {
            JsonNode blockDevNode = entry.get(BLOCKDEV);
            if (blockDevNode != null)
            {
                devices.add(blockDevNode.asText());
            }
        }
    }
}
