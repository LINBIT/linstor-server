package com.linbit.linstor.layer.storage.spdk.utils;

import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.extproc.ExtCmd;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.layer.storage.spdk.SpdkCommands;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.Commands;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;

public class SpdkUtils
{
    public static final String SPDK_PATH_PREFIX = "spdk:";
    public static final String SPDK_PRODUCK_NAME = "product_name";
    public static final String SPDK_LOGICAL_VOLUME = "Logical Volume";
    public static final String SPDK_ALIASES = "aliases";
    public static final String SPDK_BLOCK_SIZE = "block_size";
    public static final String SPDK_NUM_BLOCKS = "num_blocks";
    public static final String SPDK_NAME = "name";
    public static final String SPDK_NAMESPACES = "namespaces";
    public static final String SPDK_UUID = "uuid";
    public static final String SPDK_NQN = "nqn";
    public static final String SPDK_NSID = "nsid";
    public static final String SPDK_FREE_CLUSTERS = "free_clusters";
    public static final String SPDK_TOTAL_DATA_CLUSTERS = "total_data_clusters";
    public static final String SPDK_CLUSTER_SIZE = "cluster_size";
    public static final String SPDK_DRIVER_SPECIFIC = "driver_specific";
    public static final String SPDK_LVOL = "lvol";
    public static final String SPDK_LVOL_STORE_UUID = "lvol_store_uuid";
    private static final float SPDK_DEFAULT_DATA_PERCENT = 100;

    private SpdkUtils()
    {
    }

    public static class LvsInfo
    {
        public final String volumeGroup;
        public final @Nullable String thinPool;
        public final String identifier;
        public final String path;
        public final long size;
        public final float dataPercent;
        public final @Nullable String attributes;

        LvsInfo(
            String volumeGroupRef,
            @Nullable String thinPoolRef,
            String identifierRef,
            String pathRef,
            long sizeRef,
            float dataPercentRef,
            @Nullable String attributesRef
        )
        {
            volumeGroup = volumeGroupRef;
            thinPool = thinPoolRef;
            identifier = identifierRef;
            path = pathRef;
            size = sizeRef;
            dataPercent = dataPercentRef;
            attributes = attributesRef;
        }
    }

    public static <T> HashMap<String, LvsInfo> getLvsInfo(
        final SpdkCommands<T> spdkCommands,
        final Set<String> volumeGroups
    )
        throws StorageException, AccessDeniedException
    {
        final HashMap<String, LvsInfo> infoByIdentifier = new HashMap<>();

        Iterator<JsonNode> elements = spdkCommands.getJsonElements(spdkCommands.lvs());
        while (elements.hasNext())
        {
            JsonNode element = elements.next();

            if (element.path(SPDK_PRODUCK_NAME).asText().equals(SPDK_LOGICAL_VOLUME))
            {
                final String vgStr = SpdkUtils.getVgNameFromUuid(
                    spdkCommands,
                    element.path(SPDK_DRIVER_SPECIFIC).path(SPDK_LVOL).path(SPDK_LVOL_STORE_UUID).asText()
                );
                if (volumeGroups.contains(vgStr))
                {
                    Iterator<JsonNode> aliases = element.path(SPDK_ALIASES).elements();
                    String pathAlias = "";

                    while (aliases.hasNext())
                    {
                        JsonNode alias = aliases.next();
                        pathAlias = alias.asText();
                    }

                    final String identifier = pathAlias.split("/")[1]; // 0 is Volume Group, 1 is Logical Volume
                    final String path = String.format(SPDK_PATH_PREFIX + "%s/%s", vgStr, identifier);
                    final long size = element.path(SPDK_BLOCK_SIZE).asLong() * element.path(SPDK_NUM_BLOCKS).asLong();
                    final float dataPercent = SPDK_DEFAULT_DATA_PERCENT;
                    final String attributes = null;
                    final String thinPoolStr = null;

                    final LvsInfo state = new LvsInfo(
                        vgStr,
                        thinPoolStr,
                        identifier,
                        path,
                        size,
                        dataPercent,
                        attributes
                    );
                    infoByIdentifier.put(vgStr + File.separator + identifier, state);
                }
            }
        }

        return infoByIdentifier;
    }

    public static <T> Map<String, Long> getExtentSize(SpdkCommands<T> spdkCommands, Set<String> volumeGroups)
        throws StorageException, AccessDeniedException
    {
        final Map<String, Long> result = new HashMap<>();

        Iterator<JsonNode> elements = spdkCommands.getJsonElements(spdkCommands.getLvolStores());
        while (elements.hasNext())
        {
            JsonNode element = elements.next();
            if (volumeGroups.contains(element.path(SPDK_NAME).asText()))
            {
                result.put(
                    element.path(SPDK_NAME).asText(),
                    element.path(SPDK_BLOCK_SIZE).asLong() // in KiB
                );
            }
        }
        return result;
    }

    public static <T> Long getBlockSizeByName(final SpdkCommands<T> spdkCommands, String name)
        throws StorageException, AccessDeniedException
    {
        Long blockSize;
        Iterator<JsonNode> elements = spdkCommands.getJsonElements(spdkCommands.lvsByName(name));
        if (elements.hasNext())
        {
            JsonNode element = elements.next();
            blockSize = element.path(SPDK_BLOCK_SIZE).asLong() * element.path(SPDK_NUM_BLOCKS).asLong();
        }
        else
        {
            throw new StorageException("Volume not found: " + name);
        }
        return blockSize;
    }

    public static <T> Map<String, Long> getVgTotalSize(final SpdkCommands<T> spdkCommands, Set<String> volumeGroups)
        throws StorageException, AccessDeniedException
    {
        final Map<String, Long> result = new HashMap<>();

        Iterator<JsonNode> elements = spdkCommands.getJsonElements(spdkCommands.getLvolStores());
        while (elements.hasNext())
        {
            JsonNode element = elements.next();
            if (volumeGroups.contains(element.path(SPDK_NAME).asText()))
            {
                result.put(
                    element.path(SPDK_NAME).asText(),
                    SizeConv.convert(
                        element.path(SPDK_CLUSTER_SIZE).asLong() * element.path(SPDK_TOTAL_DATA_CLUSTERS).asLong(),
                        SizeUnit.UNIT_B,
                        SizeUnit.UNIT_KiB
                    )
                );
            }
        }
        return result;
    }

    public static <T> Map<String, Long> getVgFreeSize(final SpdkCommands<T> spdkCommands, Set<String> volumeGroups)
        throws StorageException, AccessDeniedException
    {
        final Map<String, Long> result = new HashMap<>();

        Iterator<JsonNode> elements = spdkCommands.getJsonElements(spdkCommands.getLvolStores());
        while (elements.hasNext())
        {
            JsonNode element = elements.next();
            if (volumeGroups.contains(element.path(SPDK_NAME).asText()))
            {
                result.put(
                    element.path(SPDK_NAME).asText(),
                    SizeConv.convert(
                        element.path(SPDK_CLUSTER_SIZE).asLong() * element.path(SPDK_FREE_CLUSTERS).asLong(),
                        SizeUnit.UNIT_B,
                        SizeUnit.UNIT_KiB
                    )
                );
            }
        }
        return result;
    }

    public static <T> void checkVgExists(final SpdkCommands<T> spdkCommands, String volumeGroup)
        throws StorageException, AccessDeniedException
    {
        boolean found = false;

        Iterator<JsonNode> elements = spdkCommands.getJsonElements(spdkCommands.getLvolStores());
        while (elements.hasNext())
        {
            JsonNode element = elements.next();

            if (element.path(SPDK_NAME).asText().equals(volumeGroup.trim()))
            {
                found = true;
                break;
            }
        }
        if (!found)
        {
            throw new StorageException("checkVgExists Volume group '" + volumeGroup + "' not found");
        }
    }

    public static <T> String getVgNameFromUuid(SpdkCommands<T> spdkCommands, String volumeGroup)
        throws StorageException, AccessDeniedException
    {
        String vgName = null;
        Iterator<JsonNode> elements = spdkCommands.getJsonElements(spdkCommands.getLvolStores());
        while (elements.hasNext())
        {
            JsonNode element = elements.next();

            if (element.path(SPDK_UUID).asText().equals(volumeGroup.trim()))
            {
                vgName = element.path(SPDK_NAME).asText();
                break;
            }
        }
        if (vgName == null)
        {
            throw new StorageException("getVgNameFromUuid Volume group '" + volumeGroup + "' not found");
        }
        return vgName;
    }

    public static <T> boolean checkTargetExists(final SpdkCommands<T> spdkCommands, String nqn)
        throws StorageException, AccessDeniedException
    {
        boolean targetExists = false;
        Iterator<JsonNode> elements = spdkCommands.getJsonElements(spdkCommands.getNvmfSubsystems());
        while (elements.hasNext() && !targetExists)
        {
            JsonNode element = elements.next();

            if (element.path(SPDK_NQN).asText().equals(nqn))
            {
                targetExists = true;
            }
        }
        return targetExists;
    }

    public static <T> boolean checkNamespaceExists(final SpdkCommands<T> spdkCommands, String nqn, int nsid)
        throws StorageException, AccessDeniedException
    {
        boolean namespaceExists = false;
        Iterator<JsonNode> elements = spdkCommands.getJsonElements(spdkCommands.getNvmfSubsystems());
        while (elements.hasNext() && !namespaceExists)
        {
            JsonNode element = elements.next();

            if (element.path(SPDK_NQN).asText().equals(nqn))
            {
                Iterator<JsonNode> namespaces = element.path(SPDK_NAMESPACES).elements();
                while (namespaces.hasNext() && !namespaceExists)
                {
                    JsonNode namespace = namespaces.next();
                    if (namespace.path(SPDK_NSID).asInt() == nsid)
                    {
                        namespaceExists = true;
                    }
                }
            }
        }
        return namespaceExists;
    }

    public static List<String> lspci(ExtCmd extCmd)
        throws StorageException
    {
        ExtCmd.OutputData outputData = Commands.genericExecutor(
            extCmd,
            new String[]
            {
                "lspci",
                "-mm",
                "-n",
                "-D"
            },
            "Failed to execute lspci",
            "Failed to execute lspci"
        );

        return parseNvmeDrivesAddresses(new String(outputData.stdoutData, StandardCharsets.UTF_8));
    }

    static List<String> parseNvmeDrivesAddresses(String lspciOutput)
    {
        ArrayList<String> lspciEntries = new ArrayList<>();
        for (String line : lspciOutput.split("\n"))
        {
            // Matching NVMe Drives - PCI devices with Class 01, Subclass 08, ProgIf 02
            if (line.trim().matches("(.*)\"0108\"+(.*)-p02+(.*)"))
            {
                // Extracting PCI address
                lspciEntries.add(line.trim().split(" ")[0]);
            }
        }
        return lspciEntries;
    }
}
