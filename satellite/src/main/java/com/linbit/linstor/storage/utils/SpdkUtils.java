package com.linbit.linstor.storage.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.storage.StorageException;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
        public final String thinPool;
        public final String identifier;
        public final String path;
        public final long size;
        public final float dataPercent;
        public final String attributes;

        LvsInfo(
            String volumeGroupRef,
            String thinPoolRef,
            String identifierRef,
            String pathRef,
            long sizeRef,
            float dataPercentRef,
            String attributesRef
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

    public static HashMap<String, LvsInfo> getLvsInfo(
        final ExtCmd ec,
        final Set<String> volumeGroups
    )
        throws StorageException
    {
        final HashMap<String, LvsInfo> infoByIdentifier = new HashMap<>();

        Iterator<JsonNode> elements = getJsonElements(SpdkCommands.lvs(ec));
        while (elements.hasNext())
        {
            JsonNode element = elements.next();

            if (element.path(SPDK_PRODUCK_NAME).asText().equals(SPDK_LOGICAL_VOLUME))
            {
                final String vgStr = SpdkUtils.getVgNameFromUuid(
                    ec,
                    element.path(SPDK_DRIVER_SPECIFIC).path(SPDK_LVOL).path(SPDK_LVOL_STORE_UUID).asText()
                );
                if (volumeGroups.contains(vgStr))
                {
                    Iterator<JsonNode> aliases = element.path(SPDK_ALIASES).elements();
                    String path_alias = "";

                    while (aliases.hasNext())
                    {
                        JsonNode alias = aliases.next();
                        path_alias = alias.asText();
                    }

                    final String identifier = path_alias.split("/")[1]; // 0 is Volume Group, 1 is Logical Volume
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


    public static Map<String, Long> getExtentSize(ExtCmd extCmd, Set<String> volumeGroups) throws StorageException
    {
        final Map<String, Long> result = new HashMap<>();

        Iterator<JsonNode> elements = getJsonElements(SpdkCommands.getLvolStores(extCmd));
        while (elements.hasNext())
        {
            JsonNode element = elements.next();
            if (volumeGroups.contains(element.path(SPDK_NAME).asText()))
            {
                result.put(
                    element.path(SPDK_NAME).asText(),
                    element.path(SPDK_BLOCK_SIZE).asLong() //in KiB
                );
            }
        }
        return result;
    }

    public static Long getBlockSizeByName(ExtCmd extCmd, String name) throws StorageException
    {
        Iterator<JsonNode> elements = getJsonElements(SpdkCommands.lvsByName(extCmd, name));
        while (elements.hasNext())
        {
            JsonNode element = elements.next();
            return element.path(SPDK_BLOCK_SIZE).asLong() * element.path(SPDK_NUM_BLOCKS).asLong();
        }
        throw new StorageException("Volume not found: " + name);
    }

    public static Map<String, Long> getVgTotalSize(ExtCmd extCmd, Set<String> volumeGroups) throws StorageException
    {
        final Map<String, Long> result = new HashMap<>();

        Iterator<JsonNode> elements = getJsonElements(SpdkCommands.getLvolStores(extCmd));
        while (elements.hasNext())
        {
            JsonNode element = elements.next();
            if (volumeGroups.contains(element.path(SPDK_NAME).asText()))
            {
                result.put(
                    element.path(SPDK_NAME).asText(),
                    element.path(SPDK_BLOCK_SIZE).asLong() * element.path(SPDK_TOTAL_DATA_CLUSTERS).asLong() // KiB
                );
            }
        }
        return result;
    }

    public static Map<String, Long> getVgFreeSize(ExtCmd extCmd, Set<String> volumeGroups) throws StorageException
    {
        final Map<String, Long> result = new HashMap<>();

        Iterator<JsonNode> elements = getJsonElements(SpdkCommands.getLvolStores(extCmd));
        while (elements.hasNext())
        {
            JsonNode element = elements.next();
            if (volumeGroups.contains(element.path(SPDK_NAME).asText()))
            {
                result.put(
                    element.path(SPDK_NAME).asText(),
                    element.path(SPDK_BLOCK_SIZE).asLong() * element.path(SPDK_FREE_CLUSTERS).asLong() //bytes to KiB
                );
            }
        }
        return result;
    }

    public static void checkVgExists(ExtCmd extCmd, String volumeGroup) throws StorageException
    {
        boolean found = false;

        Iterator<JsonNode> elements = getJsonElements(SpdkCommands.getLvolStores(extCmd));
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

    public static String getVgNameFromUuid(ExtCmd extCmd, String volumeGroup) throws StorageException
    {
        String vgName = null;
        Iterator<JsonNode> elements = getJsonElements(SpdkCommands.getLvolStores(extCmd));
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

    public static boolean checkTargetExists(ExtCmd extCmd, String nqn) throws StorageException
    {
        boolean targetExists = false;
        Iterator<JsonNode> elements = getJsonElements(SpdkCommands.getNvmfSubsystems(extCmd));
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

    public static boolean checkNamespaceExists(ExtCmd extCmd, String nqn, int nsid) throws StorageException
    {
        boolean namespaceExists = false;
        Iterator<JsonNode> elements = getJsonElements(SpdkCommands.getNvmfSubsystems(extCmd));
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

    private static Iterator<JsonNode> getJsonElements(OutputData output)
        throws StorageException
    {
        JsonNode rootNode = null;
        ObjectMapper objectMapper = new ObjectMapper();

        try
        {
            rootNode = objectMapper.readTree(output.stdoutData);
        }
        catch (IOException ioExc)
        {
            throw new StorageException("I/O error while parsing SPDK response");
        }

        return rootNode.elements();
    }
}
