package com.linbit.drbdmanage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;

public class ConfFile
{
    private StringBuilder stringBuilder = new StringBuilder();

    private ConfFile()
    {
        // for private use only :)
    }

    public static String asConfigFile(
        final AccessContext accCtx,
        final Resource resource,
        final VolumeNumber[] volNumbers,
        final Set<Resource> peerResources
    )
        throws AccessDeniedException
    {
        ConfFile conf = new ConfFile();

        final Set<Set<String>> nodeMeshes = new HashSet<>();
        final Map<String, List<String>> singleConnections = new HashMap<>();

        conf.append("resource %s\n", resource.getDefinition().getName().displayValue);
        conf.append("{\n");
        for (VolumeNumber volNr : volNumbers)
        {
            final Volume volume = resource.getVolume(volNr);
            conf.append("    volume %s\n", volNr.value);
            conf.append("    {\n");
            final String disk;
            if (volume.getBlockDevicePath(accCtx) == null)
            {
                disk = "none";
            }
            else
            {
                disk = volume.getBlockDevicePath(accCtx);
            }
            conf.append("        disk        %s;\n", disk);
            final String metaDisk;
            if (volume.getMetaDiskPath(accCtx) == null)
            {
                metaDisk = "internal";
            }
            else
            {
                metaDisk = volume.getMetaDiskPath(accCtx);
            }
            conf.append("        meta-disk   %s;\n", metaDisk);
            conf.append("        device      minor %d;\n", volume.getVolumeDfn().getMinorNr(accCtx).value);
        }
        conf.append("    }\n");
        for (final Resource peerResource : peerResources)
        {
            conf.append("    on %s\n", peerResource.getAssignedNode().getName().value);
            conf.append("    {\n");
            conf.append("        node-id     %s;\n", peerResource.getNodeId().value);
            conf.append("        address     %s;\n"); // TODO: insert node's address here
            // if a node may have multiple addresses / connections... which one to choose here?

            // TODO: implement "multi-connection / path magic" (nodeMeshes + singleConnections vars)
            // sb.append(peerResource.co)
        }
        conf.append("    }\n");

        if (!nodeMeshes.isEmpty())
        {
            conf.append("    connection-mesh\n");
            conf.append("    {\n");
            for (final Set<String> mesh : nodeMeshes)
            {
                conf.append("        hosts");
                for (String node : mesh)
                {
                    conf.append(" ").append(node);
                }
                conf.append(";\n");
            }
            conf.append("    }\n");
        }

        if (!singleConnections.isEmpty())
        {
            final Set<Entry<String,List<String>>> entrySet = singleConnections.entrySet();
            for (final Entry<String, List<String>> entry : entrySet)
            {
                final String source = entry.getKey();
                final List<String> targets = entry.getValue();
                for (final String target : targets)
                {
                    conf.append("    connection\n");
                    conf.append("    {\n");
                    conf.append("        host %s;\n", source);
                    conf.append("        host %s;\n", target);
                    conf.append("    }\n");
                }
            }
        }

        return conf.stringBuilder.toString();
    }

    private ConfFile append(String format, Object... args)
    {
        stringBuilder.append(String.format(format, args));
        return this;
    }
}
