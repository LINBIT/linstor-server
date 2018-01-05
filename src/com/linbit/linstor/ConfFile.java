package com.linbit.linstor;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

public class ConfFile implements Comparator<Resource>
{
    private StringBuilder stringBuilder = new StringBuilder();

    private ConfFile()
    {
        // for private use only :)
    }

    public static String asConfigFile(
        final AccessContext accCtx,
        final Resource localRsc,
        final Collection<Resource> remoteResources
    )
        throws AccessDeniedException
    {
        ConfFile conf = new ConfFile();

        Set<Resource> peerRscSet = new TreeSet<Resource>(conf);
        peerRscSet.addAll(remoteResources); // node-alphabetically sorted
        Set<Resource> clients = new TreeSet<Resource>(conf);
        Set<Resource> satellites = new TreeSet<Resource>(conf);

        final Set<Set<String>> nodeMeshes = new HashSet<>();
        final Map<String, List<String>> singleConnections = new HashMap<>();

        conf.append("resource %s\n", localRsc.getDefinition().getName().displayValue);
        conf.append("{\n");

        conf.append("    net\n");
        conf.append("    {\n");
        conf.append("        cram-hmac-alg     %s;\n", "sha1"); // TODO: make configurable
        conf.append("        shared-secret     \"%s\";\n", localRsc.getDefinition().getSecret(accCtx)); // TODO: make configurable
        // TODO: print other "custom" net properties
        conf.append("    }\n");

        // TODO: print options properties

        if (localRsc.getStateFlags().isSet(accCtx, RscFlags.DISKLESS))
        {
            clients.add(localRsc);
        }
        else
        {
            satellites.add(localRsc);
        }

        int port =  localRsc.getDefinition().getPort(accCtx).value;
        // Create local network configuration
        {
            Iterator<NetInterface> netIfIter = localRsc.getAssignedNode().iterateNetInterfaces(accCtx);
            while (netIfIter.hasNext())
            {
                NetInterface localNetIf = netIfIter.next();
                String localAddr = localNetIf.getAddress(accCtx).getAddress();
                conf.append("    on %s\n", localRsc.getAssignedNode().getName().displayValue);
                conf.append("    {\n");
                Iterator<Volume> vlmIterator = localRsc.iterateVolumes();
                while (vlmIterator.hasNext())
                {
                    final Volume vlm = vlmIterator.next();
                    if (vlm.getFlags().isUnset(accCtx, Volume.VlmFlags.DELETE))
                    {
                        appendVlm(vlm, accCtx, conf);
                    }
                }
                conf.append("        address    %s:%d;\n", localAddr, port);
                conf.append("        node-id    %d;\n", localRsc.getNodeId().value);
                conf.append("    }\n");
            }
        }

        for (final Resource peerRsc : peerRscSet)
        {
            Iterator<NetInterface> netIfIter = peerRsc.getAssignedNode().iterateNetInterfaces(accCtx);
            if (netIfIter.hasNext())
            {
                NetInterface peerNetIf = netIfIter.next();
                String peerAddr = peerNetIf.getAddress(accCtx).getAddress();
                conf.append("\n");
                conf.append("    on %s\n", peerRsc.getAssignedNode().getName().displayValue);
                conf.append("    {\n");
                Iterator<Volume> peerVlms = peerRsc.iterateVolumes();
                while (peerVlms.hasNext())
                {
                    final Volume peerVlm = peerVlms.next();
                    appendVlm(peerVlm, accCtx, conf);
                }

                conf.append("        address     %s:%d;\n", peerAddr, port);
                conf.append("        node-id     %s;\n", peerRsc.getNodeId().value);

                // TODO: implement "multi-connection / path magic" (nodeMeshes + singleConnections vars)
                // sb.append(peerResource.co)
                conf.append("    }\n");
            }
        }

        {
            // TODO: find a better way to generate the connections
            // first generate all with local first
            for (final Resource peerRsc : peerRscSet)
            {
                Node fromNode = localRsc.getAssignedNode();
                Node toNode = peerRsc.getAssignedNode();

                Iterator<NetInterface> fromIfIter = fromNode.iterateNetInterfaces(accCtx);
                Iterator<NetInterface> toIfIter = toNode.iterateNetInterfaces(accCtx);

                if (fromIfIter.hasNext() && toIfIter.hasNext())
                {
                    NetInterface fromNetIf = fromNode.iterateNetInterfaces(accCtx).next();
                    String fromHost = fromNode.getName().displayValue;
                    String fromAddr = fromNetIf.getAddress(accCtx).getAddress();

                    NetInterface toNetIf = toNode.iterateNetInterfaces(accCtx).next();
                    String toHost = toNode.getName().displayValue;
                    String toAddr = toNetIf.getAddress(accCtx).getAddress();

                    int hostNameLen = Math.max(fromHost.length(), toHost.length());
                    conf.append("    connection\n");
                    conf.append("    {\n");
                    String format = "        host %-" + hostNameLen + "s address %s:%d;\n";
                    conf.append(
                        format,
                        fromHost,
                        fromAddr,
                        port
                    );
                    conf.append(
                        format,
                        toHost,
                        toAddr,
                        port
                    );
                    conf.append("    }\n");
                }
            }
        }

//        if (!nodeMeshes.isEmpty())
//        {
//            conf.append("    connection-mesh\n");
//            conf.append("    {\n");
//            for (final Set<String> mesh : nodeMeshes)
//            {
//                conf.append("        hosts");
//                for (String node : mesh)
//                {
//                    conf.append(" ").append(node);
//                }
//                conf.append(";\n");
//            }
//            conf.append("    }\n");
//        }
//
//        if (!singleConnections.isEmpty())
//        {
//            final Set<Entry<String,List<String>>> entrySet = singleConnections.entrySet();
//            for (final Entry<String, List<String>> entry : entrySet)
//            {
//                final String source = entry.getKey();
//                final List<String> targets = entry.getValue();
//                for (final String target : targets)
//                {
//                    conf.append("    connection\n");
//                    conf.append("    {\n");
//                    conf.append("        host %s;\n", source);
//                    conf.append("        host %s;\n", target);
//                    conf.append("    }\n");
//                }
//            }
//        }

        conf.append("}\n");

        return conf.stringBuilder.toString();
    }

    private static void appendVlm(Volume vlm, AccessContext accCtx, ConfFile conf)
        throws AccessDeniedException
    {
        final String disk;
        if (vlm.getBlockDevicePath(accCtx) == null ||
            vlm.getResource().getStateFlags().isSet(accCtx, RscFlags.DISKLESS)
        )
        {
            disk = "none";
        }
        else
        {
            String tmpDisk = vlm.getBlockDevicePath(accCtx);
            if (tmpDisk.trim().equals(""))
            {
                disk = "/dev/drbd/this/is/not/used";
            }
            else
            {
                disk = vlm.getBlockDevicePath(accCtx);
            }
        }
        final String metaDisk;
        if (vlm.getMetaDiskPath(accCtx) == null)
        {
            metaDisk = "internal";
        }
        else
        {
            String tmpMeta = vlm.getMetaDiskPath(accCtx);
            if (tmpMeta.trim().equals(""))
            {
                metaDisk = "internal";
            }
            else
            {
                metaDisk = vlm.getMetaDiskPath(accCtx);
            }
        }
        conf.append("        volume %s\n", vlm.getVolumeDefinition().getVolumeNumber().value);
        conf.append("        {\n");
        conf.append("            disk        %s;\n", disk);
        // TODO: print disk properties
        conf.append("            meta-disk   %s;\n", metaDisk);
        conf.append("            device      minor %d;\n",
            vlm.getVolumeDefinition().getMinorNr(accCtx).value
            // TODO: impl and ask storPool for device
        );
        // TODO: add "disk { ... }" section
        conf.append("        }\n");

    }

    private ConfFile append(String format, Object... args)
    {
        stringBuilder.append(String.format(format, args));
        return this;
    }

    @Override
    public int compare(Resource o1, Resource o2)
    {
        return o1.getAssignedNode().getName().compareTo(o2.getAssignedNode().getName());
    }
}
