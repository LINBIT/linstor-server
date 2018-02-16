package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class ConfFileBuilder
{
    private static final ResourceNameComparator RESOURCE_NAME_COMPARATOR = new ResourceNameComparator();

    private final ErrorReporter errorReporter;
    private final AccessContext accCtx;
    private final Resource localRsc;
    private final Collection<Resource> remoteResources;

    private StringBuilder stringBuilder;
    private int indentDepth;

    public ConfFileBuilder(
        final ErrorReporter errorReporterRef,
        final AccessContext accCtxRef,
        final Resource localRscRef,
        final Collection<Resource> remoteResourcesRef
    )
    {
        errorReporter = errorReporterRef;
        accCtx = accCtxRef;
        localRsc = localRscRef;
        remoteResources = remoteResourcesRef;

        stringBuilder = new StringBuilder();
        indentDepth = 0;
    }

    public String build()
        throws AccessDeniedException
    {
        Set<Resource> peerRscSet = new TreeSet<>(RESOURCE_NAME_COMPARATOR);
        peerRscSet.addAll(remoteResources); // node-alphabetically sorted
        Set<Resource> clients = new TreeSet<>(RESOURCE_NAME_COMPARATOR);
        Set<Resource> satellites = new TreeSet<>(RESOURCE_NAME_COMPARATOR);

        final Set<Set<String>> nodeMeshes = new HashSet<>();
        final Map<String, List<String>> singleConnections = new HashMap<>();

        appendLine("resource %s", localRsc.getDefinition().getName().displayValue);
        try (Section resourceSection = new Section())
        {
            appendLine("net");
            try (Section netSection = new Section())
            {
                // TODO: make configurable
                appendLine("cram-hmac-alg     %s;", "sha1");
                // TODO: make configurable
                appendLine("shared-secret     \"%s\";", localRsc.getDefinition().getSecret(accCtx));
                // TODO: print other "custom" net properties
            }

            // TODO: print options properties

            if (localRsc.getStateFlags().isSet(accCtx, RscFlags.DISKLESS))
            {
                clients.add(localRsc);
            }
            else
            {
                satellites.add(localRsc);
            }

            int port = localRsc.getDefinition().getPort(accCtx).value;
            // Create local network configuration
            {
                NetInterface localNetIf = getPreferredNetIf(localRsc);
                LsIpAddress localAddr = localNetIf.getAddress(accCtx);

                String localAddrText = localAddr.getAddress();
                appendLine("on %s", localRsc.getAssignedNode().getName().displayValue);
                try (Section onSection = new Section())
                {
                    Iterator<Volume> vlmIterator = localRsc.iterateVolumes();
                    while (vlmIterator.hasNext())
                    {
                        appendVlmIfPresent(vlmIterator.next(), accCtx);
                    }
                    if (localAddr.getAddressType() == LsIpAddress.AddrType.IPv6)
                    {
                        appendLine("address    ipv6 [%s]:%d;", localAddrText, port);
                    }
                    else
                    {
                        appendLine("address    ipv4 %s:%d;", localAddrText, port);
                    }
                    appendLine("node-id    %d;", localRsc.getNodeId().value);
                }
            }

            for (final Resource peerRsc : peerRscSet)
            {
                if (peerRsc.getStateFlags().isUnset(accCtx, RscFlags.DELETE))
                {
                    NetInterface peerNetIf = getPreferredNetIf(peerRsc);
                    LsIpAddress peerAddr = peerNetIf.getAddress(accCtx);

                    String peerAddrText = peerAddr.getAddress();
                    appendLine("");
                    appendLine("on %s", peerRsc.getAssignedNode().getName().displayValue);
                    try (Section onSection = new Section())
                    {
                        Iterator<Volume> peerVlms = peerRsc.iterateVolumes();
                        while (peerVlms.hasNext())
                        {
                            appendVlmIfPresent(peerVlms.next(), accCtx);
                        }

                        if (peerAddr.getAddressType() == LsIpAddress.AddrType.IPv6)
                        {
                            appendLine("address     ipv6 [%s]:%d;", peerAddrText, port);
                        }
                        else
                        {
                            appendLine("address     ipv4 %s:%d;", peerAddrText, port);
                        }
                        appendLine("node-id     %s;", peerRsc.getNodeId().value);

                        // TODO: implement "multi-connection / path magic" (nodeMeshes + singleConnections vars)
                        // sb.append(peerResource.co)
                    }
                }
            }

            // TODO: find a better way to generate the connections
            // first generate all with local first
            for (final Resource peerRsc : peerRscSet)
            {
                if (peerRsc.getStateFlags().isUnset(accCtx, RscFlags.DELETE))
                {
                    Node fromNode = localRsc.getAssignedNode();
                    Node toNode = peerRsc.getAssignedNode();

                    String fromHost = fromNode.getName().displayValue;
                    String toHost = toNode.getName().displayValue;

                    int hostNameLen = Math.max(fromHost.length(), toHost.length());
                    appendLine("connection");
                    try (Section connectionSection = new Section())
                    {
                        String format = "host %s;";
                        appendLine(format, fromHost);
                        appendLine(format, toHost);
                    }
                }
            }

//            FIXME: dead code (for connection meshes)
//            if (!nodeMeshes.isEmpty())
//            {
//                appendLine("connection-mesh");
//                try (Section connectionMeshSection = new Section())
//                {
//                    for (final Set<String> mesh : nodeMeshes)
//                    {
//                        appendIndent();
//                        append("hosts");
//                        for (String node : mesh)
//                        {
//                            append(" ");
//                            append(node);
//                        }
//                        appendLine(";");
//                    }
//                }
//            }
//
//            if (!singleConnections.isEmpty())
//            {
//                final Set<Entry<String,List<String>>> entrySet = singleConnections.entrySet();
//                for (final Entry<String, List<String>> entry : entrySet)
//                {
//                    final String source = entry.getKey();
//                    final List<String> targets = entry.getValue();
//                    for (final String target : targets)
//                    {
//                        appendLine("connection");
//                        try (Section connectionSection = new Section())
//                        {
//                            appendLine("host %s;", source);
//                            appendLine("host %s;", target);
//                        }
//                    }
//                }
//            }

        }

        return stringBuilder.toString();
    }

    private NetInterface getPreferredNetIf(Resource rsc)
    {
        NetInterface preferredNetIf = null;
        try
        {
            Volume firstVlm = rsc.iterateVolumes().next();

            PriorityProps prioProps;
            prioProps = new PriorityProps(
                firstVlm.getStorPool(accCtx).getProps(accCtx),
                firstVlm.getProps(accCtx),
                rsc.getProps(accCtx),
                rsc.getAssignedNode().getProps(accCtx)
            );

            String prefNic = prioProps.getProp(ApiConsts.KEY_STOR_POOL_PREF_NIC);

            if (prefNic != null)
            {
                preferredNetIf = rsc.getAssignedNode().getNetInterface(
                    accCtx,
                    new NetInterfaceName(prefNic) // TODO: validate on controller
                );

                if (preferredNetIf == null)
                {
                    errorReporter.logWarning(
                        String.format("Preferred network interface '%s' not found, fallback to default", prefNic)
                    );
                }
            }

            // fallback if preferred couldn't be found
            if (preferredNetIf == null)
            {
                Node assgNode = rsc.getAssignedNode();
                // Try to find the 'default' network interface
                preferredNetIf = assgNode.getNetInterface(accCtx, NetInterfaceName.DEFAULT_NET_INTERFACE_NAME);
                // If there is not even a 'default', use the first one that is found in the node's
                // list of network interfaces
                if (preferredNetIf == null)
                {
                    Iterator<NetInterface> netIfIter = assgNode.iterateNetInterfaces(accCtx);
                    if (netIfIter.hasNext())
                    {
                        preferredNetIf = netIfIter.next();
                    }
                }
            }
        }
        catch (AccessDeniedException | InvalidKeyException | InvalidNameException implError)
        {
            throw new ImplementationError(implError);
        }

        return preferredNetIf;
    }

    private void appendVlmIfPresent(Volume vlm, AccessContext localAccCtx)
        throws AccessDeniedException
    {
        if (vlm.getFlags().isUnset(localAccCtx, Volume.VlmFlags.DELETE, Volume.VlmFlags.CLEAN))
        {
            final String disk;
            if (vlm.getBlockDevicePath(localAccCtx) == null ||
                vlm.getResource().getStateFlags().isSet(localAccCtx, RscFlags.DISKLESS))
            {
                disk = "none";
            }
            else
            {
                String tmpDisk = vlm.getBlockDevicePath(localAccCtx);
                if (tmpDisk.trim().equals(""))
                {
                    disk = "/dev/drbd/this/is/not/used";
                }
                else
                {
                    disk = vlm.getBlockDevicePath(localAccCtx);
                }
            }
            final String metaDisk;
            if (vlm.getMetaDiskPath(localAccCtx) == null)
            {
                metaDisk = "internal";
            }
            else
            {
                String tmpMeta = vlm.getMetaDiskPath(localAccCtx);
                if (tmpMeta.trim().equals(""))
                {
                    metaDisk = "internal";
                }
                else
                {
                    metaDisk = vlm.getMetaDiskPath(localAccCtx);
                }
            }
            appendLine("volume %s", vlm.getVolumeDefinition().getVolumeNumber().value);
            try (Section volumeSection = new Section())
            {
                appendLine("disk        %s;", disk);
                // TODO: print disk properties
                appendLine("meta-disk   %s;", metaDisk);
                appendLine("device      minor %d;",
                    vlm.getVolumeDefinition().getMinorNr(localAccCtx).value
                // TODO: impl and ask storPool for device
                );
                // TODO: add "disk { ... }" section
            }
        }
    }

    private void appendIndent()
    {
        for (int idx = 0; idx < indentDepth; idx++)
        {
            stringBuilder.append("    ");
        }
    }

    private void append(String format, Object... args)
    {
        stringBuilder.append(String.format(format, args));
    }

    private void appendLine(String format, Object... args)
    {
        appendIndent();
        append(format, args);
        stringBuilder.append("\n");
    }

    private static class ResourceNameComparator implements Comparator<Resource>
    {
        @Override
        public int compare(Resource o1, Resource o2)
        {
            return o1.getAssignedNode().getName().compareTo(o2.getAssignedNode().getName());
        }
    }

    /**
     * Allows a section to be expressed using try-with-resources so that it is automatically closed.
     * <p>
     * Non-static to allow access to the indentDepth.
     */
    private class Section implements AutoCloseable
    {
        Section()
        {
            appendLine("{");
            indentDepth++;
        }

        @Override
        public void close()
        {
            indentDepth--;
            appendLine("}");
        }
    }
}
