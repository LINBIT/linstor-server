package com.linbit.linstor.debug;

import com.linbit.InvalidNameException;
import com.linbit.linstor.LsIpAddress;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.utils.TreePrinter;
import com.linbit.utils.UuidUtils;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;

public class CmdDisplayNodes extends BaseDebugCmd
{
    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    private final FilteredObjectLister<Node> lister;

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            FilteredObjectLister.PRM_NAME,
            "Name of the node(s) to display"
        );
        PARAMETER_DESCRIPTIONS.put(
            FilteredObjectLister.PRM_FILTER_NAME,
            "Filter pattern to apply to the node name.\n" +
            "Nodes with a name matching the pattern will be displayed."
        );
    }

    public CmdDisplayNodes()
    {
        super(
            new String[]
            {
                "DspNode"
            },
            "Display node(s)",
            "Displays information about one or multiple nodes",
            PARAMETER_DESCRIPTIONS,
            null,
            false
        );

        lister = new FilteredObjectLister<>(
            "node",
            "node",
            new NodeHandler()
        );
    }

    @Override
    public void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        AccessContext accCtx,
        Map<String, String> parameters
    )
        throws Exception
    {
        lister.execute(debugOut, debugErr, accCtx, parameters);
    }

    private class NodeHandler implements FilteredObjectLister.ObjectHandler<Node>
    {
        @Override
        public List<Lock> getRequiredLocks()
        {
            return Arrays.asList(
                cmnDebugCtl.getReconfigurationLock().readLock(),
                cmnDebugCtl.getNodesMapLock().readLock()
            );
        }

        @Override
        public void ensureSearchAccess(final AccessContext accCtx)
            throws AccessDeniedException
        {
            ObjectProtection nodesMapProt = cmnDebugCtl.getNodesMapProt();
            if (nodesMapProt != null)
            {
                nodesMapProt.requireAccess(accCtx, AccessType.VIEW);
            }
        }

        @Override
        public Collection<Node> getAll()
        {
            return cmnDebugCtl.getNodesMap().values();
        }

        @Override
        public Node getByName(final String name)
            throws InvalidNameException
        {
            return cmnDebugCtl.getNodesMap().get(new NodeName(name));
        }

        @Override
        public String getName(final Node nodeRef)
        {
            return nodeRef.getName().value;
        }

        @Override
        public void displayObjects(final PrintStream output, final Node nodeRef, final AccessContext accCtx)
        {
            try
            {
                ObjectProtection objProt = nodeRef.getObjProt();
                objProt.requireAccess(accCtx, AccessType.VIEW);

                TreePrinter.Builder treeBuilder = TreePrinter
                    .builder(
                        "\u001b[1;37m%-40s\u001b[0m %-36s",
                        nodeRef.getName().displayValue,
                        nodeRef.getUuid().toString().toUpperCase()
                    )
                    .leaf("Volatile UUID: %s", UuidUtils.dbgInstanceIdString(nodeRef))
                    .leaf("Flags: %016x", nodeRef.getFlags().getFlagsBits(accCtx))
                    .leaf("Creator: %-24s Owner: %-24s",
                        objProt.getCreator().name.displayValue, objProt.getOwner().name.displayValue)
                    .leaf("Security type: %-24s", objProt.getSecurityType().name.displayValue);

                Iterator<NetInterface> netIfIter = nodeRef.iterateNetInterfaces(accCtx);

                treeBuilder.branchHideEmpty("Network interfaces:");
                while (netIfIter.hasNext())
                {
                    NetInterface netIf = netIfIter.next();
                    String address = "<No authorized>";
                    try
                    {
                        LsIpAddress lsIp = netIf.getAddress(accCtx);
                        if (lsIp != null)
                        {
                            String addrStr = lsIp.getAddress();
                            if (addrStr != null)
                            {
                                address = addrStr;
                            }
                        }
                    }
                    catch (AccessDeniedException ignored)
                    {
                    }

                    treeBuilder
                        .branch(
                            "\u001b[1;37m%-24s\u001b[0m %s",
                            netIf.getName().displayValue,
                            netIf.getUuid().toString().toUpperCase()
                        )
                        .leaf("Address: %s", address)
                        .endBranch();
                }
                treeBuilder.endBranch();

                treeBuilder.print(output);
            }
            catch (AccessDeniedException accExc)
            {
                // No view access
            }
        }

        @Override
        public int countObjects(final Node nodeRef, final AccessContext accCtx)
        {
            return 1;
        }
    }
}
