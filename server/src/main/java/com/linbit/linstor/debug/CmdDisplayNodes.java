package com.linbit.linstor.debug;

import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.utils.TreePrinter;
import com.linbit.utils.UuidUtils;

import java.io.PrintStream;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Supplier;

public class CmdDisplayNodes extends BaseDebugCmd
{
    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

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

    private final ReadWriteLock reconfigurationLock;
    private final ReadWriteLock nodesMapLock;
    private final @Nullable Supplier<ObjectProtection> nodesMapProt;
    private final CoreModule.NodesMap nodesMap;

    private final FilteredObjectLister<Node> lister;

    public CmdDisplayNodes(
        ReadWriteLock reconfigurationLockRef,
        ReadWriteLock nodesMapLockRef,
        @Nullable Supplier<ObjectProtection> nodesMapProtRef,
        CoreModule.NodesMap nodesMapRef
    )
    {
        super(
            new String[]
            {
                "DspNode"
            },
            "Display node(s)",
            "Displays information about one or multiple nodes",
            PARAMETER_DESCRIPTIONS,
            null
        );

        reconfigurationLock = reconfigurationLockRef;
        nodesMapLock = nodesMapLockRef;
        nodesMapProt = nodesMapProtRef;
        nodesMap = nodesMapRef;

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
        public Lock[] getRequiredLocks()
        {
            return new Lock[] {reconfigurationLock.readLock(), nodesMapLock.readLock()};
        }

        @Override
        public void ensureSearchAccess(final AccessContext accCtx)
            throws AccessDeniedException
        {
            if (nodesMapProt != null)
            {
                nodesMapProt.get().requireAccess(accCtx, AccessType.VIEW);
            }
        }

        @Override
        public Collection<Node> getAll()
        {
            return nodesMap.values();
        }

        @Override
        public @Nullable Node getByName(final String name)
            throws InvalidNameException
        {
            return nodesMap.get(new NodeName(name));
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

                treeBuilder.branchHideEmpty("Network interfaces:");
                nodeRef.streamNetInterfaces(accCtx).forEach(netIf ->
                {
                    String address = "<No authorized>";
                    try
                    {
                        LsIpAddress lsIp = netIf.getAddress(accCtx);
                        String addrStr = lsIp.getAddress();
                        address = addrStr;
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
                );

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
