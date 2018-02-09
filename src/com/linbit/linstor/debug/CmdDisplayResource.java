package com.linbit.linstor.debug;

import com.linbit.InvalidNameException;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
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

public class CmdDisplayResource extends BaseDebugCmd
{
    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    private final FilteredObjectLister<ResourceDefinition> lister;

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            FilteredObjectLister.PRM_NAME,
            "Name of the resource(s) to display"
        );
        PARAMETER_DESCRIPTIONS.put(
            FilteredObjectLister.PRM_FILTER_NAME,
            "Filter pattern to apply to the resource name.\n" +
            "Resources with a name matching the pattern will be displayed."
        );
    }

    public CmdDisplayResource()
    {
        super(
            new String[]
            {
                "DspRsc"
            },
            "Display resource(s)",
            "Displays information about one or multiple resource(s)",
            PARAMETER_DESCRIPTIONS,
            null,
            false
        );

        lister = new FilteredObjectLister<>(
            "resource definition",
            "resource",
            new ResourceHandler()
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

    private class ResourceHandler implements FilteredObjectLister.ObjectHandler<ResourceDefinition>
    {
        @Override
        public List<Lock> getRequiredLocks()
        {
            return Arrays.asList(
                cmnDebugCtl.getReconfigurationLock().readLock(),
                cmnDebugCtl.getNodesMapLock().readLock(),
                cmnDebugCtl.getRscDfnMapLock().readLock()
            );
        }

        @Override
        public void ensureSearchAccess(final AccessContext accCtx)
            throws AccessDeniedException
        {
            ObjectProtection rscDfnMapProt = cmnDebugCtl.getRscDfnMapProt();
            if (rscDfnMapProt != null)
            {
                rscDfnMapProt.requireAccess(accCtx, AccessType.VIEW);
            }
        }

        @Override
        public Collection<ResourceDefinition> getAll()
        {
            return cmnDebugCtl.getRscDfnMap().values();
        }

        @Override
        public ResourceDefinition getByName(final String name)
            throws InvalidNameException
        {
            return cmnDebugCtl.getRscDfnMap().get(new ResourceName(name));
        }

        @Override
        public String getName(final ResourceDefinition rscDfn)
        {
            return rscDfn.getName().value;
        }

        @Override
        public void displayObjects(
            final PrintStream output, final ResourceDefinition rscDfn, final AccessContext accCtx
        )
        {
            ResourceName rscName = rscDfn.getName();
            try
            {
                if (rscDfn.getResourceCount() >= 1)
                {
                    Iterator<Resource> rscIter = rscDfn.iterateResource(accCtx);

                    TreePrinter.Builder treeBuilder = TreePrinter.builder(
                        "\u001b[1;37m%-48s\u001b[0m %s",
                        rscName.displayValue, rscDfn.getUuid().toString().toUpperCase()
                    );

                    while (rscIter.hasNext())
                    {
                        Resource rsc = rscIter.next();
                        ObjectProtection rscProt = rsc.getObjProt();
                        Node peerNode = rsc.getAssignedNode();
                        NodeName peerNodeName = peerNode.getName();

                        treeBuilder.branch(peerNodeName.displayValue)
                            .leaf("Resource UUID: %s", rsc.getUuid().toString().toUpperCase())
                            .leaf("Resource volatile UUID: %s", UuidUtils.dbgInstanceIdString(rsc))
                            .leaf("Resource definition UUID: %s", rscDfn.getUuid().toString().toUpperCase())
                            .leaf("Resource definition volatile UUID: %s", UuidUtils.dbgInstanceIdString(rscDfn))
                            .leaf("Node-ID: %d", rsc.getNodeId().value)
                            .leaf("Node UUID: %s", peerNode.getUuid().toString().toUpperCase())
                            .leaf("Node volatile UUID: %s", UuidUtils.dbgInstanceIdString(peerNode));

                        try
                        {
                            long flagsBits = rsc.getStateFlags().getFlagsBits(accCtx);
                            treeBuilder.leaf("Flags: %016X", flagsBits);
                        }
                        catch (AccessDeniedException ignored)
                        {
                        }

                        treeBuilder
                            .leaf(
                                "Creator: %-24s Owner: %-24s",
                                rscProt.getCreator().name.displayValue,
                                rscProt.getOwner().name.displayValue
                            )
                            .leaf("Security type: %s", rscProt.getSecurityType().name.displayValue);

                        treeBuilder.endBranch();
                    }

                    treeBuilder.print(output);
                }
            }
            catch (AccessDeniedException ignored)
            {
                // Not authorized for the resource definition
            }
        }

        @Override
        public int countObjects(final ResourceDefinition rscDfn, final AccessContext accCtx)
        {
            return rscDfn.getResourceCount();
        }
    }
}
