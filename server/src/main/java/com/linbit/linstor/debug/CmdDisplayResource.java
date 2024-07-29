package com.linbit.linstor.debug;

import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.utils.TreePrinter;
import com.linbit.utils.UuidUtils;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Supplier;

public class CmdDisplayResource extends BaseDebugCmd
{
    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

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

    private final ReadWriteLock reconfigurationLock;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final @Nullable Supplier<ObjectProtection> rscDfnMapProt;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;

    private final FilteredObjectLister<ResourceDefinition> lister;

    public CmdDisplayResource(
        ReadWriteLock reconfigurationLockRef,
        ReadWriteLock nodesMapLockRef,
        ReadWriteLock rscDfnMapLockRef,
        @Nullable Supplier<ObjectProtection> rscDfnMapProtRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef
    )
    {
        super(
            new String[]
            {
                "DspRsc"
            },
            "Display resource(s)",
            "Displays information about one or multiple resource(s)",
            PARAMETER_DESCRIPTIONS,
            null
        );

        reconfigurationLock = reconfigurationLockRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        rscDfnMapProt = rscDfnMapProtRef;
        rscDfnMap = rscDfnMapRef;

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
        public Lock[] getRequiredLocks()
        {
            return new Lock[]
            {
                reconfigurationLock.readLock(),
                nodesMapLock.readLock(),
                rscDfnMapLock.readLock()
            };
        }

        @Override
        public void ensureSearchAccess(final AccessContext accCtx)
            throws AccessDeniedException
        {
            if (rscDfnMapProt != null)
            {
                rscDfnMapProt.get().requireAccess(accCtx, AccessType.VIEW);
            }
        }

        @Override
        public Collection<ResourceDefinition> getAll()
        {
            return rscDfnMap.values();
        }

        @Override
        public @Nullable ResourceDefinition getByName(final String name)
            throws InvalidNameException
        {
            return rscDfnMap.get(new ResourceName(name));
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
                        Node peerNode = rsc.getNode();
                        NodeName peerNodeName = peerNode.getName();

                        treeBuilder.branch(peerNodeName.displayValue)
                            .leaf("Resource UUID: %s", rsc.getUuid().toString().toUpperCase())
                            .leaf("Resource volatile UUID: %s", UuidUtils.dbgInstanceIdString(rsc))
                            .leaf("Resource definition UUID: %s", rscDfn.getUuid().toString().toUpperCase())
                            .leaf("Resource definition volatile UUID: %s", UuidUtils.dbgInstanceIdString(rscDfn))
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

                        Iterator<Volume> vlmIter = rsc.iterateVolumes();
                        while (vlmIter.hasNext())
                        {
                            Volume vlm = vlmIter.next();
                            VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
                            List<String> storPoolNames = new ArrayList<>();
                            {
                                Set<StorPool> vlmStorPools = LayerVlmUtils.getStorPoolSet(vlm, accCtx, true);
                                for (StorPool storPool : vlmStorPools)
                                {
                                    storPoolNames.add(storPool.getName().displayValue);
                                }
                            }
                            treeBuilder.branch("Volume %d", vlmDfn.getVolumeNumber().value)
                                .leaf("Volume UUID: %s", vlm.getUuid().toString().toUpperCase())
                                .leaf("Volume definition UUID: %s", vlmDfn.getUuid().toString().toUpperCase())
                                .leaf("Size:     %16d", vlmDfn.getVolumeSize(accCtx))
                                .leaf("Flags:    %016x", vlm.getFlags().getFlagsBits(accCtx))
                                .leaf("Storage pool%s: %s",
                                    storPoolNames.size() == 1 ? "" : "s",
                                    storPoolNames.size() == 1 ? storPoolNames.get(0) : storPoolNames.toString()
                                )
                                .endBranch();
                        }

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
