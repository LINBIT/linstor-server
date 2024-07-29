package com.linbit.linstor.debug;

import com.linbit.AutoIndent;
import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.utils.TreePrinter;
import com.linbit.utils.UuidUtils;

import java.io.PrintStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Supplier;

public class CmdDisplayResourceDfn extends BaseDebugCmd
{
    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    private final FilteredObjectLister<ResourceDefinition> lister;

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            FilteredObjectLister.PRM_NAME,
            "Name of the resource definition(s) to display"
        );
        PARAMETER_DESCRIPTIONS.put(
            FilteredObjectLister.PRM_FILTER_NAME,
            "Filter pattern to apply to the resource definition name.\n" +
            "Resource definitions with a name matching the pattern will be displayed."
        );
    }

    private final ReadWriteLock reconfigurationLock;
    private final ReadWriteLock rscDfnMapLock;
    private final @Nullable Supplier<ObjectProtection> rscDfnMapProt;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;

    public CmdDisplayResourceDfn(
        ReadWriteLock reconfigurationLockRef,
        ReadWriteLock rscDfnMapLockRef,
        @Nullable Supplier<ObjectProtection> rscDfnMapProtRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef
    )
    {
        super(
            new String[]
            {
                "DspRscDfn"
            },
            "Display resource definition(s)",
            "Displays information about one or multiple resource definition(s)",
            PARAMETER_DESCRIPTIONS,
            null
        );

        reconfigurationLock = reconfigurationLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        rscDfnMapProt = rscDfnMapProtRef;
        rscDfnMap = rscDfnMapRef;

        lister = new FilteredObjectLister<>(
            "resource definition",
            "resource definition",
            new ResourceDefinitionHandler()
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

    private class ResourceDefinitionHandler implements FilteredObjectLister.ObjectHandler<ResourceDefinition>
    {
        @Override
        public Lock[] getRequiredLocks()
        {
            return new Lock[] {reconfigurationLock.readLock(), rscDfnMapLock.readLock()};
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
        public String getName(final ResourceDefinition rscDfnRef)
        {
            return rscDfnRef.getName().value;
        }

        @Override
        public void displayObjects(
            final PrintStream output, final ResourceDefinition rscDfnRef, final AccessContext accCtx
        )
        {
            try
            {
                ObjectProtection objProt = rscDfnRef.getObjProt();
                objProt.requireAccess(accCtx, AccessType.VIEW);

                TreePrinter.Builder treeBuilder = TreePrinter
                    .builder(
                        "\u001b[1;37m%-40s\u001b[0m %-36s",
                        rscDfnRef.getName().displayValue,
                        rscDfnRef.getUuid().toString().toUpperCase()
                    )
                    .leaf("Volatile UUID: %s", UuidUtils.dbgInstanceIdString(rscDfnRef))
                    .leaf("Flags: %016x", rscDfnRef.getFlags().getFlagsBits(accCtx))
                    .leaf(
                        "Creator: %-24s Owner: %-24s",
                        objProt.getCreator().name.displayValue,
                        objProt.getOwner().name.displayValue
                    )
                    .leaf("Security type: %-24s", objProt.getSecurityType().name.displayValue);

                byte[] extName = rscDfnRef.getExternalName();
                if (extName != null)
                {
                    String extNameDump = AutoIndent.formatWithIndent(
                        4,
                        HexViewer.binaryToHexDump(extName)
                    );
                    treeBuilder.leaf("External name:\n%s", extNameDump);
                }

                Iterator<VolumeDefinition> vlmDfnIter = rscDfnRef.iterateVolumeDfn(accCtx);

                treeBuilder.branchHideEmpty("Volume definitions");
                while (vlmDfnIter.hasNext())
                {
                    VolumeDefinition vlmDfnRef = vlmDfnIter.next();

                    treeBuilder
                        .branch(
                            "\u001b[1;37mVolume %6d\u001b[0m %-36s",
                            vlmDfnRef.getVolumeNumber().value,
                            vlmDfnRef.getUuid().toString().toUpperCase()
                        )
                        .leaf("Size:     %16d", vlmDfnRef.getVolumeSize(accCtx))
                        .leaf("Flags:    %016x", vlmDfnRef.getFlags().getFlagsBits(accCtx))
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
        public int countObjects(final ResourceDefinition rscDfnRef, final AccessContext accCtx)
        {
            return 1;
        }
    }
}
