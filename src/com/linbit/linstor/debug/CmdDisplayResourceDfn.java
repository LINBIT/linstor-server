package com.linbit.linstor.debug;

import com.linbit.AutoIndent;
import com.linbit.InvalidNameException;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.utils.UuidUtils;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;

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

    public CmdDisplayResourceDfn()
    {
        super(
            new String[]
            {
                "DspRscDfn"
            },
            "Display resource definition(s)",
            "Displays information about one or multiple resource definition(s)",
            PARAMETER_DESCRIPTIONS,
            null,
            false
        );

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
        public List<Lock> getRequiredLocks()
        {
            return Arrays.asList(
                cmnDebugCtl.getReconfigurationLock().readLock(),
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
                output.printf(
                    "\u001b[1;37m%-40s\u001b[0m %-36s\n" +
                        "%s  Volatile UUID: %s\n" +
                        "%s  Flags: %016x\n" +
                        "%s  Creator: %-24s Owner: %-24s\n" +
                        "%s  Security type: %-24s\n",
                    rscDfnRef.getName().displayValue,
                    rscDfnRef.getUuid().toString().toUpperCase(),
                    PFX_SUB, UuidUtils.dbgInstanceIdString(rscDfnRef),
                    PFX_SUB, rscDfnRef.getFlags().getFlagsBits(accCtx),
                    PFX_SUB, objProt.getCreator().name.displayValue,
                    objProt.getOwner().name.displayValue,
                    PFX_SUB_LAST, objProt.getSecurityType().name.displayValue
                );
                Iterator<VolumeDefinition> vlmDfnIter = rscDfnRef.iterateVolumeDfn(accCtx);
                if (vlmDfnIter.hasNext())
                {
                    output.println("    Volume definitions");
                }
                while (vlmDfnIter.hasNext())
                {
                    VolumeDefinition vlmDfnRef = vlmDfnIter.next();
                    String itemPfx;
                    String treePfx;
                    if (vlmDfnIter.hasNext())
                    {
                        itemPfx = PFX_SUB;
                        treePfx = PFX_VLINE;
                    }
                    else
                    {
                        itemPfx = PFX_SUB_LAST;
                        treePfx = "  ";
                    }
                    AutoIndent.printWithIndent(
                        output, 4,
                        String.format(
                            "%s  \u001b[1;37mVolume %6d\u001b[0m %-36s\n" +
                                "%s  %s  Size:     %16d\n" +
                                "%s  %s  Minor Nr: %16d\n" +
                                "%s  %s  Flags:    %016x\n",
                            itemPfx, vlmDfnRef.getVolumeNumber().value,
                            vlmDfnRef.getUuid().toString().toUpperCase(),
                            treePfx, PFX_SUB, vlmDfnRef.getVolumeSize(accCtx),
                            treePfx, PFX_SUB, vlmDfnRef.getMinorNr(accCtx).value,
                            treePfx, PFX_SUB_LAST, vlmDfnRef.getFlags().getFlagsBits(accCtx)
                        )
                    );
                }
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
