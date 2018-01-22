package com.linbit.linstor.debug;

import com.linbit.AutoIndent;
import com.linbit.InvalidNameException;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolName;
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

public class CmdDisplayStorPoolDfn extends BaseDebugCmd
{
    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    private final FilteredObjectLister<StorPoolDefinition> lister;

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            FilteredObjectLister.PRM_NAME,
            "Name of the storage pool definition(s) to display"
        );
        PARAMETER_DESCRIPTIONS.put(
            FilteredObjectLister.PRM_FILTER_NAME,
            "Filter pattern to apply to the storage pool definition name.\n" +
            "Storage pool definitions with a name matching the pattern will be displayed."
        );
    }

    public CmdDisplayStorPoolDfn()
    {
        super(
            new String[]
            {
                "DspStorPoolDfn"
            },
            "Display storage pool definition(s)",
            "Displays information about one or multiple storage pool definition(s)",
            PARAMETER_DESCRIPTIONS,
            null,
            false
        );

        lister = new FilteredObjectLister<>(
            "storage pool definition",
            "storage pool definition",
            new StorPoolDefinitionHandler()
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

    private class StorPoolDefinitionHandler implements FilteredObjectLister.ObjectHandler<StorPoolDefinition>
    {
        @Override
        public List<Lock> getRequiredLocks()
        {
            return Arrays.asList(
                cmnDebugCtl.getReconfigurationLock().readLock(),
                cmnDebugCtl.getStorPoolDfnMapLock().readLock()
            );
        }

        @Override
        public void ensureSearchAccess(final AccessContext accCtx)
            throws AccessDeniedException
        {
            ObjectProtection storPoolDfnMapProt = cmnDebugCtl.getStorPoolDfnMapProt();
            if (storPoolDfnMapProt != null)
            {
                storPoolDfnMapProt.requireAccess(accCtx, AccessType.VIEW);
            }
        }

        @Override
        public Collection<StorPoolDefinition> getAll()
        {
            return cmnDebugCtl.getStorPoolDfnMap().values();
        }

        @Override
        public StorPoolDefinition getByName(final String name)
            throws InvalidNameException
        {
            return cmnDebugCtl.getStorPoolDfnMap().get(new StorPoolName(name));
        }

        @Override
        public String getName(final StorPoolDefinition storPoolDfnRef)
        {
            return storPoolDfnRef.getName().value;
        }

        @Override
        public void displayObjects(
            final PrintStream output, final StorPoolDefinition storPoolDfnRef, final AccessContext accCtx
        )
        {
            try
            {
                ObjectProtection objProt = storPoolDfnRef.getObjProt();
                objProt.requireAccess(accCtx, AccessType.VIEW);
                output.printf(
                    "\u001b[1;37m%-40s\u001b[0m %-36s\n" +
                        "%s  Volatile UUID: %s\n" +
                        "%s  Creator: %-24s Owner: %-24s\n" +
                        "%s  Security type: %-24s\n",
                    storPoolDfnRef.getName().displayValue,
                    storPoolDfnRef.getUuid().toString().toUpperCase(),
                    PFX_SUB, UuidUtils.dbgInstanceIdString(storPoolDfnRef),
                    PFX_SUB, objProt.getCreator().name.displayValue,
                    objProt.getOwner().name.displayValue,
                    PFX_SUB_LAST, objProt.getSecurityType().name.displayValue
                );
                Iterator<StorPool> storPoolIterator = storPoolDfnRef.iterateStorPools(accCtx);
                if (storPoolIterator.hasNext())
                {
                    AutoIndent.printWithIndent(output, 4, "Storage pools");
                }
                while (storPoolIterator.hasNext())
                {
                    StorPool storPool = storPoolIterator.next();
                    String itemPfx;
                    String treePfx;
                    if (storPoolIterator.hasNext())
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
                            "%s  \u001b[1;37mStorage pool\u001b[0m %-36s\n" +
                                "%s  %s  Node name: %s\n" +
                                "%s  %s  Node UUID: %s\n" +
                                "%s  %s  Node volatile UUID: %s\n" +
                                "%s  %s  Driver name: %s\n" +
                                "%s  %s  Volume count: %d\n",
                            itemPfx, storPool.getUuid().toString().toUpperCase(),
                            treePfx, PFX_SUB, storPool.getNode().getName().displayValue,
                            treePfx, PFX_SUB, storPool.getNode().getUuid().toString().toUpperCase(),
                            treePfx, PFX_SUB, UuidUtils.dbgInstanceIdString(storPool.getNode()),
                            treePfx, PFX_SUB, storPool.getDriverName(),
                            treePfx, PFX_SUB_LAST, storPool.getVolumes(accCtx).size()
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
        public int countObjects(final StorPoolDefinition storPoolDfnRef, final AccessContext accCtx)
        {
            return 1;
        }
    }
}
