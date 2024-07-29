package com.linbit.linstor.debug;

import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
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

    private final ReadWriteLock reconfigurationLock;
    private final ReadWriteLock storPoolDfnMapLock;
    private final @Nullable Supplier<ObjectProtection> storPoolDfnMapProt;
    private final CoreModule.StorPoolDefinitionMap storPoolDfnMap;

    public CmdDisplayStorPoolDfn(
        ReadWriteLock reconfigurationLockRef,
        ReadWriteLock storPoolDfnMapLockRef,
        @Nullable Supplier<ObjectProtection> storPoolDfnMapProtRef,
        CoreModule.StorPoolDefinitionMap storPoolDfnMapRef
    )
    {
        super(
            new String[]
            {
                "DspStorPoolDfn"
            },
            "Display storage pool definition(s)",
            "Displays information about one or multiple storage pool definition(s)",
            PARAMETER_DESCRIPTIONS,
            null
        );

        reconfigurationLock = reconfigurationLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        storPoolDfnMapProt = storPoolDfnMapProtRef;
        storPoolDfnMap = storPoolDfnMapRef;

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
        public Lock[] getRequiredLocks()
        {
            return new Lock[] {reconfigurationLock.readLock(), storPoolDfnMapLock.readLock()};
        }

        @Override
        public void ensureSearchAccess(final AccessContext accCtx)
            throws AccessDeniedException
        {
            if (storPoolDfnMapProt != null)
            {
                storPoolDfnMapProt.get().requireAccess(accCtx, AccessType.VIEW);
            }
        }

        @Override
        public Collection<StorPoolDefinition> getAll()
        {
            return storPoolDfnMap.values();
        }

        @Override
        public @Nullable StorPoolDefinition getByName(final String name)
            throws InvalidNameException
        {
            return storPoolDfnMap.get(new StorPoolName(name));
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

                TreePrinter.Builder treeBuilder = TreePrinter
                    .builder(
                        "\u001b[1;37m%-40s\u001b[0m %-36s",
                        storPoolDfnRef.getName().displayValue,
                        storPoolDfnRef.getUuid().toString().toUpperCase()
                    )
                    .leaf("Volatile UUID: %s", UuidUtils.dbgInstanceIdString(storPoolDfnRef))
                    .leaf(
                        "Creator: %-24s Owner: %-24s",
                        objProt.getCreator().name.displayValue,
                        objProt.getOwner().name.displayValue
                    )
                    .leaf("Security type: %-24s", objProt.getSecurityType().name.displayValue);

                Iterator<StorPool> storPoolIterator = storPoolDfnRef.iterateStorPools(accCtx);

                treeBuilder.branchHideEmpty("Storage pools");
                while (storPoolIterator.hasNext())
                {
                    StorPool storPool = storPoolIterator.next();

                    treeBuilder
                        .branch(
                            "\u001b[1;37mStorage pool\u001b[0m %-36s",
                            storPool.getUuid().toString().toUpperCase()
                        )
                        .leaf("Node name: %s", storPool.getNode().getName().displayValue)
                        .leaf("Node UUID: %s", storPool.getNode().getUuid().toString().toUpperCase())
                        .leaf("Node volatile UUID: %s", UuidUtils.dbgInstanceIdString(storPool.getNode()))
                        .leaf("Driver name: %s", storPool.getDeviceProviderKind())
                        .leaf("Volume count: %d", storPool.getVolumes(accCtx).size())
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
        public int countObjects(final StorPoolDefinition storPoolDfnRef, final AccessContext accCtx)
        {
            return 1;
        }
    }
}
