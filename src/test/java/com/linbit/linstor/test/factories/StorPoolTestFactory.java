package com.linbit.linstor.test.factories;

import com.linbit.InvalidNameException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.apicallhandler.controller.exceptions.IllegalStorageDriverException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.FreeSpaceMgrControllerFactory;
import com.linbit.linstor.core.objects.FreeSpaceTracker;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolControllerFactory;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.TestAccessContextProvider;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.HashMap;

@Singleton
public class StorPoolTestFactory
{
    private final NodeTestFactory nodeFact;
    private final StorPoolDefinitionTestFactory storPoolDfnFact;
    private final StorPoolControllerFactory fact;
    private final FreeSpaceMgrControllerFactory fsmFact;

    private final HashMap<Pair<String, String>, StorPool> storPoolMap = new HashMap<>();

    private AccessContext dfltAccCtx = TestAccessContextProvider.PUBLIC_CTX;
    private DeviceProviderKind dfltDriverKind = DeviceProviderKind.LVM;

    @Inject
    public StorPoolTestFactory(
        NodeTestFactory nodeFactRef,
        StorPoolDefinitionTestFactory storPoolDfnFactRef,
        StorPoolControllerFactory factRef,
        FreeSpaceMgrControllerFactory fsmFactRef
    )
    {
        nodeFact = nodeFactRef;
        storPoolDfnFact = storPoolDfnFactRef;
        fact = factRef;
        fsmFact = fsmFactRef;
    }

    public StorPool get(String nodeName, String storPoolName, boolean createIfNotExists)
        throws AccessDeniedException, DatabaseException, LinStorDataAlreadyExistsException,
        IllegalStorageDriverException, InvalidNameException
    {
        StorPool sp = storPoolMap.get(new Pair<>(nodeName.toUpperCase(), storPoolName.toUpperCase()));
        if (sp == null && createIfNotExists)
        {
            sp = create(nodeName, storPoolName);
        }
        return sp;
    }

    public StorPoolTestFactory setDfltAccCtx(AccessContext dfltAccCtxRef)
    {
        dfltAccCtx = dfltAccCtxRef;
        return this;
    }

    public StorPoolTestFactory setDfltDriverKind(DeviceProviderKind dfltDriverKindRef)
    {
        dfltDriverKind = dfltDriverKindRef;
        return this;
    }

    public StorPool create(Node node, StorPoolDefinition storPoolDfn)
        throws AccessDeniedException, DatabaseException, LinStorDataAlreadyExistsException, InvalidNameException,
        IllegalStorageDriverException
    {
        return builder(node, storPoolDfn).build();
    }

    public StorPool create(Node node, String storPoolName)
        throws AccessDeniedException, DatabaseException, LinStorDataAlreadyExistsException,
        IllegalStorageDriverException, InvalidNameException
    {
        return builder(node.getName().displayValue, storPoolName).build();
    }

    public StorPool create(String nodeName, String storPoolName)
        throws AccessDeniedException, DatabaseException, LinStorDataAlreadyExistsException, InvalidNameException,
        IllegalStorageDriverException
    {
        return builder(nodeName, storPoolName).build();
    }

    public StorPoolBuilder builder(Node node, StorPoolDefinition storPoolDfn)
        throws AccessDeniedException, DatabaseException, InvalidNameException
    {
        return new StorPoolBuilder(node.getName().displayValue, storPoolDfn.getName().displayValue);
    }

    public StorPoolBuilder builder(Node node, String storPoolName)
        throws AccessDeniedException, DatabaseException, InvalidNameException
    {
        return new StorPoolBuilder(node.getName().displayValue, storPoolName);
    }

    public StorPoolBuilder builder(String nodeName, String storPoolName)
        throws AccessDeniedException, DatabaseException, InvalidNameException
    {
        return new StorPoolBuilder(nodeName, storPoolName);
    }

    public class StorPoolBuilder
    {
        private AccessContext accCtx;
        private String nodeName;
        private String storPoolName;
        private DeviceProviderKind driverKind;
        private FreeSpaceTracker freeSpaceTracker;
        private boolean externalLocking;

        public StorPoolBuilder(String nodeNameRef, String storPoolNameRef)
            throws AccessDeniedException, DatabaseException, InvalidNameException
        {
            nodeName = nodeNameRef;
            storPoolName = storPoolNameRef;

            accCtx = dfltAccCtx;
            driverKind = dfltDriverKind;
            freeSpaceTracker = fsmFact.getInstance(
                accCtx,
                new SharedStorPoolName(new NodeName(nodeNameRef), new StorPoolName(storPoolNameRef))
            );
        }

        public StorPoolBuilder setStorPoolName(String storPoolNameRef)
        {
            storPoolName = storPoolNameRef;
            return this;
        }

        public StorPoolBuilder setNodeName(String nodeNameRef)
        {
            nodeName = nodeNameRef;
            return this;
        }

        public StorPoolBuilder setDriverKind(DeviceProviderKind driverKindRef)
        {
            driverKind = driverKindRef;
            return this;
        }

        public StorPoolBuilder setAccCtx(AccessContext accCtxRef)
        {
            accCtx = accCtxRef;
            return this;
        }

        public StorPoolBuilder setFreeSpaceMgrName(String fsmName)
            throws AccessDeniedException, DatabaseException, InvalidNameException
        {
            freeSpaceTracker = fsmFact.getInstance(accCtx, new SharedStorPoolName(fsmName));
            return this;
        }

        public StorPoolBuilder setExternalLocking(boolean extLockingRef)
        {
            externalLocking = extLockingRef;
            return this;
        }

        public StorPool build()
            throws AccessDeniedException, DatabaseException, LinStorDataAlreadyExistsException, InvalidNameException,
            IllegalStorageDriverException
        {
            StorPool storPool = fact.create(
                accCtx,
                nodeFact.get(nodeName, true),
                storPoolDfnFact.get(storPoolName, true),
                driverKind,
                freeSpaceTracker,
                externalLocking
            );
            storPoolMap.put(new Pair<>(nodeName.toUpperCase(), storPoolName.toUpperCase()), storPool);
            return storPool;
        }
    }
}
