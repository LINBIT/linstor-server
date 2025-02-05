package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.apicallhandler.controller.exceptions.IllegalStorageDriverException;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDatabaseDriver;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.TreeMap;
import java.util.UUID;

public class StorPoolControllerFactory
{
    private static final String ALLOC_GRAN_1GIB_IN_KIB = Long.toString(
        SizeConv.convert(1, SizeUnit.UNIT_GiB, SizeUnit.UNIT_KiB)
    );
    private static final String ALLOC_GRAN_1MIB_IN_KIB = Long.toString(
        SizeConv.convert(1, SizeUnit.UNIT_MiB, SizeUnit.UNIT_KiB)
    );
    private static final String ALLOC_GRAN_4MIB_IN_KIB = Long.toString(
        SizeConv.convert(4, SizeUnit.UNIT_MiB, SizeUnit.UNIT_KiB)
    );
    private static final String ALLOC_GRAN_1KIB = "1";
    private static final String ALLOC_GRAN_8KIB = "8";
    private static final String ALLOC_GRAN_64KIB = "64";

    private final StorPoolDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public StorPoolControllerFactory(
        StorPoolDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        driver = driverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public StorPool create(
        AccessContext accCtx,
        Node node,
        StorPoolDefinition storPoolDef,
        DeviceProviderKind deviceProviderKindRef,
        FreeSpaceTracker freeSpaceTrackerRef,
        boolean externalLockingRef
    )
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException,
        IllegalStorageDriverException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);
        StorPool storPool = null;

        storPool = node.getStorPool(accCtx, storPoolDef.getName());

        if (storPool != null)
        {
            throw new LinStorDataAlreadyExistsException("The StorPool already exists");
        }

        Node.Type nodeType = node.getNodeType(accCtx);
        if (!nodeType.isDeviceProviderKindAllowed(deviceProviderKindRef))
        {
            throw new IllegalStorageDriverException(
                "Illegal storage driver.",
                "The current node type '" + nodeType.name() + "' does not support the " +
                "storage driver '" + deviceProviderKindRef + "'.",
                null,
                "The allowed storage drivers for the current node type are:\n   " +
                nodeType.getAllowedKindClasses(),
                null
            );
        }
        storPool = new StorPool(
            UUID.randomUUID(),
            node,
            storPoolDef,
            deviceProviderKindRef,
            freeSpaceTrackerRef,
            externalLockingRef,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );
        driver.create(storPool);
        node.addStorPool(accCtx, storPool);
        storPoolDef.addStorPool(accCtx, storPool);

        setDefaultProps(accCtx, storPool);

        return storPool;
    }

    private void setDefaultProps(AccessContext accCtxRef, StorPool storPoolRef)
        throws AccessDeniedException, DatabaseException
    {
        try
        {
            Props props = storPoolRef.getProps(accCtxRef);
            String dfltAllocGran = null;
            switch (storPoolRef.getDeviceProviderKind())
            {
                case DISKLESS: // fall-through
                case EBS_INIT:
                    // does not need special props
                    break;
                case EBS_TARGET:
                    dfltAllocGran = ALLOC_GRAN_1GIB_IN_KIB;
                    break;
                case EXOS: // fall-through
                case LVM: // fall-through
                case LVM_THIN:
                    dfltAllocGran = ALLOC_GRAN_4MIB_IN_KIB;
                    break;
                case FILE: // fall-through
                case FILE_THIN:
                    dfltAllocGran = ALLOC_GRAN_1KIB;
                    break;
                case REMOTE_SPDK:
                    dfltAllocGran = ALLOC_GRAN_1KIB; // its actually 1B, but linstor calculates in KiB...
                    break;
                case SPDK:
                    dfltAllocGran = ALLOC_GRAN_1MIB_IN_KIB;
                    break;
                case STORAGE_SPACES: // fall-through
                case STORAGE_SPACES_THIN:
                    dfltAllocGran = ALLOC_GRAN_64KIB;
                    break;
                case ZFS:
                case ZFS_THIN:
                    dfltAllocGran = ALLOC_GRAN_8KIB;
                    break;
                case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
                default:
                    throw new ImplementationError(
                        "Unhandled device provider kind: " + storPoolRef.getDeviceProviderKind()
                    );

            }

            if (dfltAllocGran != null)
            {
                props.setProp(
                    InternalApiConsts.ALLOCATION_GRANULARITY,
                    dfltAllocGran,
                    StorageConstants.NAMESPACE_INTERNAL
                );

            }
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
    }
}
