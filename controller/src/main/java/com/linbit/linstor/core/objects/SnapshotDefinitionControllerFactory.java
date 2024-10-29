package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionDatabaseDriver;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.TreeMap;
import java.util.UUID;

public class SnapshotDefinitionControllerFactory
{
    private final SnapshotDefinitionDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final ObjectProtectionFactory objectProtectionFactory;

    @Inject
    public SnapshotDefinitionControllerFactory(
        SnapshotDefinitionDatabaseDriver driverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        driver = driverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public SnapshotDefinition create(
        AccessContext accCtx,
        ResourceDefinition rscDfn,
        SnapshotName snapshotName,
        SnapshotDefinition.Flags[] initFlags
    )
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        rscDfn.getObjProt().requireAccess(accCtx, AccessType.USE);

        SnapshotDefinition snapshotDfnData = rscDfn.getSnapshotDfn(accCtx, snapshotName);

        if (snapshotDfnData != null)
        {
            throw new LinStorDataAlreadyExistsException("The SnapshotDefinition already exists");
        }

        snapshotDfnData = new SnapshotDefinition(
            UUID.randomUUID(),
            objectProtectionFactory.getInstance(
                accCtx,
                ObjectProtection.buildPath(rscDfn.getName(), snapshotName),
                true
            ),
            rscDfn,
            snapshotName,
            StateFlagsBits.getMask(initFlags),
            driver,
            transObjFactory,
            propsContainerFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>(),
            new TreeMap<>()
        );

        try
        {
            long sequenceNumber = maxSequenceNumber(accCtx, rscDfn) + 1L;
            snapshotDfnData.getSnapDfnProps(accCtx)
                .setProp(ApiConsts.KEY_SNAPSHOT_DFN_SEQUENCE_NUMBER, Long.toString(sequenceNumber));
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError("Internal property not valid", exc);
        }

        driver.create(snapshotDfnData);
        rscDfn.addSnapshotDfn(accCtx, snapshotDfnData);

        return snapshotDfnData;
    }

    public static long maxSequenceNumber(AccessContext accCtx, ResourceDefinition rscDfn)
        throws AccessDeniedException
    {
        long maxSequenceNumber = 0L;
        for (SnapshotDefinition snapshotDfn : rscDfn.getSnapshotDfns(accCtx))
        {
            try
            {
                String sequenceNumberProp = snapshotDfn.getSnapDfnProps(accCtx)
                    .getProp(ApiConsts.KEY_SNAPSHOT_DFN_SEQUENCE_NUMBER);

                long sequenceNumber = Long.valueOf(sequenceNumberProp);
                if (sequenceNumber > maxSequenceNumber)
                {
                    maxSequenceNumber = sequenceNumber;
                }
            }
            catch (InvalidKeyException exc)
            {
                throw new ImplementationError("Internal property not valid", exc);
            }
            catch (NumberFormatException exc)
            {
                throw new ImplementationError(
                    "Unable to parse internal value of internal property " +
                        ApiConsts.KEY_SNAPSHOT_DFN_SEQUENCE_NUMBER,
                    exc
                );
            }
        }
        return maxSequenceNumber;
    }
}
