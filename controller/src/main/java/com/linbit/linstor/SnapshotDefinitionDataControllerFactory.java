package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionDataDatabaseDriver;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.sql.SQLException;
import java.util.TreeMap;
import java.util.UUID;

public class SnapshotDefinitionDataControllerFactory
{
    private final SnapshotDefinitionDataDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public SnapshotDefinitionDataControllerFactory(
        SnapshotDefinitionDataDatabaseDriver driverRef,
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

    public SnapshotDefinitionData create(
        AccessContext accCtx,
        ResourceDefinition rscDfn,
        SnapshotName snapshotName,
        SnapshotDefinition.SnapshotDfnFlags[] initFlags
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        rscDfn.getObjProt().requireAccess(accCtx, AccessType.USE);

        SnapshotDefinitionData snapshotDfnData = (SnapshotDefinitionData) rscDfn.getSnapshotDfn(accCtx, snapshotName);

        if (snapshotDfnData != null)
        {
            throw new LinStorDataAlreadyExistsException("The SnapshotDefinition already exists");
        }

        snapshotDfnData = new SnapshotDefinitionData(
            UUID.randomUUID(),
            rscDfn,
            snapshotName,
            StateFlagsBits.getMask(initFlags),
            driver,
            transObjFactory,
            propsContainerFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );

        try
        {
            long sequenceNumber = maxSequenceNumber(accCtx, rscDfn) + 1L;
            snapshotDfnData.getProps(accCtx).setProp(
                ApiConsts.KEY_SNAPSHOT_DFN_SEQUENCE_NUMBER, Long.toString(sequenceNumber));
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
                String sequenceNumberProp =
                    snapshotDfn.getProps(accCtx).getProp(ApiConsts.KEY_SNAPSHOT_DFN_SEQUENCE_NUMBER);

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
