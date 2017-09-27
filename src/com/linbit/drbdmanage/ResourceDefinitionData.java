package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsAccess;
import com.linbit.drbdmanage.propscon.PropsContainer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.stateflags.StateFlags;
import com.linbit.drbdmanage.stateflags.StateFlagsBits;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ResourceDefinitionData extends BaseTransactionObject implements ResourceDefinition
{
    // Object identifier
    private final UUID objId;

    // Resource name
    private final ResourceName resourceName;

    // Volumes of the resource
    private final Map<VolumeNumber, VolumeDefinition> volumeMap;

    // Resources defined by this ResourceDefinition
    private final Map<NodeName, Resource> resourceMap;

    // State flags
    private final StateFlags<RscDfnFlags> flags;

    // Object access controls
    private final ObjectProtection objProt;

    // Properties container for this resource definition
    private final Props rscDfnProps;

    private final ResourceDefinitionDataDatabaseDriver dbDriver;

    private boolean deleted = false;

    /*
     * used by getInstance
     */
    private ResourceDefinitionData(
        AccessContext accCtx,
        ResourceName resName,
        long initialFlags,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException
    {
        this(
            UUID.randomUUID(),
            ObjectProtection.getInstance(
                accCtx,
                ObjectProtection.buildPath(resName),
                true,
                transMgr
            ),
            resName,
            initialFlags,
            transMgr
        );
    }

    /*
     * used by database drivers
     */
    ResourceDefinitionData(
        UUID objIdRef,
        ObjectProtection objProtRef,
        ResourceName resName,
        long initialFlags,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        ErrorCheck.ctorNotNull(ResourceDefinitionData.class, ResourceName.class, resName);
        ErrorCheck.ctorNotNull(ResourceDefinitionData.class, ObjectProtection.class, objProtRef);
        objId = objIdRef;
        objProt = objProtRef;
        resourceName = resName;

        dbDriver = DrbdManage.getResourceDefinitionDataDatabaseDriver();

        volumeMap = new TreeMap<>();
        resourceMap = new TreeMap<>();

        rscDfnProps = PropsContainer.getInstance(
            PropsContainer.buildPath(resName),
            transMgr
        );
        flags = new RscDfnFlagsImpl(objProt, this, dbDriver.getStateFlagsPersistence(), initialFlags);

        transObjs = Arrays.asList(
            flags,
            objProt,
            rscDfnProps
        );
    }

    public static ResourceDefinitionData getInstance(
        AccessContext accCtx,
        ResourceName resName,
        RscDfnFlags[] flags,
        TransactionMgr transMgr,
        boolean createIfNotExists
    )
        throws SQLException, AccessDeniedException
    {
        ResourceDefinitionDataDatabaseDriver driver = DrbdManage.getResourceDefinitionDataDatabaseDriver();

        ResourceDefinitionData resDfn = null;
        resDfn = driver.load(resName, false, transMgr);

        if (resDfn == null && createIfNotExists)
        {
            resDfn = new ResourceDefinitionData(
                accCtx,
                resName,
                StateFlagsBits.getMask(flags),
                transMgr
            );
            driver.create(resDfn, transMgr);
        }
        if (resDfn != null)
        {
            resDfn.initialized();
        }
        return resDfn;
    }

    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    @Override
    public ResourceName getName()
    {
        checkDeleted();
        return resourceName;
    }

    @Override
    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, objProt, rscDfnProps);
    }



    synchronized void putVolumeDefinition(AccessContext accCtx, VolumeDefinition volDfn)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        volumeMap.put(volDfn.getVolumeNumber(accCtx), volDfn);
    }

    synchronized void removeVolumeDefinition(AccessContext accCtx, VolumeDefinition volDfn)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        volumeMap.remove(volDfn.getVolumeNumber(accCtx));
    }

    @Override
    public VolumeDefinition getVolumeDfn(AccessContext accCtx, VolumeNumber volNr)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return volumeMap.get(volNr);
    }

    @Override
    public Iterator<VolumeDefinition> iterateVolumeDfn(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return volumeMap.values().iterator();
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return objProt;
    }

    @Override
    public Resource getResource(AccessContext accCtx, NodeName clNodeName)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceMap.get(clNodeName);
    }

    void addResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);

        resourceMap.put(resRef.getAssignedNode().getName(), resRef);
    }

    void removeResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);

        resourceMap.remove(resRef.getAssignedNode().getName());
    }

    @Override
    public StateFlags<RscDfnFlags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CONTROL);

        dbDriver.delete(this, transMgr);
        deleted = true;
    }

    private void checkDeleted()
    {
        if (deleted)
        {
            throw new ImplementationError("Access to deleted node", null);
        }
    }

    private static final class RscDfnFlagsImpl extends StateFlagsBits<ResourceDefinitionData, RscDfnFlags>
    {
        RscDfnFlagsImpl(
            ObjectProtection objProtRef,
            ResourceDefinitionData parent,
            StateFlagsPersistence<ResourceDefinitionData> persistenceRef,
            long initialFlags
        )
        {
            super(objProtRef, parent, StateFlagsBits.getMask(RscDfnFlags.values()), persistenceRef, initialFlags);
        }
    }
}
