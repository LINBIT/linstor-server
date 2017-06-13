package com.linbit.drbdmanage;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.TransactionMap;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDefinitionDatabaseDriver;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsAccess;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.propscon.SerialPropsContainer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.ObjectProtection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import com.linbit.drbdmanage.stateflags.StateFlags;
import com.linbit.drbdmanage.stateflags.StateFlagsBits;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ResourceDefinitionData implements ResourceDefinition
{
    // Object identifier
    private UUID objId;

    // Resource name
    private ResourceName resourceName;

    // Connections to the peer resources
    private TransactionMap<NodeName, Map<Integer, ConnectionDefinition>> connectionMap;

    // Volumes of the resource
    private TransactionMap<VolumeNumber, VolumeDefinition> volumeMap;

    // Resources defined by this ResourceDefinition
    private TransactionMap<NodeName, Resource> resourceMap;

    // State flags
    private StateFlags<RscDfnFlags> flags;

    // Object access controls
    private ObjectProtection objProt;

    // Properties container for this resource definition
    private Props rscDfnProps;

    private ResourceDefinitionDatabaseDriver dbDriver;

    private ResourceDefinitionData(
        AccessContext accCtx,
        ResourceName resName,
        SerialGenerator srlGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        ErrorCheck.ctorNotNull(ResourceDefinitionData.class, ResourceName.class, resName);
        objId = UUID.randomUUID();
        resourceName = resName;

        dbDriver = DrbdManage.getResourceDefinitionDatabaseDriver(this);

        connectionMap = new TransactionMap<>(
            new TreeMap<NodeName, Map<Integer, ConnectionDefinition>>(),
            dbDriver.getConnectionMapDriver()
        );
        volumeMap = new TransactionMap<>(
            new TreeMap<VolumeNumber, VolumeDefinition>(),
            dbDriver.getVolumeMapDriver()
        );
        resourceMap = new TransactionMap<>(
            new TreeMap<NodeName, Resource>(),
            dbDriver.getResourceMapDriver()
        );
        rscDfnProps = SerialPropsContainer.createRootContainer(srlGen);
        objProt = ObjectProtection.load(
            transMgr,
            ObjectProtection.buildPath(this),
            true,
            accCtx
        );
        flags = new RscDfnFlagsImpl(objProt, dbDriver.getStateFlagsPersistence());
    }

    public static ResourceDefinitionData create(
        AccessContext accCtx,
        ResourceName resName,
        SerialGenerator srlGen
    )
        throws SQLException
    {
        return create(accCtx, resName, srlGen, null);
    }

    public static ResourceDefinitionData create(
        AccessContext accCtx,
        ResourceName resName,
        SerialGenerator srlGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        return new ResourceDefinitionData(accCtx, resName, srlGen, transMgr);
    }

    public static ResourceDefinitionData load(
        AccessContext accCtx,
        ResourceName resName,
        TransactionMgr transMgr
    )

    {
        // TODO: implement ResourceDefinitionData.load(...)
        return null;
    }

    @Override
    public UUID getUuid()
    {
        return objId;
    }

    @Override
    public ResourceName getName()
    {
        return resourceName;
    }

    @Override
    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        return PropsAccess.secureGetProps(accCtx, objProt, rscDfnProps);
    }

    @Override
    public ConnectionDefinition getConnectionDfn(AccessContext accCtx, NodeName clNodeName, Integer connNr)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        ConnectionDefinition connDfn = null;
        Map<Integer, ConnectionDefinition> nodeConnMap = connectionMap.get(clNodeName);
        if (nodeConnMap != null)
        {
            connDfn = nodeConnMap.get(connNr);
        }
        return connDfn;
    }

    @Override
    public VolumeDefinition getVolumeDfn(AccessContext accCtx, VolumeNumber volNr)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return volumeMap.get(volNr);
    }

    @Override
    public Iterator<VolumeDefinition> iterateVolumeDfn(AccessContext accCtx)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return volumeMap.values().iterator();
    }

    @Override
    public ObjectProtection getObjProt()
    {
        return objProt;
    }

    @Override
    public Resource getResource(AccessContext accCtx, NodeName clNodeName)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceMap.get(clNodeName);
    }

    @Override
    public void addResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.USE);

        resourceMap.put(resRef.getAssignedNode().getName(), resRef);
    }

    @Override
    public void removeResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.USE);

        resourceMap.remove(resRef.getAssignedNode().getName());
    }

    @Override
    public StateFlags<RscDfnFlags> getFlags()
    {
        return flags;
    }

    private static final class RscDfnFlagsImpl extends StateFlagsBits<RscDfnFlags>
    {
        RscDfnFlagsImpl(ObjectProtection objProtRef, StateFlagsPersistence persistenceRef)
        {
            super(objProtRef, StateFlagsBits.getMask(RscDfnFlags.ALL_FLAGS), persistenceRef);
        }
    }

    @Override
    public void setConnection(TransactionMgr transMgr) throws ImplementationError
    {
        transMgr.register(this);
        dbDriver.setConnection(transMgr.dbCon);
    }

    @Override
    public void commit()
    {
        connectionMap.commit();
        volumeMap.commit();
        resourceMap.commit();
        flags.commit();
        objProt.commit();
    }

    @Override
    public void rollback()
    {
        connectionMap.rollback();
        volumeMap.rollback();
        resourceMap.rollback();
        flags.rollback();
        objProt.rollback();
    }

    @Override
    public boolean isDirty()
    {
        return connectionMap.isDirty() ||
            volumeMap.isDirty() ||
            resourceMap.isDirty() ||
            flags.isDirty() ||
            objProt.isDirty();
    }
}
