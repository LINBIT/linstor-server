package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import com.linbit.ErrorCheck;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsAccess;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.propscon.SerialPropsContainer;
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
    private UUID objId;

    // Resource name
    private ResourceName resourceName;

    // Connections to the peer resources
    private Map<NodeName, Map<Integer, ConnectionDefinition>> connectionMap;

    // Volumes of the resource
    private Map<VolumeNumber, VolumeDefinition> volumeMap;

    // Resources defined by this ResourceDefinition
    private Map<NodeName, Resource> resourceMap;

    // State flags
    private StateFlags<RscDfnFlags> flags;

    // Object access controls
    private ObjectProtection objProt;

    // Properties container for this resource definition
    private Props rscDfnProps;

    private ResourceDefinitionDataDatabaseDriver dbDriver;

    /*
     * used by getInstance
     */
    private ResourceDefinitionData(
        AccessContext accCtx,
        ResourceName resName,
        SerialGenerator srlGen,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException
    {
        this(
            UUID.randomUUID(),
            ObjectProtection.getInstance(
                accCtx,
                transMgr,
                ObjectProtection.buildPath(resName),
                true
            ),
            resName,
            srlGen,
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
        SerialGenerator serialGen, 
        TransactionMgr transMgr
    )
        throws SQLException
    {
        ErrorCheck.ctorNotNull(ResourceDefinitionData.class, ResourceName.class, resName);
        ErrorCheck.ctorNotNull(ResourceDefinitionData.class, ObjectProtection.class, objProtRef);
        objId = objIdRef;
        objProt = objProtRef;
        resourceName = resName;

        dbDriver = DrbdManage.getResourceDefinitionDataDatabaseDriver(resName);

        connectionMap = new TreeMap<>();
        volumeMap = new TreeMap<>();
        resourceMap = new TreeMap<>();
        
        rscDfnProps = SerialPropsContainer.getInstance(dbDriver.getPropsConDriver(), transMgr, serialGen);
        flags = new RscDfnFlagsImpl(objProt, dbDriver.getStateFlagsPersistence());

        transObjs = Arrays.asList(
            flags,
            objProt,
            rscDfnProps
        );
    }

    public static ResourceDefinitionData getInstance(
        AccessContext accCtx,
        ResourceName resName,
        SerialGenerator serialGen,
        TransactionMgr transMgr,
        boolean createIfNotExists
    )
        throws SQLException, AccessDeniedException
    {
        ResourceDefinitionDataDatabaseDriver driver = DrbdManage.getResourceDefinitionDataDatabaseDriver(resName);

        ResourceDefinitionData resDfn = null;
        if (transMgr != null)
        {
            resDfn = driver.load(transMgr.dbCon, serialGen, transMgr);
        }

        if (resDfn == null)
        {
            if (createIfNotExists)
            {
                resDfn = new ResourceDefinitionData(accCtx, resName, serialGen, transMgr);
                if (transMgr != null)
                {
                    driver.create(transMgr.dbCon, resDfn);
                }
            }
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
    public void addConnection(
        AccessContext accCtx, 
        NodeName nodeName, 
        int conDfnNr,
        ConnectionDefinition conDfn
    )
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.USE);
            
        Map<Integer, ConnectionDefinition> nodeConnMap = connectionMap.get(nodeName);
        if (nodeConnMap == null)
        {
            nodeConnMap = new HashMap<>();
            connectionMap.put(nodeName, nodeConnMap);
        }
        nodeConnMap.put(conDfnNr, conDfn);        
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
    public void putVolumeDefinition(AccessContext accCtx, VolumeDefinition volDfn) 
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.USE);
        volumeMap.put(volDfn.getVolumeNumber(accCtx), volDfn);
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
            super(objProtRef, StateFlagsBits.getMask(RscDfnFlags.values()), persistenceRef);
        }
    }
}
