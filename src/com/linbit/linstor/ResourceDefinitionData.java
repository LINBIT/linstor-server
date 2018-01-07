package com.linbit.linstor;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.UUID;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMap;
import com.linbit.TransactionMgr;
import com.linbit.TransactionSimpleObject;
import com.linbit.linstor.api.pojo.RscDfnPojo;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import java.util.ArrayList;
import java.util.Map;

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

    // Tcp Port
    private final TransactionSimpleObject<ResourceDefinitionData, TcpPortNumber> port;

    // Volumes of the resource
    private final TransactionMap<VolumeNumber, VolumeDefinition> volumeMap;

    // Resources defined by this ResourceDefinition
    private final TransactionMap<NodeName, Resource> resourceMap;

    // State flags
    private final StateFlags<RscDfnFlags> flags;

    private final TransactionSimpleObject<ResourceDefinitionData, TransportType> transportType;

    // Object access controls
    private final ObjectProtection objProt;

    // Properties container for this resource definition
    private final Props rscDfnProps;

    private final String secret;

    private final ResourceDefinitionDataDatabaseDriver dbDriver;

    private boolean deleted = false;


    /*
     * used by getInstance
     */
    private ResourceDefinitionData(
        AccessContext accCtx,
        ResourceName resName,
        TcpPortNumber port,
        long initialFlags,
        String secret,
        TransportType transType,
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
            port,
            initialFlags,
            secret,
            transType,
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
        TcpPortNumber portRef,
        long initialFlags,
        String secretRef,
        TransportType transTypeRef,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        ErrorCheck.ctorNotNull(ResourceDefinitionData.class, ResourceName.class, resName);
        ErrorCheck.ctorNotNull(ResourceDefinitionData.class, ObjectProtection.class, objProtRef);
        objId = objIdRef;
        objProt = objProtRef;
        resourceName = resName;
        secret = secretRef;

        dbDriver = LinStor.getResourceDefinitionDataDatabaseDriver();

        port = new TransactionSimpleObject<>(this, portRef, dbDriver.getPortDriver());
        volumeMap = new TransactionMap<>(new TreeMap<VolumeNumber, VolumeDefinition>(), null);
        resourceMap = new TransactionMap<>(new TreeMap<NodeName, Resource>(), null);

        rscDfnProps = PropsContainer.getInstance(
            PropsContainer.buildPath(resName),
            transMgr
        );
        flags = new RscDfnFlagsImpl(objProt, this, dbDriver.getStateFlagsPersistence(), initialFlags);

        transportType = new TransactionSimpleObject<>(this, transTypeRef, dbDriver.getTransportTypeDriver());

        transObjs = Arrays.asList(
            flags,
            objProt,
            volumeMap,
            resourceMap,
            rscDfnProps,
            port,
            transportType
        );
    }

    public static ResourceDefinitionData getInstance(
        AccessContext accCtx,
        ResourceName resName,
        TcpPortNumber port,
        RscDfnFlags[] flags,
        String secret,
        TransportType transType,
        TransactionMgr transMgr,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        ResourceDefinitionDataDatabaseDriver driver = LinStor.getResourceDefinitionDataDatabaseDriver();

        ResourceDefinitionData resDfn = null;
        resDfn = driver.load(resName, false, transMgr);

        if (failIfExists && resDfn != null)
        {
            throw new LinStorDataAlreadyExistsException("The ResourceDefinition already exists");
        }

        if (resDfn == null && createIfNotExists)
        {
            resDfn = new ResourceDefinitionData(
                accCtx,
                resName,
                port,
                StateFlagsBits.getMask(flags),
                secret,
                transType,
                transMgr
            );
            driver.create(resDfn, transMgr);
        }
        if (resDfn != null)
        {
            resDfn.initialized();
            resDfn.setConnection(transMgr);
        }
        return resDfn;
    }

    public static ResourceDefinitionData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        ResourceName rscName,
        TcpPortNumber portRef,
        RscDfnFlags[] initFlags,
        String secret,
        TransportType transType,
        SatelliteTransactionMgr transMgr
    )
        throws ImplementationError
    {
        ResourceDefinitionDataDatabaseDriver driver = LinStor.getResourceDefinitionDataDatabaseDriver();
        ResourceDefinitionData rscDfn = null;
        try
        {
            rscDfn = driver.load(rscName, false, transMgr);
            if (rscDfn == null)
            {
                rscDfn = new ResourceDefinitionData(
                    uuid,
                    ObjectProtection.getInstance(accCtx, "", false, transMgr),
                    rscName,
                    portRef,
                    StateFlagsBits.getMask(initFlags),
                    secret,
                    transType,
                    transMgr
                );
            }
            rscDfn.initialized();
            rscDfn.setConnection(transMgr);
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return rscDfn;
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
        volumeMap.put(volDfn.getVolumeNumber(), volDfn);
    }

    synchronized void removeVolumeDefinition(AccessContext accCtx, VolumeDefinition volDfn)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        volumeMap.remove(volDfn.getVolumeNumber());
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
    public int getResourceCount()
    {
        return resourceMap.size();
    }

    @Override
    public Iterator<Resource> iterateResource(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceMap.values().iterator();
    }

    @Override
    public void copyResourceMap(
        AccessContext accCtx, Map<? super NodeName, ? super Resource> dstMap
    )
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        dstMap.putAll(resourceMap);
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

    @Override
    public TcpPortNumber getPort(AccessContext accCtx) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return port.get();
    }

    @Override
    public void setPort(AccessContext accCtx, TcpPortNumber port) throws AccessDeniedException, SQLException
    {
        objProt.requireAccess(accCtx, AccessType.USE);
        this.port.set(port);
    }

    public void addResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
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
    public String getSecret(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return secret;
    }

    @Override
    public TransportType getTransportType(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return transportType.get();
    }

    @Override
    public void setTransportType(AccessContext accCtx, TransportType type)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        transportType.set(type);
    }

    @Override
    public void markDeleted(AccessContext accCtx) throws AccessDeniedException, SQLException
    {
        getFlags().enableFlags(accCtx, RscDfnFlags.DELETE);
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

    @Override
    public ResourceDefinition.RscDfnApi getApiData(AccessContext accCtx)
        throws AccessDeniedException
    {
        ArrayList<VolumeDefinition.VlmDfnApi> vlmDfnList = new ArrayList<>();
        Iterator<VolumeDefinition> vlmDfnIter = iterateVolumeDfn(accCtx);
        while (vlmDfnIter.hasNext())
        {
            VolumeDefinition vd = vlmDfnIter.next();
            vlmDfnList.add(vd.getApiData(accCtx));
        }
        return new RscDfnPojo(
            getUuid(),
            getName().getDisplayName(),
            getPort(accCtx).value,
            getSecret(accCtx),
            getFlags().getFlagsBits(accCtx),
            getTransportType(accCtx).name(),
            getProps(accCtx).map(),
            vlmDfnList
        );
    }

    @Override
    public String toString()
    {
        return "Rsc: '" + resourceName + "'";
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
