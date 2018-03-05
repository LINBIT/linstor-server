package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.DerbyDriver;
import com.linbit.linstor.dbdrivers.derby.DerbyConstants;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.UuidUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Singleton
public class VolumeConnectionDataDerbyDriver implements VolumeConnectionDataDatabaseDriver
{
    private static final String TBL_VOL_CON_DFN = DerbyConstants.TBL_VOLUME_CONNECTIONS;

    private static final String UUID = DerbyConstants.UUID;
    private static final String NODE_SRC = DerbyConstants.NODE_NAME_SRC;
    private static final String NODE_DST = DerbyConstants.NODE_NAME_DST;
    private static final String RES_NAME = DerbyConstants.RESOURCE_NAME;
    private static final String VOL_NR = DerbyConstants.VLM_NR;

    private static final String SELECT =
        " SELECT " + UUID + ", " + NODE_SRC + ", " + NODE_DST + ", " +
                     RES_NAME + ", " + VOL_NR +
        " FROM "  + TBL_VOL_CON_DFN +
        " WHERE " + NODE_SRC + " = ? AND " +
                    NODE_DST + " = ? AND " +
                    RES_NAME + " = ? AND " +
                    VOL_NR + " = ?";
    private static final String SELECT_BY_VOL_SRC_OR_DST =
        " SELECT " + UUID + ", " + NODE_SRC + ", " + NODE_DST + ", " +
                     RES_NAME + ", " + VOL_NR +
        " FROM "  + TBL_VOL_CON_DFN +
        " WHERE (" +
                        NODE_SRC + " = ? OR " +
                        NODE_DST + " = ? " +
               ") AND " +
                    RES_NAME + " = ? AND " +
                    VOL_NR + " = ?";

    private static final String INSERT =
        " INSERT INTO " + TBL_VOL_CON_DFN +
        " (" +
            UUID + ", " + NODE_SRC + ", " + NODE_DST + ", " +
            RES_NAME + ", " + VOL_NR +
        ") VALUES (?, ?, ?, ?, ?)";

    private static final String DELETE =
        " DELETE FROM " + TBL_VOL_CON_DFN +
         " WHERE " + NODE_SRC + " = ? AND " +
                     NODE_DST + " = ? AND " +
                     RES_NAME + " = ? AND " +
                     VOL_NR + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final Provider<NodeDataDerbyDriver> nodeDataDerbyDriverProvider;
    private final Provider<ResourceDefinitionDataDerbyDriver> resourceDefinitionDataDerbyDriverProvider;
    private final Provider<ResourceDataDerbyDriver> resourceDataDerbyDriverProvider;
    private final Provider<VolumeDefinitionDataDerbyDriver> volumeDefinitionDataDerbyDriverProvider;
    private final Provider<VolumeDataDerbyDriver> volumeDataDerbyDriverProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public VolumeConnectionDataDerbyDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        Provider<NodeDataDerbyDriver> nodeDataDerbyDriverProviderRef,
        Provider<ResourceDefinitionDataDerbyDriver> resourceDefinitionDataDerbyDriverProviderRef,
        Provider<ResourceDataDerbyDriver> resourceDataDerbyDriverProviderRef,
        Provider<VolumeDefinitionDataDerbyDriver> volumeDefinitionDataDerbyDriverProviderRef,
        Provider<VolumeDataDerbyDriver> volumeDataDerbyDriverProviderRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        nodeDataDerbyDriverProvider = nodeDataDerbyDriverProviderRef;
        resourceDefinitionDataDerbyDriverProvider = resourceDefinitionDataDerbyDriverProviderRef;
        resourceDataDerbyDriverProvider = resourceDataDerbyDriverProviderRef;
        volumeDefinitionDataDerbyDriverProvider = volumeDefinitionDataDerbyDriverProviderRef;
        volumeDataDerbyDriverProvider = volumeDataDerbyDriverProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public VolumeConnectionData load(
        Volume sourceVolume,
        Volume targetVolume,
        boolean logWarnIfNotExists
    )
        throws SQLException
    {
        ResourceDefinition resDfn = sourceVolume.getResource().getDefinition();
        VolumeDefinition volDfn = sourceVolume.getVolumeDefinition();
        if (volDfn != targetVolume.getVolumeDefinition() ||
            resDfn != targetVolume.getResource().getDefinition())
        {
            throw new ImplementationError(
                String.format(
                    "Failed to load VolumeConnection between unrelated volumes. %s %s",
                    getVolumeTraceId(sourceVolume),
                    getVolumeTraceId(targetVolume)
                ),
                null
            );
        }

        errorReporter.logTrace(
            "Loading VolumeConnection %s",
            getId(sourceVolume, targetVolume)
        );

        Node sourceNode = sourceVolume.getResource().getAssignedNode();
        Node targetNode = targetVolume.getResource().getAssignedNode();

        VolumeConnectionData ret = cacheGet(
            sourceNode,
            targetNode,
            resDfn,
            volDfn
        );

        if (ret == null)
        {
            try (PreparedStatement stmt = getConnection().prepareStatement(SELECT))
            {
                stmt.setString(1, sourceNode.getName().value);
                stmt.setString(2, targetNode.getName().value);
                stmt.setString(3, resDfn.getName().value);
                stmt.setInt(4, volDfn.getVolumeNumber().value);

                try (ResultSet resultSet = stmt.executeQuery())
                {
                    if (resultSet.next())
                    {
                        ret = restoreVolumeConnectionData(resultSet);
                        errorReporter.logTrace(
                            "VolumeConnection loaded %s",
                            getId(ret)
                        );
                    }
                    else
                    if (logWarnIfNotExists)
                    {
                        errorReporter.logWarning(
                            "VolumeConnection not found in the DB %s",
                            getId(sourceVolume, targetVolume)
                        );
                    }
                }
            }
        }
        return ret;
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public List<VolumeConnectionData> loadAllByVolume(Volume volume)
        throws SQLException
    {
        errorReporter.logTrace(
            "Loading all VolumeConnections for Volume %s",
            getVolumeTraceId(volume)
        );

        List<VolumeConnectionData> connections = new ArrayList<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_BY_VOL_SRC_OR_DST))
        {
            NodeName nodeName = volume.getResource().getAssignedNode().getName();
            stmt.setString(1, nodeName.value);
            stmt.setString(2, nodeName.value);
            stmt.setString(3, volume.getResourceDefinition().getName().value);
            stmt.setInt(4, volume.getVolumeDefinition().getVolumeNumber().value);

            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    VolumeConnectionData conDfn = restoreVolumeConnectionData(resultSet);
                    connections.add(conDfn);
                }
            }
        }

        errorReporter.logTrace(
            "%d VolumeConnections loaded for Resource %s",
            connections.size(),
            getVolumeDebugId(volume)
        );
        return connections;
    }

    private VolumeConnectionData restoreVolumeConnectionData(ResultSet resultSet)
        throws SQLException
    {
        NodeName sourceNodeName = null;
        NodeName targetNodeName = null;
        ResourceName resourceName = null;
        VolumeNumber volNr = null;
        try
        {
            sourceNodeName = new NodeName(resultSet.getString(NODE_SRC));
            targetNodeName = new NodeName(resultSet.getString(NODE_DST));
            resourceName = new ResourceName(resultSet.getString(RES_NAME));
            volNr = new VolumeNumber(resultSet.getInt(VOL_NR));
        }
        catch (InvalidNameException invalidNameExc)
        {
            String col;
            String format = "A %s of a stored VolumeConnection in table %s could not be restored. ";
            if (sourceNodeName == null)
            {
                format += "(invalid SourceNodeName=%s, TargetNodeName=%s, ResName=%s)";
                col = "SourceNodeName";
            }
            else
            if (targetNodeName == null)
            {
                format += "(SourceNodeName=%s, invalid TargetNodeName=%s, ResName=%s)";
                col = "TargetNodeName";
            }
            else
            {
                format += "(SourceNodeName=%s, TargetNodeName=%s, invalid ResName=%s)";
                col = "ResName";
            }
            throw new LinStorSqlRuntimeException(
                String.format(
                    format,
                    col,
                    TBL_VOL_CON_DFN,
                    resultSet.getString(NODE_SRC),
                    resultSet.getString(NODE_DST),
                    resultSet.getString(RES_NAME),
                    resultSet.getInt(VOL_NR)
                ),
                invalidNameExc
            );
        }
        catch (ValueOutOfRangeException invalidNameExc)
        {
            throw new LinStorSqlRuntimeException(
                String.format(
                    "A VolumeNumber of a stored VolumeConnection in table %s could not be restored. " +
                        "(SourceNodeName=%s, TargetNodeName=%s, ResName=%s, invalid VolNr=%d)",
                    TBL_VOL_CON_DFN,
                    resultSet.getString(NODE_SRC),
                    resultSet.getString(NODE_DST),
                    resultSet.getString(RES_NAME),
                    resultSet.getInt(VOL_NR)
                ),
                invalidNameExc
            );
        }

        NodeDataDerbyDriver nodeDataDerbyDriver = nodeDataDerbyDriverProvider.get();
        Node sourceNode = nodeDataDerbyDriver.load(sourceNodeName, true);
        Node targetNode = nodeDataDerbyDriver.load(targetNodeName, true);

        ResourceDefinition resourceDefinition = resourceDefinitionDataDerbyDriverProvider.get().load(
            resourceName,
            true
        );
        VolumeDefinition volumeDfn = volumeDefinitionDataDerbyDriverProvider.get()
            .load(resourceDefinition, volNr, true);

        ResourceDataDerbyDriver resourceDataDerbyDriver = resourceDataDerbyDriverProvider.get();
        Resource sourceResource = resourceDataDerbyDriver.load(sourceNode, resourceName, true);
        Resource targetResource = resourceDataDerbyDriver.load(targetNode, resourceName, true);

        VolumeDataDerbyDriver volumeDataDerbyDriver = volumeDataDerbyDriverProvider.get();
        Volume sourceVolume = volumeDataDerbyDriver.load(sourceResource, volumeDfn, true);
        Volume targetVolume = volumeDataDerbyDriver.load(targetResource, volumeDfn, true);

        VolumeConnectionData ret = null;
        try
        {
            ret = cacheGet(
                sourceNode,
                targetNode,
                sourceVolume.getResourceDefinition(),
                sourceVolume.getVolumeDefinition()
            );
            if (ret == null)
            {
                UUID uuid = UuidUtils.asUuid(resultSet.getBytes(UUID));

                ret = new VolumeConnectionData(
                    uuid,
                    dbCtx,
                    sourceVolume,
                    targetVolume,
                    this,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
        return ret;
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(VolumeConnectionData conDfnData) throws SQLException
    {
        errorReporter.logTrace("Creating VolumeConnection %s", getId(conDfnData));

        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT))
        {
            Volume sourceVolume = conDfnData.getSourceVolume(dbCtx);
            Volume targetVolume = conDfnData.getTargetVolume(dbCtx);

            stmt.setBytes(1, UuidUtils.asByteArray(conDfnData.getUuid()));
            stmt.setString(2, sourceVolume.getResource().getAssignedNode().getName().value);
            stmt.setString(3, targetVolume.getResource().getAssignedNode().getName().value);
            stmt.setString(4, sourceVolume.getResourceDefinition().getName().value);
            stmt.setInt(5, sourceVolume.getVolumeDefinition().getVolumeNumber().value);

            stmt.executeUpdate();

            errorReporter.logTrace("VolumeConnection created %s", getId(conDfnData));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void delete(VolumeConnectionData conDfnData) throws SQLException
    {
        errorReporter.logTrace("Deleting VolumeConnection %s", getId(conDfnData));

        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE))
        {
            Volume sourceVolume = conDfnData.getSourceVolume(dbCtx);
            Volume targetVolume = conDfnData.getTargetVolume(dbCtx);

            stmt.setString(1, sourceVolume.getResource().getAssignedNode().getName().value);
            stmt.setString(2, targetVolume.getResource().getAssignedNode().getName().value);
            stmt.setString(3, sourceVolume.getResourceDefinition().getName().value);
            stmt.setInt(4, sourceVolume.getVolumeDefinition().getVolumeNumber().value);

            stmt.executeUpdate();

            errorReporter.logTrace("VolumeConnection deleted %s", getId(conDfnData));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
    }

    private VolumeConnectionData cacheGet(
        Node nodeSrc,
        Node nodeDst,
        ResourceDefinition resDfn,
        VolumeDefinition volDfn
    )
    {
        VolumeConnectionData ret = null;
        try
        {
            Resource resSrc = resDfn.getResource(dbCtx, nodeSrc.getName());
            Resource resDst = resDfn.getResource(dbCtx, nodeDst.getName());
            if (resSrc != null && resDst != null)
            {
                Volume volSrc = resSrc.getVolume(volDfn.getVolumeNumber());
                Volume volDst = resDst.getVolume(volDfn.getVolumeNumber());

                if (volSrc != null && volDst != null)
                {
                    ret = (VolumeConnectionData) volSrc.getVolumeConnection(dbCtx, volDst);
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
        return ret;
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(VolumeConnectionData volConData)
    {
        String id = null;
        try
        {
            Volume sourceVolume = volConData.getSourceVolume(dbCtx);
            Volume targetVolume = volConData.getTargetVolume(dbCtx);
            id = getId(
                sourceVolume.getResource().getAssignedNode().getName().displayValue,
                targetVolume.getResource().getAssignedNode().getName().displayValue,
                sourceVolume.getResourceDefinition().getName().displayValue,
                sourceVolume.getVolumeDefinition().getVolumeNumber().value
            );
        }
        catch (AccessDeniedException accDeniedException)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedException);
        }
        return id;
    }

    private String getId(Volume sourceVolume, Volume targetVolume)
    {
        return getId(
            sourceVolume.getResource().getAssignedNode().getName().displayValue,
            targetVolume.getResource().getAssignedNode().getName().displayValue,
            sourceVolume.getResource().getDefinition().getName().displayValue,
            sourceVolume.getVolumeDefinition().getVolumeNumber().value
        );
    }

    private String getId(
        String sourceName,
        String targetName,
        String resName,
        int volNr
    )
    {
        return "(SourceNode=" + sourceName +
            " TargetNode=" + targetName +
            " ResName=" + resName +
            " VolNr=" + volNr + ")";
    }

    private String getVolumeTraceId(Volume volume)
    {
        return getVolumeId(
            volume.getResource().getAssignedNode().getName().value,
            volume.getResourceDefinition().getName().value,
            volume.getVolumeDefinition().getVolumeNumber().value
        );
    }

    private String getVolumeDebugId(Volume volume)
    {
        return getVolumeId(
            volume.getResource().getAssignedNode().getName().displayValue,
            volume.getResourceDefinition().getName().displayValue,
            volume.getVolumeDefinition().getVolumeNumber().value
        );
    }

    private String getVolumeId(String nodeName, String resName, int volNr)
    {
        return "(NodeName=" + nodeName + " ResName=" + resName + " VolNr=" + volNr + ")";
    }
}
