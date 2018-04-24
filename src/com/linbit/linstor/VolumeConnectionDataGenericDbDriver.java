package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.GenericDbDriver;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Triple;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Singleton
public class VolumeConnectionDataGenericDbDriver implements VolumeConnectionDataDatabaseDriver
{
    private static final String TBL_VOL_CON_DFN = DbConstants.TBL_VOLUME_CONNECTIONS;

    private static final String UUID = DbConstants.UUID;
    private static final String NODE_SRC = DbConstants.NODE_NAME_SRC;
    private static final String NODE_DST = DbConstants.NODE_NAME_DST;
    private static final String RES_NAME = DbConstants.RESOURCE_NAME;
    private static final String VOL_NR = DbConstants.VLM_NR;

    private static final String SELECT_ALL =
        " SELECT " + UUID + ", " + NODE_SRC + ", " + NODE_DST + ", " +
            RES_NAME + ", " + VOL_NR +
        " FROM "  + TBL_VOL_CON_DFN;
    private static final String SELECT =
        SELECT_ALL +
        " WHERE " + NODE_SRC + " = ? AND " +
                    NODE_DST + " = ? AND " +
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

    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public VolumeConnectionDataGenericDbDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
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
                        ret = restoreVolumeConnectionData(
                            resultSet,
                            sourceVolume,
                            targetVolume
                        );
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

    public List<VolumeConnectionData> loadAll(
        Map<Triple<NodeName, ResourceName, VolumeNumber>, VolumeData> vlmMap
    )
        throws SQLException
    {
        errorReporter.logTrace("Loading all VolumeConnections");
        List<VolumeConnectionData> vlmConns = new ArrayList<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    NodeName nodeNameSrc = new NodeName(resultSet.getString(NODE_SRC));
                    NodeName nodeNameDst = new NodeName(resultSet.getString(NODE_DST));
                    ResourceName rscName = new ResourceName(resultSet.getString(RES_NAME));
                    VolumeNumber vlmNr = new VolumeNumber(resultSet.getInt(VOL_NR));

                    VolumeConnectionData vlmConn = restoreVolumeConnectionData(
                        resultSet,
                        vlmMap.get(new Triple<>(nodeNameSrc, rscName, vlmNr)),
                        vlmMap.get(new Triple<>(nodeNameDst, rscName, vlmNr))
                    );
                    vlmConns.add(vlmConn);
                }
            }
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(
                TBL_VOL_CON_DFN + " contains invalid name: " + exc.invalidName,
                exc
            );
        }
        catch (ValueOutOfRangeException exc)
        {
            throw new ImplementationError(
                TBL_VOL_CON_DFN + " contains invalid volume number",
                exc
            );
        }
        return vlmConns;
    }

    private VolumeConnectionData restoreVolumeConnectionData(
        ResultSet resultSet,
        Volume sourceVolume,
        Volume targetVolume
    )
        throws SQLException
    {
        return new VolumeConnectionData(
            java.util.UUID.fromString(resultSet.getString(UUID)),
            sourceVolume,
            targetVolume,
            this,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );
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

            stmt.setString(1, conDfnData.getUuid().toString());
            stmt.setString(2, sourceVolume.getResource().getAssignedNode().getName().value);
            stmt.setString(3, targetVolume.getResource().getAssignedNode().getName().value);
            stmt.setString(4, sourceVolume.getResourceDefinition().getName().value);
            stmt.setInt(5, sourceVolume.getVolumeDefinition().getVolumeNumber().value);

            stmt.executeUpdate();

            errorReporter.logTrace("VolumeConnection created %s", getId(conDfnData));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            GenericDbDriver.handleAccessDeniedException(accDeniedExc);
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
            GenericDbDriver.handleAccessDeniedException(accDeniedExc);
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
            GenericDbDriver.handleAccessDeniedException(accDeniedExc);
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
            GenericDbDriver.handleAccessDeniedException(accDeniedException);
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

    private String getVolumeId(String nodeName, String resName, int volNr)
    {
        return "(NodeName=" + nodeName + " ResName=" + resName + " VolNr=" + volNr + ")";
    }
}
