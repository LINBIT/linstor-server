package com.linbit.linstor.dbcp.migration;

import com.linbit.crypto.SecretGenerator;
import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.modularcrypto.ModularCryptoProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Migration(
    version = "2019.04.10.08.47",
    description = "Fixing incorrect Migration LayerData of resources with no volumes"
)
@SuppressWarnings("checkstyle:magicnumber")
public class Migration_2019_04_10_Fix_LayerData_NoVolumes extends LinstorMigration
{
    private static final int DEFAULT_START_PORT = 7000;
    private static final int DEFAULT_DRBD_AL_STRIPES = 1;
    private static final long DEFAULT_DRBD_AL_STRIPE_SIZE = 32;
    private static final int DEFAULT_DRBD_PEER_SLOTS = 7;

    private static final String SELECT_ALL_LAYER_RESOURCE_IDS =
        " SELECT LAYER_RESOURCE_ID, NODE_NAME, RESOURCE_NAME " +
        " FROM LAYER_RESOURCE_IDS";
    private static final String SELECT_ALL_RSCS =
        " SELECT NODE_NAME, RESOURCE_NAME, RESOURCE_FLAGS " +
        " FROM RESOURCES";
    private static final String SELECT_SINGLE_PROP =
        "SELECT PROP_VALUE " +
        "FROM PROPS_CONTAINERS " +
        "WHERE PROPS_INSTANCE = ? AND " +
               "PROP_KEY = ?";
    private static final String SELECT_HIGHEST_DRBD_TCP_PORT =
        "SELECT MAX(TCP_PORT) AS MAX_TCP_PORT " +
        "FROM LAYER_DRBD_RESOURCE_DEFINITIONS";

    private static final String INSERT_LAYER_RSC_ID =
        "INSERT INTO LAYER_RESOURCE_IDS " +
        "(LAYER_RESOURCE_ID, NODE_NAME, RESOURCE_NAME," +
        " LAYER_RESOURCE_KIND, LAYER_RESOURCE_PARENT_ID, LAYER_RESOURCE_SUFFIX) " +
        "VALUES (?, ?, ?, ?, ?, ?)";
    private static final String INSERT_DRBD_RSC_DFN =
        "INSERT INTO LAYER_DRBD_RESOURCE_DEFINITIONS " +
        "(RESOURCE_NAME, RESOURCE_NAME_SUFFIX, PEER_SLOTS, AL_STRIPES, " +
        " AL_STRIPE_SIZE, TCP_PORT, TRANSPORT_TYPE, SECRET) " +
        "VALUES (?, ?, ?, ?, ? ,? ,? ,?)";
    private static final String INSERT_DRBD_RSC =
        "INSERT INTO LAYER_DRBD_RESOURCES " +
        "(LAYER_RESOURCE_ID, PEER_SLOTS, AL_STRIPES, AL_STRIPE_SIZE, FLAGS, NODE_ID) " +
        "VALUES (?, ?, ?, ?, ?, ?)";

    private static final String LAYER_KIND_DRBD = "DRBD";
    private static final String LAYER_KIND_STORAGE = "STORAGE";

    @Override
    public void migrate(Connection connection, DatabaseInfo.DbProduct dbProduct) throws Exception
    {
        Set<RscKey> rscKeysWithLayerData = new HashSet<>();
        int nextLayerResourceId = -1;

        int nextTcpPort;

        try (PreparedStatement selectMaxTcp = connection.prepareStatement(SELECT_HIGHEST_DRBD_TCP_PORT))
        {
            try (ResultSet maxTcpRs = selectMaxTcp.executeQuery())
            {
                if (maxTcpRs.next())
                {
                    nextTcpPort = maxTcpRs.getInt("MAX_TCP_PORT") + 1;
                }
                else
                {
                    nextTcpPort = DEFAULT_START_PORT;
                }
            }
        }

        try (PreparedStatement selectLayerIds = connection.prepareStatement(SELECT_ALL_LAYER_RESOURCE_IDS))
        {
            try (ResultSet layerIdsRs = selectLayerIds.executeQuery())
            {
                while (layerIdsRs.next())
                {
                    rscKeysWithLayerData.add(
                        new RscKey(
                            layerIdsRs.getString("NODE_NAME"),
                            layerIdsRs.getString("RESOURCE_NAME")
                        )
                    );
                    int id = layerIdsRs.getInt("LAYER_RESOURCE_ID");
                    if (id > nextLayerResourceId)
                    {
                        nextLayerResourceId = id;
                    }
                }
            }
        }

        Set<String> drbdRscDfns = new HashSet<>();
        Map<String, List<String>> drbdRscsPerDfn = new HashMap<>();

        final ModularCryptoProvider cryptoProvider = getCryptoProvider();
        final SecretGenerator secretGen = cryptoProvider.createSecretGenerator();
        try (PreparedStatement selectRscs = connection.prepareStatement(SELECT_ALL_RSCS))
        {
            try (ResultSet rscRs = selectRscs.executeQuery())
            {
                while (rscRs.next())
                {
                    RscKey key = new RscKey(
                        rscRs.getString("NODE_NAME"),
                        rscRs.getString("RESOURCE_NAME")
                    );
                    if (!rscKeysWithLayerData.contains(key))
                    {
                        /*
                         * until now no migration created rscLayerData for this resource
                         * this is known to happen when a resource does not have any volumes
                         * during migration.
                         *
                         * however, as we simply NEED to create some rscLayerData and we now have
                         * no information if this is a swordfish resource or if its (non existent)
                         * volume should be encrypted or not, we simply put a DRBD,STORAGE layerData
                         * onto this resource
                         */

                        int drbdId = ++nextLayerResourceId;
                        int stoageId = ++nextLayerResourceId;

                        insertLayerRscId(connection, drbdId, key, LAYER_KIND_DRBD, null);
                        insertLayerRscId(connection, stoageId, key, LAYER_KIND_STORAGE, drbdId);

                        if (drbdRscDfns.add(key.rscName))
                        {
                            String peerSlotsStr = queryProp(
                                connection,
                                "/RESOURCEDEFINITIONS/" + key.rscName.toUpperCase(),
                                "PeerSlotsNewResource"
                            );

                            insertDrbdRscDfn(
                                connection,
                                key,
                                peerSlotsStr == null ? DEFAULT_DRBD_PEER_SLOTS : Integer.parseInt(peerSlotsStr),
                                DEFAULT_DRBD_AL_STRIPES,
                                DEFAULT_DRBD_AL_STRIPE_SIZE,
                                nextTcpPort++,
                                "IP", // unfortunately we lost this information
                                secretGen.generateDrbdSharedSecret() // unfortunateley we lost this information
                            );
                        }
                        String peerSlotsStr = queryProp(
                            connection,
                            "/RESOURCES/" + key.nodeName.toUpperCase() + "/" + key.rscName.toUpperCase(),
                            "PeerSlotsNewResource"
                        );

                        // we lost the information about nodeId
                        List<String> nodeNames = drbdRscsPerDfn.computeIfAbsent(
                            key.rscName,
                            ignored -> new ArrayList<>()
                        );
                        nodeNames.add(key.nodeName);
                        int nodeId = nodeNames.size();

                        insertDrbdRsc(
                            connection,
                            drbdId,
                            peerSlotsStr == null ? DEFAULT_DRBD_PEER_SLOTS : Integer.parseInt(peerSlotsStr),
                            DEFAULT_DRBD_AL_STRIPES,
                            DEFAULT_DRBD_AL_STRIPE_SIZE,
                            rscRs.getLong("RESOURCE_FLAGS"),
                            nodeId
                        );

                        // no special RscStorageData database entry required
                    }
                }
            }
        }
    }

    private void insertLayerRscId(
        Connection connectionRef,
        int id,
        RscKey key,
        String kind,
        @Nullable Integer parentId
    )
        throws SQLException
    {
        try (PreparedStatement stmt = connectionRef.prepareStatement(INSERT_LAYER_RSC_ID))
        {
            stmt.setInt(1, id);
            stmt.setString(2, key.nodeName);
            stmt.setString(3, key.rscName);
            stmt.setString(4, kind);
            if (parentId == null)
            {
                stmt.setNull(5, Types.INTEGER);
            }
            else
            {
                stmt.setInt(5, parentId);
            }
            stmt.setString(6, ""); // rscNameSuffix
            stmt.executeUpdate();
        }
    }

    private @Nullable String queryProp(Connection connection, String instanceName, String key)
        throws SQLException
    {
        String ret = null;
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_SINGLE_PROP))
        {
            stmt.setString(1, instanceName);
            stmt.setString(2, key);

            try (ResultSet resultSet = stmt.executeQuery())
            {
                if (resultSet.next())
                {
                    ret = resultSet.getString("PROP_VALUE");
                }
            }
        }
        return ret;
    }

    private void insertDrbdRscDfn(
        Connection connectionRef,
        RscKey key,
        int peerSlots,
        int alStripesRef,
        long alStripeSizeRef,
        int tcpPortRef,
        String transportType,
        String secret
    )
        throws SQLException
    {
        try (PreparedStatement stmt = connectionRef.prepareStatement(INSERT_DRBD_RSC_DFN))
        {
            stmt.setString(1, key.rscName);
            stmt.setString(2, ""); // rscNameSuffix
            stmt.setInt(3, peerSlots);
            stmt.setInt(4, alStripesRef);
            stmt.setLong(5, alStripeSizeRef);
            stmt.setInt(6, tcpPortRef);
            stmt.setString(7, transportType);
            stmt.setString(8, secret);

            stmt.executeUpdate();
        }
    }

    private void insertDrbdRsc(
        Connection connectionRef,
        int layerId,
        int peerSlots,
        int alStripes,
        long alStripeSize,
        long flags,
        int nodeId
    )
        throws SQLException
    {

        try (PreparedStatement stmt = connectionRef.prepareStatement(INSERT_DRBD_RSC))
        {
            stmt.setInt(1, layerId);
            stmt.setInt(2, peerSlots);
            stmt.setInt(3, alStripes);
            stmt.setLong(4, alStripeSize);
            stmt.setLong(5, flags);
            stmt.setInt(6, nodeId);

            stmt.executeUpdate();
        }
    }

    private static class RscKey
    {
        String nodeName;
        String rscName;

        private RscKey(String nodeNameRef, String rscNameRef)
        {
            nodeName = nodeNameRef;
            rscName = rscNameRef;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(nodeName, rscName);
        }

        @Override
        public boolean equals(Object obj)
        {
            boolean eq = this == obj;
            if (!eq && obj != null && obj instanceof RscKey)
            {
                RscKey other = (RscKey) obj;
                eq = Objects.equals(nodeName, other.nodeName) &&
                    Objects.equals(rscName, other.rscName);
            }
            return eq;
        }

        @Override
        public String toString()
        {
            return "RscKey [nodeName=" + nodeName + ", rscName=" + rscName + "]";
        }
    }

}
