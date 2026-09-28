package com.linbit.linstor.prometheus;

import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.core.apis.NetInterfaceApi;
import com.linbit.linstor.core.apis.NetInterfaceApi.StltConn;
import com.linbit.linstor.core.apis.NodeApi;
import com.linbit.linstor.core.apis.ResourceApi;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.apis.VolumeApi;
import com.linbit.linstor.core.apis.VolumeDefinitionApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.logging.ErrorReportResult;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.Pair;
import com.linbit.utils.PairNonNull;

import jakarta.inject.Inject;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.hotspot.DefaultExports;

public class PrometheusBuilder
{
    private final ErrorReporter errorReporter;

    private static final String VOLUME_STATE_HELP;
    private static final String NODE_STATE_HELP;
    private static final String NODE_RECONNECT_ATTEMPT_COUNT_HELP = "Number of node reconnection attempts";
    private static final String NODE_FLAG_HELP = "1 if the node flag is set, 0 otherwise";
    private static final Node.Flags[] EXPORTED_NODE_FLAGS = {
        Node.Flags.DELETE, Node.Flags.EVICTED, Node.Flags.EVACUATE
    };

    private static final String RSC_OPEN_HELP =
        "-1=\"unknown\", 0=\"not open\", 1=\"a DRBD device is open (also read-only on a Secondary)\"";
    private static final int RSC_STATE_UNKNOWN = -1;
    private static final int RSC_STATE_UN_USED = 0;
    private static final int RSC_STATE_IN_USE = 1;

    private enum VolumeStates
    {
        UPTODATE("UpToDate", 1),
        CREATED("Created", 2),
        ATTACHED("Attached", 3),
        DISKLESS("Diskless", 4),
        INCONSISTENT("Inconsistent", 5),
        FAILED("Failed", 6),
        TO_CREATING("To: Creating", 7),
        TO_ATTACHABLE("To: Attachable", 8),
        TO_ATTACHING("To: Attaching", 9),
        DISKLESS_DETACHED("Diskless(Detached)", 90), // leave some free space for 'normal' states
        DUNKNOWN("DUnknown", -1);

        private final String name;
        private final int value;

        VolumeStates(String nameRef, int val)
        {
            name = nameRef;
            value = val;
        }

        private String getName()
        {
            return name;
        }

        private int getValue()
        {
            return value;
        }

        private static VolumeStates getByName(final @Nullable String name)
        {
            VolumeStates vlmStates = DUNKNOWN;
            for (VolumeStates state : VolumeStates.values())
            {
                if (state.getName().equalsIgnoreCase(name))
                {
                    vlmStates = state;
                    break;
                }
            }
            return vlmStates;
        }
    }

    static
    {
        // init volumeStatehelp
        StringBuilder help = new StringBuilder();
        for (VolumeStates state : VolumeStates.values())
        {
            help.append(state.getValue()).append("=\"").append(state.getName()).append("\", ");
        }
        VOLUME_STATE_HELP = help.toString();

        StringBuilder sb = new StringBuilder();
        for (ApiConsts.ConnectionStatus conStat : ApiConsts.ConnectionStatus.values())
        {
            sb.append(conStat.getValue()).append("=\"").append(conStat.name()).append("\", ");
        }
        NODE_STATE_HELP = sb.toString();
    }

    @Inject
    public PrometheusBuilder(
        ErrorReporter errorReporterRef
    )
    {
        errorReporter = errorReporterRef;
        DefaultExports.initialize();
    }

    public static Map<String, String> nodeExport(final NodeApi nodeApi)
    {
        final HashMap<String, String> map = new HashMap<>();
        @Nullable NetInterfaceApi activeStltConn = nodeApi.getActiveStltConn();
        if (activeStltConn != null)
        {
            @Nullable StltConn stltConnInfo = activeStltConn.getStltConn();
            if (stltConnInfo != null)
            {
                map.put("address", activeStltConn.getAddress());
                map.put("encryption", stltConnInfo.getSatelliteConnectionEncryptionType());
                map.put("port", stltConnInfo.getSatelliteConnectionPort() + "");
            }
        }
        map.put("node", nodeApi.getName());
        map.put("nodetype", nodeApi.getType());
        return map;
    }

    public static Map<String, String> resourceDfnLabels(
        final ResourceDefinitionApi rscDfnApi
    )
    {
        final HashMap<String, String> map = new HashMap<>();
        map.put("name", rscDfnApi.getResourceName());
        map.put("resource_group", rscDfnApi.getResourceGroup().getName());
        return map;
    }

    public static Map<String, String> volumeDfnLabels(
        final ResourceDefinitionApi rscDfnApi,
        final VolumeDefinitionApi vlmDfnApi
    )
    {
        final HashMap<String, String> map = new HashMap<>();
        map.put("name", rscDfnApi.getResourceName());
        map.put("volume_number", vlmDfnApi.getVolumeNr() + "");
        map.put("resource_group", rscDfnApi.getResourceGroup().getName());
        return map;
    }

    public static Map<String, String> resourceExport(final ResourceApi resourceApi)
    {
        final HashMap<String, String> map = new HashMap<>();
        map.put("name", resourceApi.getName());
        map.put("node", resourceApi.getNodeName());
        return map;
    }

    private @Nullable SatelliteResourceState getResourceState(
        final Map<NodeName, SatelliteState> satelliteStates,
        final ResourceApi rscApi
    )
    {
        SatelliteResourceState state = null;
        try
        {
            final ResourceName rscNameRes = new ResourceName(rscApi.getName());
            final NodeName linNodeName = new NodeName(rscApi.getNodeName());
            if (satelliteStates.containsKey(linNodeName) && satelliteStates.get(linNodeName)
                    .getResourceStates().containsKey(rscNameRes))
            {
                state = satelliteStates.get(linNodeName).getResourceStates().get(rscNameRes);
            }
        }
        catch (InvalidNameException invNamExc)
        {
            errorReporter.reportError(invNamExc);
        }
        return state;
    }

    private static int resourceState(@Nullable SatelliteResourceState resState)
    {
        int state = RSC_STATE_UNKNOWN;
        if (resState != null && resState.isInUse() != null)
        {
            state = Boolean.TRUE.equals(resState.isInUse()) ? RSC_STATE_IN_USE : RSC_STATE_UN_USED;
        }
        return state;
    }

    /**
     * Writes whether a DRBD device of each resource is open, also if it is only opened read-only on a Secondary.
     * A separate gauge instead of a new value of linstor_resource_state, so that the meaning of that metric stays
     * the same for existing dashboards.
     */
    private void writeResourceOpen(TextFormat tf, ResourceList rl)
    {
        tf.startGauge("linstor_resource_open", RSC_OPEN_HELP);
        for (ResourceApi resApi : rl.getResources())
        {
            tf.writeSample(resourceExport(resApi), resourceOpen(getResourceState(rl.getSatelliteStates(), resApi)));
        }
    }

    private static int resourceOpen(@Nullable SatelliteResourceState resState)
    {
        int open = RSC_STATE_UNKNOWN;
        if (resState != null && resState.isOpen() != null)
        {
            open = Boolean.TRUE.equals(resState.isOpen()) ? 1 : 0;
        }
        return open;
    }

    private static Map<String, String> volumeExport(
        final ResourceApi resourceApi,
        final VolumeApi vlmApi
    )
    {
        final HashMap<String, String> map = new HashMap<>();
        map.put("resource", resourceApi.getName());
        map.put("node", resourceApi.getNodeName());
        map.put("volume", vlmApi.getVlmNr() + "");
        map.put("device_path", vlmApi.getDevicePath());
        map.put("storage_pool", vlmApi.getStorageStorPool().map(StorPoolApi::getStorPoolName).orElse("_unknown"));
        return map;
    }

    private static double volumeState(ResourceApi rscApi, JsonGenTypes.Volume vlm)
    {
        if (vlm.state == null)
        {
            return VolumeStates.DUNKNOWN.getValue();
        }

        String diskState = vlm.state.disk_state;
        if (diskState != null && diskState.equals("Diskless") &&
            (rscApi.getFlags() & Resource.Flags.DRBD_DISKLESS.flagValue) != Resource.Flags.DRBD_DISKLESS.flagValue)
        {
            diskState = "Diskless(Detached)";
        }

        return VolumeStates.getByName(diskState).getValue();
    }

    private static Map<String, String> storagePoolExport(final StorPoolApi storPoolApi)
    {
        final HashMap<String, String> map = new HashMap<>();
        map.put("storage_pool", storPoolApi.getStorPoolName());
        map.put("node", storPoolApi.getNodeName());
        map.put("driver", storPoolApi.getDeviceProviderKind().toString());
        map.put("backing_pool", storPoolApi.getBackingPoolName());
        return map;
    }

    private static double safeStorPoolValue(final Optional<Long> optVal, final DeviceProviderKind kind)
    {
        double val = optVal.isPresent() ? optVal.get() * 1024 : Double.NaN;
        if (kind == DeviceProviderKind.DISKLESS)
        {
            val = 0.0;
        }
        return val;
    }

    /**
     * Writes the user-visible node flags of every node as 0/1, so that an alert on "== 1" also sees the flag being
     * cleared. QIGNORE is internal and not exported. EVICTED includes the DELETE bit, but an evicted node is not
     * being deleted, so DELETE is only reported for nodes that are not evicted.
     */
    private static void writeNodeFlags(TextFormat tf, List<NodeApi> nodeApiList)
    {
        tf.startGauge("linstor_node_flag", NODE_FLAG_HELP);
        for (NodeApi node : nodeApiList)
        {
            final long flags = node.getFlags();
            final boolean evicted = isFlagSet(flags, Node.Flags.EVICTED);
            for (Node.Flags flag : EXPORTED_NODE_FLAGS)
            {
                boolean set = isFlagSet(flags, flag) && !(flag == Node.Flags.DELETE && evicted);
                Map<String, String> labels = nodeExport(node);
                labels.put("flag", flag.name());
                tf.writeSample(labels, set ? 1 : 0);
            }
        }
    }

    private static boolean isFlagSet(long flags, Node.Flags flag)
    {
        return (flags & flag.flagValue) == flag.flagValue;
    }

    /**
     * Builds the prometheus metrics format in a TextWriter and jvm statistics in a StringWriter and finally
     * converts them to a String.
     *
     * @param nodeApiList node list included in the report
     * @param rscDfns resource definition included in the report
     * @param rl resource list included in the report
     * @param storagePoolList storage pool list included in the report
     * @param errorReports error report result for counting statistics
     * @param scrapeRequestCount how often was yet scraped
     * @param scrapeStartMillis scrape start time in millis
     * @return A String in the prometheus text format output
     * @throws IOException Prometheus format library may throw on final build.
     */
    public String build(
        @Nullable final List<NodeApi> nodeApiList,
        @Nullable final List<ResourceDefinitionApi> rscDfns,
        @Nullable final ResourceList rl,
        @Nullable final List<StorPoolApi> storagePoolList,
        @Nullable final ErrorReportResult errorReports,
        final long scrapeRequestCount,
        final long scrapeStartMillis) throws IOException
    {
        TextFormat tf = new TextFormat();

        tf.startGauge("linstor_info");
        Map<String, String> infoMap = new HashMap<>();
        infoMap.put("version", LinStor.VERSION_INFO_PROVIDER.getVersion());
        infoMap.put("buildtime", LinStor.VERSION_INFO_PROVIDER.getBuildTime());
        infoMap.put("gitid", LinStor.VERSION_INFO_PROVIDER.getGitCommitId());
        tf.writeSample(infoMap, 1);

        if (nodeApiList != null)
        {
            tf.startGauge("linstor_node_state", NODE_STATE_HELP);
            for (NodeApi node : nodeApiList)
            {
                tf.writeSample(
                    nodeExport(node),
                    node.connectionStatus().getValue()
                );
            }

            tf.startGauge("linstor_node_reconnect_attempt_count", NODE_RECONNECT_ATTEMPT_COUNT_HELP);
            for (NodeApi node : nodeApiList)
            {
                tf.writeSample(
                    nodeExport(node),
                    (double) node.getReconnectAttemptCount()
                );
            }

            writeNodeFlags(tf, nodeApiList);
        }

        if (rscDfns != null)
        {
            tf.startGauge("linstor_resource_definition_count");
            tf.writeSample(rscDfns.size());

            if (rl != null)
            {
                tf.startGauge("linstor_resource_definition_resource_count");
                for (var rscDfn : rscDfns)
                {
                    double count = rl.getResources().stream()
                        .filter(r -> r.getName().equalsIgnoreCase(rscDfn.getResourceName())).count();
                    tf.writeSample(resourceDfnLabels(rscDfn), count);
                }
            }

            tf.startGauge("linstor_volume_definition_size_bytes");
            for (var rscDfn : rscDfns)
            {
                for (var vlmDfn : rscDfn.getVlmDfnList())
                {
                    tf.writeSample(volumeDfnLabels(rscDfn, vlmDfn), (double) (vlmDfn.getSize() * 1024));
                }
            }
        }

        if (rl != null)
        {
            tf.startGauge("linstor_resource_state", "-1=\"unknown state\", 0=\"secondary\", 1=\"primary\"");
            ArrayList<PairNonNull<ResourceApi, VolumeApi>> volumeApis = new ArrayList<>();
            for (ResourceApi resApi : rl.getResources())
            {
                SatelliteResourceState resState = getResourceState(rl.getSatelliteStates(), resApi);
                tf.writeSample(resourceExport(resApi), resourceState(resState));
                volumeApis.addAll(resApi.getVlmList().stream()
                    .map(vlmApi -> new PairNonNull<ResourceApi, VolumeApi>(resApi, vlmApi))
                    .collect(Collectors.toList()));
            }
            writeResourceOpen(tf, rl);

            tf.startGauge("linstor_volume_state", VOLUME_STATE_HELP);
            for (PairNonNull<ResourceApi, VolumeApi> pair : volumeApis)
            {
                JsonGenTypes.Volume vlm = Json.apiToVolume(pair.objB);
                vlm.state = Json.apiToVolumeState(
                    rl.getSatelliteStates(), pair.objA.getNodeName(), pair.objA.getName(), vlm.volume_number
                );
                tf.writeSample(volumeExport(pair.objA, pair.objB), volumeState(pair.objA, vlm));
            }

            tf.startGauge("linstor_volume_allocated_size_bytes");
            for (PairNonNull<ResourceApi, VolumeApi> pair : volumeApis)
            {
                double val = pair.objB.getAllocatedSize().isPresent() ?
                    pair.objB.getAllocatedSize().get() * 1024 : Double.NaN;
                tf.writeSample(volumeExport(pair.objA, pair.objB), val);
            }
        }

        if (storagePoolList != null)
        {
            tf.startGauge("linstor_storage_pool_capacity_free_bytes");
            for (StorPoolApi storPoolApi : storagePoolList)
            {
                tf.writeSample(
                    storagePoolExport(storPoolApi),
                    safeStorPoolValue(storPoolApi.getFreeCapacity(), storPoolApi.getDeviceProviderKind())
                );
            }

            tf.startGauge("linstor_storage_pool_capacity_total_bytes");
            for (StorPoolApi storPoolApi : storagePoolList)
            {
                tf.writeSample(
                    storagePoolExport(storPoolApi),
                    safeStorPoolValue(storPoolApi.getTotalCapacity(), storPoolApi.getDeviceProviderKind())
                );
            }

            tf.startGauge("linstor_storage_pool_error_count");
            for (StorPoolApi storPoolApi : storagePoolList)
            {
                tf.writeSample(storagePoolExport(storPoolApi),
                    (double) storPoolApi.getReports().stream()
                        .filter(ApiCallRc.RcEntry::isError).count()
                );
            }
        }

        if (errorReports != null)
        {
            tf.startGauge("linstor_error_reports_count");
            tf.writeSample(null, (double) errorReports.getTotalCount());
            for (Map.Entry<Pair<String, String>, Long> entry : errorReports.getNodeCounts().entrySet())
            {
                HashMap<String, String> errMap = new HashMap<>();
                errMap.put("hostname", entry.getKey().objA);
                errMap.put("module", entry.getKey().objB);
                tf.writeSample(errMap, entry.getValue());
            }
        }

        StringWriter sw = new StringWriter();
        io.prometheus.client.exporter.common.TextFormat.write004(
            sw, CollectorRegistry.defaultRegistry.metricFamilySamples()
        );

        tf.startCounter("linstor_scrape_requests_count");
        tf.writeSample((double) scrapeRequestCount);

        tf.startGauge("linstor_scrape_duration_seconds");
        tf.writeSample((System.currentTimeMillis() - scrapeStartMillis) / 1000.0);

        return tf.toString() + sw.toString();
    }
}
