package com.linbit.linstor.prometheus;

import com.linbit.InvalidNameException;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.Volumes;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.core.apis.NodeApi;
import com.linbit.linstor.core.apis.ResourceApi;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.apis.VolumeApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.logging.ErrorReport;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.hotspot.DefaultExports;

public class PrometheusBuilder {
    private final ErrorReporter errorReporter;

    private static final String VOLUME_STATE_HELP;
    private static final String NODE_STATE_HELP;

    private static final int RSC_STATE_UNKNOWN = -1;
    private static final int RSC_STATE_UN_USED = 0;
    private static final int RSC_STATE_IN_USE = 1;

    private enum VolumeStates {
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

        public String getName()
        {
            return name;
        }

        public int getValue()
        {
            return value;
        }

        public static VolumeStates getByName(final String name)
        {
            for(VolumeStates state : VolumeStates.values()) {
                if (state.getName().equalsIgnoreCase(name))
                {
                    return state;
                }
            }
            return DUNKNOWN;
        }
    }

    static {
        // init volumeStatehelp
        StringBuilder help = new StringBuilder();
        for (VolumeStates state : VolumeStates.values()) {
            help.append(state.getValue()).append("=\"").append(state.getName()).append("\", ");
        }
        VOLUME_STATE_HELP = help.toString();

        StringBuilder sb = new StringBuilder();
        for (ApiConsts.ConnectionStatus conStat : ApiConsts.ConnectionStatus.values()) {
            sb.append(conStat.ordinal()).append("=\"").append(conStat.name()).append("\", ");
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

    @Nonnull
    public static Map<String, String> nodeExport(final NodeApi nodeApi)
    {
        final HashMap<String, String> map = new HashMap<>();
        if (nodeApi.getActiveStltConn() != null)
        {
            map.put("address", nodeApi.getActiveStltConn().getAddress());
            map.put("encryption", nodeApi.getActiveStltConn().getSatelliteConnectionEncryptionType());
            map.put("port", nodeApi.getActiveStltConn().getSatelliteConnectionPort() + "");
        }
        map.put("node", nodeApi.getName());
        map.put("nodetype", nodeApi.getType());
        return map;
    }

    @Nonnull
    public static Map<String, String> resourceExport(final ResourceApi resourceApi)
    {
        final HashMap<String, String> map = new HashMap<>();
        map.put("name", resourceApi.getName());
        map.put("node", resourceApi.getNodeName());
        return map;
    }

    @Nullable
    private SatelliteResourceState getResourceState(
        final Map<NodeName, SatelliteState> satelliteStates, final ResourceApi rscApi)
    {
        try {
            final ResourceName rscNameRes = new ResourceName(rscApi.getName());
            final NodeName linNodeName = new NodeName(rscApi.getNodeName());
            if (satelliteStates.containsKey(linNodeName) && satelliteStates.get(linNodeName)
                    .getResourceStates().containsKey(rscNameRes))
            {
                return satelliteStates.get(linNodeName).getResourceStates().get(rscNameRes);
            }
        } catch (InvalidNameException invNamExc) {
            errorReporter.reportError(invNamExc);
        }
        return null;
    }

    private static int resourceState(@Nullable SatelliteResourceState resState)
    {
        int state = RSC_STATE_UNKNOWN;
        if (resState != null && resState.isInUse() != null)
        {
            state = resState.isInUse() == Boolean.TRUE ? RSC_STATE_IN_USE : RSC_STATE_UN_USED;
        }
        return state;
    }

    @Nonnull
    private static Map<String, String> volumeExport(
        final ResourceApi resourceApi, final VolumeApi vlmApi)
    {
        final HashMap<String, String> map = new HashMap<>();
        map.put("resource", resourceApi.getName());
        map.put("node", resourceApi.getNodeName());
        map.put("volume", vlmApi.getVlmNr() + "");
        map.put("device_path", vlmApi.getDevicePath());
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

    public String build(
        @Nullable final List<NodeApi> nodeApiList,
        @Nullable final List<ResourceDefinitionApi> rscDfns,
        @Nullable final ResourceList rl,
        @Nullable final List<StorPoolApi> storagePoolList,
        @Nullable final List<ErrorReport> errorReports,
        final long scrape_request_count,
        final long scrape_start_millis) throws IOException
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
            for (NodeApi node : nodeApiList) {
                tf.writeSample(
                        nodeExport(node),
                        node.connectionStatus().getValue());
            }
        }

        if (rscDfns != null)
        {
            tf.startGauge("linstor_resource_definition_count");
            tf.writeSample(rscDfns.size());
        }

        if (rl != null)
        {
            tf.startGauge("linstor_resource_state", "-1=\"unknown state\", 0=\"secondary\", 1=\"primary\"");
            ArrayList<Pair<ResourceApi, VolumeApi>> volumeApis = new ArrayList<>();
            for (ResourceApi resApi : rl.getResources()) {
                SatelliteResourceState resState = getResourceState(rl.getSatelliteStates(), resApi);
                tf.writeSample(resourceExport(resApi), resourceState(resState));
                volumeApis.addAll(resApi.getVlmList().stream()
                    .map(vlmApi -> new Pair<ResourceApi, VolumeApi>(resApi, vlmApi))
                    .collect(Collectors.toList()));
            }

            tf.startGauge("linstor_volume_state", VOLUME_STATE_HELP);
            for (Pair<ResourceApi, VolumeApi> pair : volumeApis) {
                JsonGenTypes.Volume vlm = Json.apiToVolume(pair.objB);
                vlm.state = Volumes.getVolumeState(
                    rl, pair.objA.getNodeName(), pair.objA.getName(), vlm.volume_number);
                tf.writeSample(volumeExport(pair.objA, pair.objB), volumeState(pair.objA, vlm));
            }

            tf.startGauge("linstor_volume_allocated_size_bytes");
            for (Pair<ResourceApi, VolumeApi> pair : volumeApis) {
                double val = pair.objB.getAllocatedSize().isPresent() ?
                    pair.objB.getAllocatedSize().get() * 1024 : Double.NaN;
                tf.writeSample(volumeExport(pair.objA, pair.objB), val);
            }
        }

        if (storagePoolList != null)
        {
            tf.startGauge("linstor_storage_pool_capacity_free_bytes");
            for (StorPoolApi storPoolApi : storagePoolList) {
                tf.writeSample(
                    storagePoolExport(storPoolApi),
                    safeStorPoolValue(storPoolApi.getFreeCapacity(), storPoolApi.getDeviceProviderKind()));
            }

            tf.startGauge("linstor_storage_pool_capacity_total_bytes");
            for (StorPoolApi storPoolApi : storagePoolList) {
                tf.writeSample(
                    storagePoolExport(storPoolApi),
                    safeStorPoolValue(storPoolApi.getTotalCapacity(), storPoolApi.getDeviceProviderKind()));
            }

            tf.startGauge("linstor_storage_pool_error_count");
            for (StorPoolApi storPoolApi : storagePoolList) {
                tf.writeSample(storagePoolExport(storPoolApi),
                    storPoolApi.getReports().getEntries().stream()
                        .filter(ApiCallRc.RcEntry::isError).count());
            }
        }

        if (errorReports != null)
        {
            tf.startGauge("linstor_error_reports_count");
            tf.writeSample(null, errorReports.size());
            Set<Pair<String, String>> nodes = errorReports.stream()
                .map(err -> new Pair<>(err.getModuleString(), err.getNodeName()))
                .collect(Collectors.toSet());

            nodes.forEach(node -> {
                HashMap<String, String> errMap = new HashMap<>();
                errMap.put("hostname", node.objB);
                errMap.put("module", node.objA);
                tf.writeSample(errMap, errorReports.stream()
                    .filter(rep -> rep.getNodeName().equals(node.objB) &&
                        rep.getModuleString().equals(node.objA))
                    .count());
            });
        }

        StringWriter sw = new StringWriter();
        io.prometheus.client.exporter.common.TextFormat.write004(
            sw, CollectorRegistry.defaultRegistry.metricFamilySamples());

        tf.startCounter("linstor_scrape_requests_count");
        tf.writeSample(scrape_request_count);

        tf.startGauge("linstor_scrape_duration_seconds");
        tf.writeSample((System.currentTimeMillis() - scrape_start_millis) / 1000.0);

        return tf.toString() + sw.toString();
    }
}
