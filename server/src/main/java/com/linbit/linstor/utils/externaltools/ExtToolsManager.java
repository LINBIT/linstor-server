package com.linbit.linstor.utils.externaltools;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class ExtToolsManager
{
    private Map<ExtTools, ExtToolsInfo> infoMap;

    private Set<DeviceLayerKind> supportedLayers;
    private Set<DeviceProviderKind> supportedProviders = new TreeSet<>();
    private Map<DeviceLayerKind, List<String>> unsupportedLayers = new TreeMap<>();
    private Map<DeviceProviderKind, List<String>> unsupportedProviders = new TreeMap<>();

    public ExtToolsManager()
    {
        supportedLayers = new TreeSet<>();
        supportedProviders = new TreeSet<>();
        unsupportedLayers = new TreeMap<>();
        unsupportedProviders = new TreeMap<>();
        infoMap = new TreeMap<>();
        updateExternalToolsInfo(Collections.emptyList());
    }

    public void updateExternalToolsInfo(Collection<ExtToolsInfo> infoList)
    {
        infoMap.clear();
        supportedLayers.clear();
        supportedProviders.clear();
        unsupportedLayers.clear();
        unsupportedProviders.clear();
        for (ExtToolsInfo info : infoList)
        {
            infoMap.put(info.getTool(), info);
        }
        for (ExtTools tool : ExtTools.values())
        {
            if (!infoMap.containsKey(tool))
            {
                infoMap.put(
                    tool,
                    new ExtToolsInfo(
                        tool,
                        false,
                        null,
                        null,
                        null,
                        Arrays.asList("No information from satellite yet")
                    )
                );
            }
        }

        for (DeviceLayerKind devLayerKind : DeviceLayerKind.values())
        {
            ExtTools[] dependencies = devLayerKind.getExtToolDependencies();
            if (allExternalToolsSupported(dependencies))
            {
                supportedLayers.add(devLayerKind);
            }
            else
            {
                unsupportedLayers.put(devLayerKind, getReasons(dependencies));
            }
        }

        for (DeviceProviderKind devProviderKind : DeviceProviderKind.values())
        {
            if (devProviderKind != DeviceProviderKind.FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER)
            {
                ExtTools[] dependencies = devProviderKind.getExtToolDependencies();
                if (allExternalToolsSupported(dependencies))
                {
                    supportedProviders.add(devProviderKind);
                }
                else
                {
                    unsupportedProviders.put(devProviderKind, getReasons(dependencies));
                }
            }
        }
    }

    public boolean isLayerSupported(DeviceLayerKind supportedLayerRef)
    {
        return supportedLayers.contains(supportedLayerRef);
    }

    public boolean isProviderSupported(DeviceProviderKind supportedProviderRef)
    {
        return supportedProviders.contains(supportedProviderRef);
    }

    public @Nullable ExtToolsInfo getExtToolInfo(ExtTools extTool)
    {
        return infoMap.get(extTool);
    }

    public Set<DeviceLayerKind> getSupportedLayers()
    {
        return supportedLayers;
    }

    public Set<DeviceProviderKind> getSupportedProviders()
    {
        return supportedProviders;
    }

    public Map<DeviceLayerKind, List<String>> getUnsupportedLayersWithReasons()
    {
        return unsupportedLayers;
    }

    public Map<DeviceProviderKind, List<String>> getUnsupportedProvidersWithReasons()
    {
        return unsupportedProviders;
    }

    public Map<String, List<String>> getUnsupportedLayersWithReasonsAsString()
    {
        Map<String, List<String>> mapTmp = new HashMap<>();
        for (Entry<DeviceLayerKind, List<String>> entry : unsupportedLayers.entrySet())
        {
            mapTmp.put(entry.getKey().toString(), entry.getValue());
        }

        return mapTmp;
    }

    public Map<String, List<String>> getUnsupportedProvidersWithReasonsAsString()
    {
        Map<String, List<String>> mapTmp = new HashMap<>();
        for (Entry<DeviceProviderKind, List<String>> entry : unsupportedProviders.entrySet())
        {
            mapTmp.put(entry.getKey().toString(), entry.getValue());
        }

        return mapTmp;
    }

    private List<String> getReasons(ExtTools[] extTools)
    {
        List<String> reasons = new ArrayList<>();

        for (ExtTools extTool : extTools)
        {
            ExtToolsInfo extToolsInfo = infoMap.get(extTool);
            if (extToolsInfo == null)
            {
                reasons.add("No information from satellite");
            }
            else
            {
                if (!extToolsInfo.isSupported())
                {
                    reasons.addAll(extToolsInfo.getNotSupportedReasons());
                }
            }
        }
        return reasons;
    }

    private boolean allExternalToolsSupported(ExtTools[] extToolsRef)
    {
        boolean allChecksSatisfied = true;
        for (ExtTools extTool : extToolsRef)
        {
            ExtToolsInfo extToolsInfo = infoMap.get(extTool);
            if (extToolsInfo == null || !extToolsInfo.isSupported())
            {
                allChecksSatisfied = false;
                break;
            }
        }
        return allChecksSatisfied;
    }

    /**
     * Returns the version of the requested ExtTools if available. Null otherwise.
     */
    public @Nullable Version getVersion(ExtTools extToolsRef)
    {
        ExtToolsInfo extToolInfo = getExtToolInfo(extToolsRef);
        return extToolInfo == null ? null : extToolInfo.getVersion();
    }
}
