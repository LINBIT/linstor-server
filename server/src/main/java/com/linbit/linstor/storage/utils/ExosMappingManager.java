package com.linbit.linstor.storage.utils;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.DynamicNumberPoolImpl;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.utils.Triple;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Deprecated(forRemoval = true)
@Singleton
public class ExosMappingManager
{
    public static final String CONNECTED = "Connected";
    private static final int EXOS_MIN_LUN = 1;
    private static final int EXOS_MAX_LUN = 1023;

    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final StltConfigAccessor stltConfAccessor;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;

    private final Map<String, ExosEnclosure> enclosureMap;

    @Inject
    public ExosMappingManager(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        StltConfigAccessor stltConfAccessorRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        stltConfAccessor = stltConfAccessorRef;
        rscDfnMap = rscDfnMapRef;

        enclosureMap = new HashMap<>();
    }

    public void allocateAfterDbLoad() throws LinStorException
    {
        try
        {
            for (ResourceDefinition rscDfn : rscDfnMap.values())
            {
                Iterator<VolumeDefinition> vlmDfnIt = rscDfn.iterateVolumeDfn(apiCtx);
                while (vlmDfnIt.hasNext())
                {
                    VolumeDefinition vlmDfn = vlmDfnIt.next();
                    Iterator<Volume> vlmIt = vlmDfn.iterateVolumes(apiCtx);
                    while (vlmIt.hasNext())
                    {
                        Volume vlm = vlmIt.next();
                        ReadOnlyProps props = vlm.getProps(apiCtx);

                        StorPool storPool = LayerVlmUtils.getStorPoolMap(vlm, apiCtx).get(RscLayerSuffixes.SUFFIX_DATA);

                        if (storPool.getDeviceProviderKind().equals(DeviceProviderKind.EXOS))
                        {
                            String enclosureName = getEnclosureName(storPool);
                            ExosEnclosure enclosure = enclosureMap.get(enclosureName);
                            if (enclosure == null)
                            {
                                enclosure = new ExosEnclosure(enclosureName);
                                enclosureMap.put(enclosureName, enclosure);
                            }

                            String enclosurePropKey = ApiConsts.NAMESPC_EXOS + "/" + enclosureName;
                            @Nullable ReadOnlyProps enclosureNamespace = stltConfAccessor.getReadonlyProps()
                                .getNamespace(enclosurePropKey);
                            if (enclosureNamespace == null)
                            {
                                throw new LinStorException("No enclosures defined in '" + enclosurePropKey + "'");
                            }
                            Iterator<String> controllersIt = enclosureNamespace.iterateNamespaces();
                            while (controllersIt.hasNext())
                            {
                                String ctrlName = controllersIt.next();
                                ExosCtrl exosCtrl = enclosure.controllerMap.get(ctrlName);
                                if (exosCtrl == null)
                                {
                                    exosCtrl = new ExosCtrl();
                                    enclosure.controllerMap.put(ctrlName, exosCtrl);
                                }

                                int lun = Integer.parseInt(props.getProp(getLunKey(ctrlName)));
                                String portStr = props.getProp(getPortKey(ctrlName));

                                ExosPort exosPort = exosCtrl.ports.get(portStr);
                                exosPort.lunPool.allocate(lun);
                                exosPort.usedLunCount++;
                            }
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException | ValueInUseException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public void findFreeExosPortAndLun(StorPool storPoolRef, Volume vlmRef)
        throws InvalidKeyException, InvalidValueException, LinStorException, ExhaustedPoolException
    {
        ReadOnlyProps nodeProps = storPoolRef.getNode().getProps(apiCtx);
        String enclosureName = getEnclosureName(storPoolRef);
        String enclosurePropKey = ApiConsts.NAMESPC_EXOS + "/" + enclosureName;


        ExosEnclosure enclosure = enclosureMap.get(enclosureName);
        if (enclosure == null)
        {
            enclosure = new ExosEnclosure(enclosureName);
            enclosureMap.put(enclosureName, enclosure);
        }

        @Nullable ReadOnlyProps enclosureNamespace = stltConfAccessor.getReadonlyProps()
            .getNamespace(enclosurePropKey);
        if (enclosureNamespace == null)
        {
            throw new LinStorException("No enclosures defined in '" + enclosurePropKey + "'");
        }

        Iterator<String> controllersIt = enclosureNamespace.iterateNamespaces();
        while (controllersIt.hasNext())
        {
            String ctrlName = controllersIt.next();
            ExosCtrl exosCtrl = enclosure.controllerMap.get(ctrlName);
            if (exosCtrl == null)
            {
                exosCtrl = new ExosCtrl();
                enclosure.controllerMap.put(ctrlName, exosCtrl);
            }
            ExosPort exosPort = null;
            int lowestLunCount = EXOS_MAX_LUN + 1;

            for (ExosPort ep : exosCtrl.ports.values())
            {
                String portVal = nodeProps
                    .getProp(ApiConsts.NAMESPC_EXOS + "/" + enclosureName + "/" + ctrlName + "/Ports/" + ep.id);
                if (CONNECTED.equalsIgnoreCase(portVal) && ep.usedLunCount < lowestLunCount)
                {
                    exosPort = ep;
                    lowestLunCount = ep.usedLunCount;
                }
            }

            if (exosPort == null)
            {
                throw new LinStorException("No Port with free LUNs left!");
            }

            int lun = exosPort.lunPool.autoAllocate();
            exosPort.usedLunCount++;

            vlmRef.getProps(apiCtx).setProp(
                getLunKey(ctrlName),
                Integer.toString(lun)
            );
            vlmRef.getProps(apiCtx).setProp(
                getPortKey(ctrlName),
                Integer.toString(exosPort.id)
            );
        }
    }

    public void unmap(StorPool storPoolRef, Volume vlmRef) throws InvalidKeyException, LinStorException
    {
        String enclosureName = getEnclosureName(storPoolRef);
        String enclosurePropKey = ApiConsts.NAMESPC_EXOS + "/" + enclosureName;

        ExosEnclosure enclosure = enclosureMap.get(enclosureName);
        if (enclosure == null)
        {
            enclosure = new ExosEnclosure(enclosureName);
            enclosureMap.put(enclosureName, enclosure);
        }

        @Nullable ReadOnlyProps enclosureNamespace = stltConfAccessor.getReadonlyProps()
            .getNamespace(enclosurePropKey);
        if (enclosureNamespace == null)
        {
            throw new LinStorException("No enclosures defined in '" + enclosurePropKey + "'");
        }

        ReadOnlyProps vlmProps = vlmRef.getProps(apiCtx);
        @Nullable ReadOnlyProps namespace = vlmProps.getNamespace(InternalApiConsts.NAMESPC_EXOS_MAP);
        if (namespace != null)
        {
            Iterator<String> ctrlNamespaces = namespace.iterateNamespaces();
            while (ctrlNamespaces.hasNext())
            {
                String ctrlName = ctrlNamespaces.next();
                String lun = vlmProps.getProp(getLunKey(ctrlName));
                String port = vlmProps.getProp(getPortKey(ctrlName));

                ExosPort exosPort = enclosure.controllerMap.get(ctrlName).ports.get(port);
                exosPort.lunPool.deallocate(Integer.parseInt(lun));
                exosPort.usedLunCount--;
            }
        }
    }

    private String getEnclosureName(StorPool storPool) throws AccessDeniedException
    {
        return storPool.getProps(apiCtx) // DO NOT USE PRIOPROPS
            .getProp(ApiConsts.KEY_STOR_POOL_EXOS_ENCLOSURE, ApiConsts.NAMESPC_EXOS);
    }

    public static List<Triple<String, String, String>> getCtrlnamePortLunList(ReadOnlyProps vlmProps)
    {
        List<Triple<String, String, String>> ret = new ArrayList<>();

        @Nullable ReadOnlyProps exosMapNamespace = vlmProps.getNamespace(InternalApiConsts.NAMESPC_EXOS_MAP);
        if (exosMapNamespace == null)
        {
            throw new ImplementationError(InternalApiConsts.NAMESPC_EXOS_MAP + " does not exist in given vlmProps");
        }
        Iterator<String> ctrlIt = exosMapNamespace.iterateNamespaces();
        while (ctrlIt.hasNext())
        {
            String ctrlName = ctrlIt.next();
            ret.add(
                new Triple<>(
                    ctrlName,
                    vlmProps.getProp(getPortKey(ctrlName)),
                    vlmProps.getProp(getLunKey(ctrlName))
                )
            );
        }
        return ret;
    }

    public static String getLunKey(String ctrlName)
    {
        return InternalApiConsts.NAMESPC_EXOS_MAP + "/" + ctrlName + "/" + InternalApiConsts.EXOS_LUN;
    }

    public static String getPortKey(String ctrlName)
    {
        return InternalApiConsts.NAMESPC_EXOS_MAP + "/" + ctrlName + "/" + InternalApiConsts.EXOS_PORT;
    }

    private class ExosEnclosure
    {
        private final String name;
        private final Map<String, ExosCtrl> controllerMap;

        ExosEnclosure(String nameRef)
        {
            name = nameRef;
            controllerMap = new HashMap<>();
        }
    }

    private class ExosCtrl
    {
        private static final int DFLT_PORT_COUNT = 4;
        private final Map<String, ExosPort> ports;

        private ExosCtrl()
        {
            ports = new HashMap<>();
            for (int portIdx = 0; portIdx < DFLT_PORT_COUNT; portIdx++)
            {
                ports.put(Integer.toString(portIdx), new ExosPort(portIdx));
            }
        }
    }

    private class ExosPort
    {
        private final int id;
        private int usedLunCount;
        private final DynamicNumberPool lunPool;

        private ExosPort(int idRef)
        {
            id = idRef;
            usedLunCount = 0;
            lunPool = new DynamicNumberPoolImpl(
                errorReporter,
                null,
                null,
                null,
                null,
                EXOS_MAX_LUN,
                EXOS_MIN_LUN,
                EXOS_MAX_LUN
            );
        }
    }

}
