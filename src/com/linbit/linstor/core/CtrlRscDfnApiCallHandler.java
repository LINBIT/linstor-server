package com.linbit.linstor.core;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceDefinitionDataControllerFactory;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.SnapshotVolumeDefinition;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinition.VlmDfnApi;
import com.linbit.linstor.VolumeDefinitionData;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.VolumeNumberAlloc;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class CtrlRscDfnApiCallHandler extends AbsApiCallHandler
{
    private final CtrlClientSerializer clientComSerializer;
    private String currentRscName;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final ObjectProtection rscDfnMapProt;
    private final ResourceDefinitionDataControllerFactory resourceDefinitionDataFactory;
    private CtrlVlmDfnApiCallHandler vlmDfnHandler;

    @Inject
    CtrlRscDfnApiCallHandler(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer interComSerializer,
        CtrlClientSerializer clientComSerializerRef,
        @ApiContext AccessContext apiCtxRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        @Named(ControllerSecurityModule.RSC_DFN_MAP_PROT) ObjectProtection rscDfnMapProtRef,
        CtrlObjectFactories objectFactories,
        ResourceDefinitionDataControllerFactory resourceDefinitionDataFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        @PeerContext AccessContext peerAccCtxRef,
        Provider<Peer> peerRef,
        CtrlVlmDfnApiCallHandler vlmDfnHandlerRef,
        WhitelistProps whitelistPropsRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            LinStorObject.RESOURCE_DEFINITION,
            interComSerializer,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef,
            whitelistPropsRef
        );
        clientComSerializer = clientComSerializerRef;

        rscDfnMap = rscDfnMapRef;
        rscDfnMapProt = rscDfnMapProtRef;
        resourceDefinitionDataFactory = resourceDefinitionDataFactoryRef;
        vlmDfnHandler = vlmDfnHandlerRef;
    }

    public ApiCallRc createResourceDefinition(
        String rscNameStr,
        Integer portInt,
        String secret,
        String transportTypeStr,
        Map<String, String> props,
        List<VlmDfnApi> volDescrMap
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.CREATE,
                apiCallRc,
                rscNameStr
            );
        )
        {
            requireRscDfnMapChangeAccess();
            ResourceDefinitionData rscDfn = createRscDfn(rscNameStr, transportTypeStr, portInt, secret);

            fillProperties(props, getProps(rscDfn), ApiConsts.FAIL_ACC_DENIED_RSC_DFN);

            List<VolumeDefinitionData> createdVlmDfns = vlmDfnHandler.createVlmDfns(rscDfn, volDescrMap);

            commit();

            rscDfnMap.put(rscDfn.getName(), rscDfn);

            for (VolumeDefinitionData vlmDfn : createdVlmDfns)
            {
                ApiCallRcEntry volSuccessEntry = new ApiCallRcEntry();
                volSuccessEntry.setReturnCode(ApiConsts.MASK_VLM_DFN | ApiConsts.CREATED);
                String successMessage = String.format(
                    "Volume definition with number '%d' successfully " +
                        " created in resource definition '%s'.",
                    vlmDfn.getVolumeNumber().value,
                    rscNameStr
                );
                volSuccessEntry.setMessageFormat(successMessage);
                volSuccessEntry.putVariable(ApiConsts.KEY_RSC_DFN, rscNameStr);
                volSuccessEntry.putVariable(ApiConsts.KEY_VLM_NR, Integer.toString(vlmDfn.getVolumeNumber().value));
                volSuccessEntry.putVariable(ApiConsts.KEY_MINOR_NR, Integer.toString(vlmDfn.getMinorNr(apiCtx).value));
                volSuccessEntry.putObjRef(ApiConsts.KEY_RSC_DFN, rscNameStr);
                volSuccessEntry.putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmDfn.getVolumeNumber().value));

                apiCallRc.addEntry(volSuccessEntry);

                errorReporter.logInfo(successMessage);
            }

            reportSuccess(rscDfn.getUuid());
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.CREATE,
                getObjectDescriptionInline(rscNameStr),
                getObjRefs(rscNameStr),
                getVariables(rscNameStr),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    public ApiCallRc modifyRscDfn(
        UUID rscDfnUuid,
        String rscNameStr,
        Integer portInt,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (AbsApiCallHandler basicallyThis = setContext(
            ApiCallType.MODIFY,
                apiCallRc,
                rscNameStr
            )
        )
        {
            requireRscDfnMapChangeAccess();

            ResourceName rscName = asRscName(rscNameStr);
            ResourceDefinitionData rscDfn = loadRscDfn(rscName, true);
            if (rscDfnUuid != null && !rscDfnUuid.equals(rscDfn.getUuid()))
            {
                addAnswer(
                    "UUID-check failed",
                    null,
                    "local UUID: " + rscDfn.getUuid().toString() + ", received UUID: " + rscDfnUuid.toString(),
                    null,
                    ApiConsts.FAIL_UUID_RSC_DFN
                );
                throw new ApiCallHandlerFailedException();
            }
            if (portInt != null)
            {
                TcpPortNumber port = asTcpPortNumber(portInt);
                rscDfn.setPort(peerAccCtx, port);
            }
            if (!overrideProps.isEmpty() || !deletePropKeys.isEmpty())
            {
                Map<String, String> map = getProps(rscDfn).map();

                fillProperties(overrideProps, getProps(rscDfn), ApiConsts.FAIL_ACC_DENIED_RSC_DFN);

                for (String delKey : deletePropKeys)
                {
                    String oldValue = map.remove(delKey);
                    if (oldValue == null)
                    {
                        addAnswer(
                            "Could not delete property '" + delKey + "' as it did not exist. " +
                                "This operation had no effect.",
                            ApiConsts.WARN_DEL_UNSET_PROP
                        );
                    }
                }
            }

            commit();

            reportSuccess(rscDfn.getUuid());
            updateSatellites(rscDfn);
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.MODIFY,
                getObjectDescriptionInline(rscNameStr),
                getObjRefs(rscNameStr),
                getVariables(rscNameStr),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    public ApiCallRc deleteResourceDefinition(
        String rscNameStr
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.DELETE,
                apiCallRc,
                rscNameStr
            );
        )
        {
            requireRscDfnMapChangeAccess();

            ResourceDefinitionData rscDfn = loadRscDfn(rscNameStr, false);
            if (rscDfn == null)
            {
                addAnswer(
                    getObjectDescription() + " not found.",
                    ApiConsts.WARN_NOT_FOUND
                );
            }
            else if (!rscDfn.getSnapshotDfns(apiCtx).isEmpty())
            {
                addAnswer(
                    "Cannot delete " + getObjectDescription() + " because it has snapshots.",
                    ApiConsts.FAIL_EXISTS_SNAPSHOT_DFN
                );
            }
            else
            {
                Iterator<Resource> rscIterator = getPrivilegedRscIterator(rscDfn);
                String successMsg;
                String details;
                UUID rscDfnUuid = rscDfn.getUuid();
                if (rscIterator.hasNext())
                {
                    markDeleted(rscDfn);

                    while (rscIterator.hasNext())
                    {
                        Resource rsc = rscIterator.next();
                        markDeletedPrivileged(rsc);
                    }

                    commit();

                    // notify satellites
                    updateSatellites(rscDfn);

                    successMsg = getObjectDescriptionInlineFirstLetterCaps() + " marked for deletion.";
                    details = getObjectDescriptionInlineFirstLetterCaps() + " UUID is: " + rscDfnUuid;
                }
                else
                {
                    ResourceName rscName = rscDfn.getName();
                    delete(rscDfn);
                    commit();

                    rscDfnMap.remove(rscName);

                    successMsg = getObjectDescriptionInlineFirstLetterCaps() + " deleted.";
                    details = getObjectDescriptionInlineFirstLetterCaps() + " UUID was: " + rscDfnUuid;
                }
                reportSuccess(successMsg, details);
            }
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.DELETE,
                getObjectDescriptionInline(rscNameStr),
                getObjRefs(rscNameStr),
                getVariables(rscNameStr),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    void handlePrimaryResourceRequest(
        int msgId,
        String rscNameStr,
        UUID rscUuid
    )
    {
        Peer currentPeer = peer.get();
        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.MODIFY,
                null, // apiCallRc
                rscNameStr
            )
        )
        {
            Resource res = loadRsc(
                currentPeer.getNode().getName().displayValue,
                rscNameStr,
                true
            );
            ResourceDefinitionData resDfn = (ResourceDefinitionData) res.getDefinition();

            Props resDfnProps = getProps(resDfn);
            if (resDfnProps.getProp(InternalApiConsts.PROP_PRIMARY_SET) == null)
            {
                resDfnProps.setProp(
                    InternalApiConsts.PROP_PRIMARY_SET,
                    res.getAssignedNode().getName().value
                );

                commit();

                errorReporter.logTrace(
                    "Primary set for " + currentPeer.getNode().getName().getDisplayName()
                );

                updateSatellites(resDfn);

                currentPeer.sendMessage(
                    internalComSerializer
                        .builder(InternalApiConsts.API_PRIMARY_RSC, msgId)
                        .primaryRequest(rscNameStr, res.getUuid().toString())
                        .build()
                );
            }
        }
        catch (InvalidKeyException | InvalidValueException | AccessDeniedException ignored)
        {
        }
        catch (SQLException sqlExc)
        {
            String errorMessage = String.format(
                "A database error occured while trying to rollback the deletion of " +
                    "resource definition '%s'.",
                rscNameStr
            );
            errorReporter.reportError(
                sqlExc,
                peerAccCtx,
                currentPeer,
                errorMessage
            );
        }
    }

    byte[] listResourceDefinitions(int msgId)
    {
        ArrayList<ResourceDefinitionData.RscDfnApi> rscdfns = new ArrayList<>();
        try
        {
            rscDfnMapProt.requireAccess(peerAccCtx, AccessType.VIEW);
            for (ResourceDefinition rscdfn : rscDfnMap.values())
            {
                try
                {
                    rscdfns.add(rscdfn.getApiData(peerAccCtx));
                }
                catch (AccessDeniedException accDeniedExc)
                {
                    // don't add storpooldfn without access
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // for now return an empty list.
        }

        return clientComSerializer
            .builder(ApiConsts.API_LST_RSC_DFN, msgId)
            .resourceDfnList(rscdfns)
            .build();
    }


    private AbsApiCallHandler setContext(
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        String rscNameStr
    )
    {
        super.setContext(
            type,
            apiCallRc,
            true, // autoClose
            getObjRefs(rscNameStr),
            getVariables(rscNameStr)
        );

        currentRscName = rscNameStr;

        return this;
    }

    private void requireRscDfnMapChangeAccess()
    {
        try
        {
            rscDfnMapProt.requireAccess(
                peerAccCtx,
                AccessType.CHANGE
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "change any resource definitions.",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
    }

    private ResourceDefinitionData createRscDfn(
        String rscNameStr,
        String transportTypeStr,
        Integer portInt,
        String secret
    )
    {
        TransportType transportType;
        if (transportTypeStr == null || transportTypeStr.trim().equals(""))
        {
            transportType = TransportType.IP;
        }
        else
        {
            try
            {
                transportType = TransportType.byValue(transportTypeStr); // TODO needs exception handling
            }
            catch (IllegalArgumentException unknownValueExc)
            {
                throw asExc(
                    unknownValueExc,
                    "The given transport type '" + transportTypeStr + "' is invalid.",
                    ApiConsts.FAIL_INVLD_TRANSPORT_TYPE
                );
            }
        }
        ResourceName rscName = asRscName(rscNameStr);

        ResourceDefinitionData rscDfn;
        try
        {
            rscDfn = resourceDefinitionDataFactory.create(
                peerAccCtx,
                rscName,
                portInt,
                null, // RscDfnFlags
                secret,
                transportType
            );
        }
        catch (ValueOutOfRangeException | ValueInUseException exc)
        {
            throw asExc(
                exc,
                String.format(
                    "The specified TCP port '%d' is invalid.",
                    portInt
                ),
                ApiConsts.FAIL_INVLD_RSC_PORT
            );
        }
        catch (ExhaustedPoolException exc)
        {
            throw asExc(
                exc,
                "Could not find free tcp port",
                ApiConsts.FAIL_POOL_EXHAUSTED_TCP_PORT
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "create " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "creating " + getObjectDescriptionInline()
            );
        }
        catch (LinStorDataAlreadyExistsException exc)
        {
            throw asExc(
                exc,
                getObjectDescription() + " already exists.",
                ApiConsts.FAIL_EXISTS_RSC_DFN
            );
        }
        return rscDfn;
    }


    private void delete(ResourceDefinitionData rscDfn)
    {
        try
        {
            rscDfn.delete(peerAccCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "delete " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "deleting " + getObjectDescriptionInline()
            );
        }
    }

    private void markDeleted(ResourceDefinitionData rscDfn)
    {
        try
        {
            rscDfn.markDeleted(peerAccCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "mark " + getObjectDescriptionInline() + " as deleted",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "deleting " + getObjectDescriptionInline()
            );
        }
    }

    private void markDeletedPrivileged(Resource rsc)
    {
        try
        {
            rsc.markDeleted(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "marking resource '" + rsc.getDefinition().getName().displayValue + "' on node '" +
                    rsc.getAssignedNode().getName().displayValue + "'"
            );
        }
    }

    private Iterator<Resource> getPrivilegedRscIterator(ResourceDefinitionData rscDfn)
    {
        Iterator<Resource> iterator;
        try
        {
            iterator = rscDfn.iterateResource(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        return iterator;
    }

    static VolumeNumber getVlmNr(VlmDfnApi vlmDfnApi, ResourceDefinition rscDfn, AccessContext accCtx)
        throws ValueOutOfRangeException, LinStorException
    {
        VolumeNumber vlmNr;
        Integer vlmNrRaw = vlmDfnApi.getVolumeNr();
        if (vlmNrRaw == null)
        {
            try
            {
                // Avoid using volume numbers that are already in use by active volumes or snapshots.
                // Re-using snapshot volume numbers would result in confusion when restoring from the snapshot.
                int[] occupiedVlmNrs = Stream.concat(
                    rscDfn.streamVolumeDfn(accCtx).map(VolumeDefinition::getVolumeNumber),
                    rscDfn.getSnapshotDfns(accCtx).stream()
                        .map(SnapshotDefinition::getAllSnapshotVolumeDefinitions).flatMap(Collection::stream)
                        .map(SnapshotVolumeDefinition::getVolumeNumber)
                )
                    .mapToInt(VolumeNumber::getValue)
                    .sorted()
                    .distinct()
                    .toArray();

                vlmNr = VolumeNumberAlloc.getFreeVolumeNumber(occupiedVlmNrs);
            }
            catch (AccessDeniedException accDeniedExc)
            {
                throw new ImplementationError(
                    "ApiCtx does not have enough privileges to iterate vlmDfns",
                    accDeniedExc
                );
            }
            catch (ExhaustedPoolException exhausedPoolExc)
            {
                throw new LinStorException(
                    "No more free volume numbers left in range " + VolumeNumber.VOLUME_NR_MIN + " - " +
                    VolumeNumber.VOLUME_NR_MAX,
                    exhausedPoolExc
                );
            }
        }
        else
        {
            vlmNr = new VolumeNumber(vlmNrRaw);
        }
        return vlmNr;
    }

    @Override
    protected String getObjectDescription()
    {
        return "Resource definition: " + currentRscName;
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return getObjectDescriptionInline(currentRscName);
    }

    static String getObjectDescriptionInline(String rscName)
    {
        return "resource definition '" + rscName + "'";
    }

    private Map<String, String> getObjRefs(String rscNameStr)
    {
        Map<String, String> map = new TreeMap<>();
        map.put(ApiConsts.KEY_RSC_DFN, rscNameStr);
        return map;
    }

    private Map<String, String> getVariables(String rscNameStr)
    {
        Map<String, String> map = new TreeMap<>();
        map.put(ApiConsts.KEY_RSC_NAME, rscNameStr);
        return map;
    }
}
