package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.NodeName;
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
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CtrlObjectFactories;
import com.linbit.linstor.core.apicallhandler.AbsApiCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
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
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;

import static com.linbit.utils.StringUtils.firstLetterCaps;

@Singleton
public class CtrlRscDfnApiCallHandler extends AbsApiCallHandler
{
    private final CtrlStltSerializer ctrlStltSerializer;
    private final CtrlClientSerializer clientComSerializer;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final ObjectProtection rscDfnMapProt;
    private final ResourceDefinitionDataControllerFactory resourceDefinitionDataFactory;
    private CtrlVlmDfnApiCallHandler vlmDfnHandler;
    private final ResponseConverter responseConverter;

    @Inject
    CtrlRscDfnApiCallHandler(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        CtrlClientSerializer clientComSerializerRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        @Named(ControllerSecurityModule.RSC_DFN_MAP_PROT) ObjectProtection rscDfnMapProtRef,
        CtrlObjectFactories objectFactories,
        ResourceDefinitionDataControllerFactory resourceDefinitionDataFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        Provider<Peer> peerRef,
        CtrlVlmDfnApiCallHandler vlmDfnHandlerRef,
        WhitelistProps whitelistPropsRef,
        ResponseConverter responseConverterRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef,
            whitelistPropsRef
        );
        ctrlStltSerializer = ctrlStltSerializerRef;
        clientComSerializer = clientComSerializerRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;

        rscDfnMap = rscDfnMapRef;
        rscDfnMapProt = rscDfnMapProtRef;
        resourceDefinitionDataFactory = resourceDefinitionDataFactoryRef;
        vlmDfnHandler = vlmDfnHandlerRef;
        responseConverter = responseConverterRef;
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
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeResourceDefinitionContext(
            peer.get(),
            ApiOperation.makeCreateOperation(),
            rscNameStr
        );

        try
        {
            requireRscDfnMapChangeAccess();
            ResourceDefinitionData rscDfn = createRscDfn(rscNameStr, transportTypeStr, portInt, secret);

            fillProperties(
                LinStorObject.RESOURCE_DEFINITION, props, getProps(rscDfn), ApiConsts.FAIL_ACC_DENIED_RSC_DFN);

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
                volSuccessEntry.setMessage(successMessage);
                volSuccessEntry.putObjRef(ApiConsts.KEY_RSC_DFN, rscNameStr);
                volSuccessEntry.putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmDfn.getVolumeNumber().value));
                volSuccessEntry.putObjRef(ApiConsts.KEY_MINOR_NR, Integer.toString(vlmDfn.getMinorNr(apiCtx).value));

                responses.addEntry(volSuccessEntry);

                errorReporter.logInfo(successMessage);
            }

            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultCreatedEntry(
                rscDfn.getUuid(), getRscDfnDescriptionInline(rscDfn)));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    public ApiCallRc modifyRscDfn(
        UUID rscDfnUuid,
        String rscNameStr,
        Integer portInt,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeResourceDefinitionContext(
            peer.get(),
            ApiOperation.makeModifyOperation(),
            rscNameStr
        );

        try
        {
            requireRscDfnMapChangeAccess();

            ResourceName rscName = asRscName(rscNameStr);
            ResourceDefinitionData rscDfn = loadRscDfn(rscName, true);
            if (rscDfnUuid != null && !rscDfnUuid.equals(rscDfn.getUuid()))
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_UUID_RSC_DFN,
                        "UUID-check failed"
                    )
                    .setDetails("local UUID: " + rscDfn.getUuid().toString() +
                        ", received UUID: " + rscDfnUuid.toString())
                    .build()
                );
            }
            if (portInt != null)
            {
                TcpPortNumber port = asTcpPortNumber(portInt);
                rscDfn.setPort(peerAccCtx.get(), port);
            }
            if (!overrideProps.isEmpty() || !deletePropKeys.isEmpty())
            {
                Map<String, String> map = getProps(rscDfn).map();

                fillProperties(
                    LinStorObject.RESOURCE_DEFINITION, overrideProps, getProps(rscDfn), ApiConsts.FAIL_ACC_DENIED_RSC_DFN);

                for (String delKey : deletePropKeys)
                {
                    String oldValue = map.remove(delKey);
                    if (oldValue == null)
                    {
                        responseConverter.addWithDetail(responses, context, ApiCallRcImpl.simpleEntry(
                            ApiConsts.WARN_DEL_UNSET_PROP,
                            "Could not delete property '" + delKey + "' as it did not exist. " +
                                                "This operation had no effect."
                        ));
                    }
                }
            }

            commit();

            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultModifiedEntry(
                rscDfn.getUuid(), getRscDfnDescriptionInline(rscDfn)));
            responseConverter.addWithDetail(responses, context, ctrlSatelliteUpdater.updateSatellites(rscDfn));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    public ApiCallRc deleteResourceDefinition(
        String rscNameStr
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeResourceDefinitionContext(
            peer.get(),
            ApiOperation.makeDeleteOperation(),
            rscNameStr
        );

        try
        {
            requireRscDfnMapChangeAccess();

            ResourceDefinitionData rscDfn = loadRscDfn(rscNameStr, false);
            if (rscDfn == null)
            {
                responseConverter.addWithDetail(responses, context, ApiCallRcImpl.simpleEntry(
                    ApiConsts.WARN_NOT_FOUND,
                    getRscDfnDescription(rscNameStr) + " not found."
                ));
            }
            else if (!rscDfn.getSnapshotDfns(apiCtx).isEmpty())
            {
                responseConverter.addWithDetail(responses, context, ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_SNAPSHOT_DFN,
                    "Cannot delete " + getRscDfnDescriptionInline(rscNameStr) + " because it has snapshots."
                ));
            }
            else
            {
                Optional<Resource> rscInUse = rscDfn.anyResourceInUse(apiCtx);
                if (rscInUse.isPresent())
                {
                    NodeName nodeName = rscInUse.get().getAssignedNode().getName();
                    responseConverter.addWithOp(responses, context, ApiCallRcImpl
                        .entryBuilder(
                            ApiConsts.FAIL_IN_USE,
                            String.format("Resource '%s' on node '%s' is still in use.",
                                rscNameStr, nodeName.displayValue)
                        )
                        .setCause("Resource is mounted/in use.")
                        .setCorrection(String.format("Un-mount resource '%s' on the node '%s'.",
                                        rscNameStr,
                                        nodeName.displayValue))
                        .build()
                    );
                }
                else
                {
                    Iterator<Resource> rscIterator = getPrivilegedRscIterator(rscDfn);
                    String successMsg;
                    String details;
                    UUID rscDfnUuid = rscDfn.getUuid();
                    String descriptionFirstLetterCaps = firstLetterCaps(getRscDfnDescriptionInline(rscNameStr));
                    if (rscIterator.hasNext())
                    {
                        markDeleted(rscDfn);

                        if (rscDfn.hasDiskless(apiCtx))
                        {
                            // if the resource definition has diskless resource
                            // first delete them, as we can't delete diskfull before
                            // diskfull resource will be delete trigger later in the notify
                            // resource deleted api
                            while (rscIterator.hasNext())
                            {
                                Resource rsc = rscIterator.next();
                                if (rsc.getStateFlags().isSet(apiCtx, Resource.RscFlags.DISKLESS))
                                {
                                    markDeletedPrivileged(rsc);
                                }
                            }
                        }
                        else
                        {
                            // mark all resources deleted
                            while (rscIterator.hasNext())
                            {
                                Resource rsc = rscIterator.next();
                                markDeletedPrivileged(rsc);
                            }
                        }

                        commit();

                        // notify satellites
                        responseConverter.addWithDetail(
                            responses, context, ctrlSatelliteUpdater.updateSatellites(rscDfn));

                        successMsg = descriptionFirstLetterCaps + " marked for deletion.";
                        details = descriptionFirstLetterCaps + " UUID is: " + rscDfnUuid;
                    }
                    else
                    {
                        ResourceName rscName = rscDfn.getName();
                        delete(rscDfn);
                        commit();

                        rscDfnMap.remove(rscName);

                        successMsg = descriptionFirstLetterCaps + " deleted.";
                        details = descriptionFirstLetterCaps + " UUID was: " + rscDfnUuid;
                    }
                    responseConverter.addWithOp(responses, context,
                        ApiCallRcImpl.entryBuilder(ApiConsts.DELETED, successMsg).setDetails(details).build());
                }
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    void handlePrimaryResourceRequest(
        int msgId,
        String rscNameStr,
        UUID rscUuid
    )
    {
        Peer currentPeer = peer.get();
        try
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

                ctrlSatelliteUpdater.updateSatellites(resDfn);

                currentPeer.sendMessage(
                    ctrlStltSerializer
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
                peerAccCtx.get(),
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
            rscDfnMapProt.requireAccess(peerAccCtx.get(), AccessType.VIEW);
            for (ResourceDefinition rscdfn : rscDfnMap.values())
            {
                try
                {
                    rscdfns.add(rscdfn.getApiData(peerAccCtx.get()));
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

    private void requireRscDfnMapChangeAccess()
    {
        try
        {
            rscDfnMapProt.requireAccess(
                peerAccCtx.get(),
                AccessType.CHANGE
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "change any resource definitions",
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
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_TRANSPORT_TYPE,
                    "The given transport type '" + transportTypeStr + "' is invalid."
                ), unknownValueExc);
            }
        }
        ResourceName rscName = asRscName(rscNameStr);

        ResourceDefinitionData rscDfn;
        try
        {
            rscDfn = resourceDefinitionDataFactory.getInstance(
                peerAccCtx.get(),
                rscName,
                portInt,
                null, // RscDfnFlags
                secret,
                transportType,
                true,
                true
            );
        }
        catch (ValueOutOfRangeException | ValueInUseException exc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_INVLD_RSC_PORT, String.format(
                "The specified TCP port '%d' is invalid.",
                portInt
            )), exc);
        }
        catch (ExhaustedPoolException exc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_POOL_EXHAUSTED_TCP_PORT,
                "Could not find free tcp port"
            ), exc);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "create " + getRscDfnDescriptionInline(rscNameStr),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        catch (LinStorDataAlreadyExistsException exc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_EXISTS_RSC_DFN,
                firstLetterCaps(getRscDfnDescriptionInline(rscNameStr)) + " already exists."
            ), exc);
        }
        return rscDfn;
    }


    private void delete(ResourceDefinitionData rscDfn)
    {
        try
        {
            rscDfn.delete(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "delete " + getRscDfnDescriptionInline(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void markDeleted(ResourceDefinitionData rscDfn)
    {
        try
        {
            rscDfn.markDeleted(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "mark " + getRscDfnDescriptionInline(rscDfn) + " as deleted",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
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
            throw new ImplementationError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
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
            throw new ImplementationError(accDeniedExc);
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

                Set<SnapshotVolumeDefinition> snapshotVlmDfns = new HashSet<>();
                for (SnapshotDefinition snapshotDfn : rscDfn.getSnapshotDfns(accCtx))
                {
                    snapshotVlmDfns.addAll(snapshotDfn.getAllSnapshotVolumeDefinitions(accCtx));
                }

                int[] occupiedVlmNrs = Stream.concat(
                    rscDfn.streamVolumeDfn(accCtx).map(VolumeDefinition::getVolumeNumber),
                    snapshotVlmDfns.stream().map(SnapshotVolumeDefinition::getVolumeNumber)
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

    public static String getRscDfnDescription(String rscName)
    {
        return "Resource definition: " + rscName;
    }

    public static String getRscDfnDescriptionInline(ResourceDefinition rscDfn)
    {
        return getRscDfnDescriptionInline(rscDfn.getName().displayValue);
    }

    public static String getRscDfnDescriptionInline(String rscName)
    {
        return "resource definition '" + rscName + "'";
    }

    private static ResponseContext makeResourceDefinitionContext(
        Peer peer,
        ApiOperation operation,
        String rscNameStr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);

        return new ResponseContext(
            peer,
            operation,
            getRscDfnDescription(rscNameStr),
            getRscDfnDescriptionInline(rscNameStr),
            ApiConsts.MASK_RSC_DFN,
            objRefs
        );
    }
}
