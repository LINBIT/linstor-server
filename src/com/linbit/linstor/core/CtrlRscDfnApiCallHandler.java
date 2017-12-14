package com.linbit.linstor.core;

import static com.linbit.linstor.api.ApiConsts.KEY_AL_SIZE;
import static com.linbit.linstor.api.ApiConsts.KEY_AL_STRIPES;
import static com.linbit.linstor.api.ApiConsts.KEY_ID;
import static com.linbit.linstor.api.ApiConsts.KEY_MINOR_NR;
import static com.linbit.linstor.api.ApiConsts.KEY_PEER_COUNT;
import static com.linbit.linstor.api.ApiConsts.KEY_ROLE;
import static com.linbit.linstor.api.ApiConsts.KEY_RSC_DFN;
import static com.linbit.linstor.api.ApiConsts.KEY_RSC_NAME;
import static com.linbit.linstor.api.ApiConsts.KEY_VLM_NR;
import static com.linbit.linstor.api.ApiConsts.KEY_VLM_SIZE;
import static com.linbit.linstor.api.ApiConsts.RC_RSC_DFN_CRT_FAIL_ACC_DENIED_RSC_DFN;
import static com.linbit.linstor.api.ApiConsts.RC_RSC_DFN_CRT_FAIL_ACC_DENIED_VLM_DFN;
import static com.linbit.linstor.api.ApiConsts.RC_RSC_DFN_CRT_FAIL_EXISTS_RSC_DFN;
import static com.linbit.linstor.api.ApiConsts.RC_RSC_DFN_CRT_FAIL_EXISTS_VLM_DFN;
import static com.linbit.linstor.api.ApiConsts.RC_RSC_DFN_CRT_FAIL_INVLD_MINOR_NR;
import static com.linbit.linstor.api.ApiConsts.RC_RSC_DFN_CRT_FAIL_INVLD_RSC_NAME;
import static com.linbit.linstor.api.ApiConsts.RC_RSC_DFN_CRT_FAIL_INVLD_RSC_PORT;
import static com.linbit.linstor.api.ApiConsts.RC_RSC_DFN_CRT_FAIL_INVLD_VLM_NR;
import static com.linbit.linstor.api.ApiConsts.RC_RSC_DFN_CRT_FAIL_INVLD_VLM_SIZE;
import static com.linbit.linstor.api.ApiConsts.RC_RSC_DFN_CRT_FAIL_SQL;
import static com.linbit.linstor.api.ApiConsts.RC_RSC_DFN_CRT_FAIL_SQL_ROLLBACK;
import static com.linbit.linstor.api.ApiConsts.RC_RSC_DFN_CRT_FAIL_UNKNOWN_ERROR;
import static com.linbit.linstor.api.ApiConsts.RC_RSC_DFN_DEL_FAIL_ACC_DENIED_RSC_DFN;
import static com.linbit.linstor.api.ApiConsts.RC_RSC_DFN_DEL_FAIL_IMPL_ERROR;
import static com.linbit.linstor.api.ApiConsts.RC_RSC_DFN_DEL_FAIL_INVLD_RSC_NAME;
import static com.linbit.linstor.api.ApiConsts.RC_RSC_DFN_DEL_FAIL_SQL;
import static com.linbit.linstor.api.ApiConsts.RC_RSC_DFN_DEL_FAIL_SQL_ROLLBACK;
import static com.linbit.linstor.api.ApiConsts.RC_RSC_DFN_DEL_FAIL_UNKNOWN_ERROR;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinition.RscDfnFlags;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinition.VlmDfnApi;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.VolumeDefinitionData;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlListSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlSerializer;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;

class CtrlRscDfnApiCallHandler extends AbsApiCallHandler
{
    private static final AtomicInteger MINOR_GEN = new AtomicInteger(1000); // FIXME use poolAllocator instead

    private final ThreadLocal<String> currentRscNameStr = new ThreadLocal<>();

    private final CtrlSerializer<Resource> rscSerializer;
    private final CtrlListSerializer<ResourceDefinition.RscDfnApi> rscDfnListSerializer;

    CtrlRscDfnApiCallHandler(
        Controller controllerRef,
        CtrlSerializer<Resource> rscSerializerRef,
        CtrlListSerializer<ResourceDefinition.RscDfnApi> rscDfnListSerializerRef,
        AccessContext apiCtxRef
    )
    {
        super(controllerRef, apiCtxRef, ApiConsts.MASK_RSC_DFN);
        rscSerializer = rscSerializerRef;
        rscDfnListSerializer = rscDfnListSerializerRef;
    }

    public ApiCallRc createResourceDefinition(
        AccessContext accCtx,
        Peer client,
        String rscNameStr,
        int portInt,
        String secret,
        String transportTypeStr,
        Map<String, String> props,
        List<VlmDfnApi> volDescrMap
    )
    {
        /*
         * Usually its better to handle exceptions "close" to their appearance.
         * However, as in this method almost every other line throws an exception,
         * the code would get completely unreadable; thus, unmaintainable.
         *
         * For that reason there is (almost) only one try block with many catches, and
         * those catch blocks handle the different cases (commented as <some>Exc<count> in
         * the try block and a matching "handle <some>Exc<count>" in the catch block)
         */

        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        ResourceDefinition rscDfn = null;
        TransactionMgr transMgr = null;

        VolumeNumber volNr = null;
        MinorNumber minorNr = null;
        VolumeDefinition.VlmDfnApi currentVlmDfnApi = null;

        short peerCount = getAsShort(props, KEY_PEER_COUNT, controller.getDefaultPeerCount());
        int alStripes = getAsInt(props, KEY_AL_STRIPES, controller.getDefaultAlStripes());
        long alStripeSize = getAsLong(props, KEY_AL_SIZE, controller.getDefaultAlSize());

        try
        {
            transMgr = new TransactionMgr(controller.dbConnPool.getConnection()); // sqlExc1

            controller.rscDfnMapProt.requireAccess(accCtx, AccessType.CHANGE); // accDeniedExc1

            RscDfnFlags[] rscDfnInitFlags = null;

            TransportType transportType;
            if (transportTypeStr == null || transportTypeStr.trim().equals(""))
            {
                transportType = TransportType.IP;
            }
            else
            {
                transportType = TransportType.byValue(transportTypeStr); // TODO needs exception handling
            }

            rscDfn = ResourceDefinitionData.getInstance( // sqlExc2, accDeniedExc1 (same as last line), alreadyExistsExc1
                accCtx,
                new ResourceName(rscNameStr), // invalidNameExc1
                new TcpPortNumber(portInt), // valOORangeExc1
                rscDfnInitFlags,
                secret,
                transportType,
                transMgr,
                true, // persist this entry
                true // throw exception if the entry exists
            );
            rscDfn.setConnection(transMgr); // in case we will create vlmDfns
            rscDfn.getProps(accCtx).map().putAll(props);

            for (VolumeDefinition.VlmDfnApi vlmDfnApi : volDescrMap)
            {
                currentVlmDfnApi = vlmDfnApi;

                volNr = null;
                minorNr = null;

                volNr = getVlmNr(vlmDfnApi, rscDfn, apiCtx);
                minorNr = getMinor(vlmDfnApi);

                long size = vlmDfnApi.getSize();

                // getGrossSize performs check and throws exception when something is invalid
                controller.getMetaDataApi().getGrossSize(size, peerCount, alStripes, alStripeSize);
                // mdExc1

                VlmDfnFlags[] vlmDfnInitFlags = null;

                VolumeDefinitionData vlmDfn = VolumeDefinitionData.getInstance( // mdExc2, sqlExc3, accDeniedExc2, alreadyExistsExc2
                    accCtx,
                    rscDfn,
                    volNr,
                    minorNr,
                    size,
                    vlmDfnInitFlags,
                    transMgr,
                    true, // persist this entry
                    true // throw exception if the entry exists
                );
                vlmDfn.setConnection(transMgr);
                vlmDfn.getProps(accCtx).map().putAll(vlmDfnApi.getProps());
            }

            transMgr.commit(); // sqlExc4

            controller.rscDfnMap.put(rscDfn.getName(), rscDfn);

            for (VolumeDefinition.VlmDfnApi volCrtData : volDescrMap)
            {
                ApiCallRcEntry volSuccessEntry = new ApiCallRcEntry();
                volSuccessEntry.setReturnCode(ApiConsts.RC_VLM_DFN_CREATED);
                String successMessage = String.format(
                    "Volume definition with number '%d' successfully " +
                        " created in resource definition '%s'.",
                    volCrtData.getVolumeNr(),
                    rscNameStr
                );
                volSuccessEntry.setMessageFormat(successMessage);
                volSuccessEntry.putVariable(KEY_RSC_DFN, rscNameStr);
                volSuccessEntry.putVariable(KEY_VLM_NR, Integer.toString(volCrtData.getVolumeNr()));
                volSuccessEntry.putVariable(KEY_MINOR_NR, Integer.toString(volCrtData.getMinorNr()));
                volSuccessEntry.putObjRef(KEY_RSC_DFN, rscNameStr);
                volSuccessEntry.putObjRef(KEY_VLM_NR, Integer.toString(volCrtData.getVolumeNr()));

                apiCallRc.addEntry(volSuccessEntry);

                controller.getErrorReporter().logInfo(successMessage);
            }

            ApiCallRcEntry successEntry = new ApiCallRcEntry();

            String successMsg = String.format(
                "Resource definition '%s' successfully created.",
                rscNameStr
            );
            successEntry.setReturnCode(ApiConsts.RC_RSC_DFN_CREATED);
            successEntry.setMessageFormat(successMsg);
            successEntry.putVariable(KEY_RSC_NAME, rscNameStr);
            successEntry.putVariable(KEY_PEER_COUNT, Short.toString(peerCount));
            successEntry.putVariable(KEY_AL_STRIPES, Integer.toString(alStripes));
            successEntry.putVariable(KEY_AL_SIZE, Long.toString(alStripeSize));
            successEntry.putObjRef(KEY_RSC_DFN, rscNameStr);

            apiCallRc.addEntry(successEntry);
            controller.getErrorReporter().logInfo(successMsg);
        }
        catch (SQLException sqlExc)
        {
            String errorMessage = String.format(
                "A database error occured while creating the resource definition '%s'.",
                rscNameStr
            );
            controller.getErrorReporter().reportError(
                sqlExc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_SQL);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(sqlExc.getMessage());
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (AccessDeniedException accExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String action;
            if (rscDfn == null)
            { // handle accDeniedExc1

                action = "create a new resource definition.";
                entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_ACC_DENIED_RSC_DFN);
            }
            else
            { // handle accDeniedExc2
                action = String.format(
                    "create a new volume definition on resource definition '%s'.",
                    rscNameStr
                );
                entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_ACC_DENIED_VLM_DFN);
                entry.putVariable(KEY_VLM_NR, Integer.toString(currentVlmDfnApi.getVolumeNr()));
                entry.putVariable(KEY_MINOR_NR, Integer.toString(currentVlmDfnApi.getMinorNr()));
                entry.putObjRef(KEY_VLM_NR, Integer.toString(currentVlmDfnApi.getVolumeNr()));
            }
            String errorMessage = String.format(
                "The access context (user: '%s', role: '%s') has no permission to %s",
                accCtx.subjectId.name.displayValue,
                accCtx.subjectRole.name.displayValue,
                action
            );
            controller.getErrorReporter().reportError(
                accExc,
                accCtx,
                client,
                errorMessage
            );
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(accExc.getMessage());
            entry.putVariable(KEY_ID, accCtx.subjectId.name.displayValue);
            entry.putVariable(KEY_ROLE, accCtx.subjectRole.name.displayValue);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (InvalidNameException nameExc)
        {
            // handle invalidNameExc1

            String errorMessage = String.format(
                "The specified resource name '%s' is not a valid.",
                rscNameStr
            );
            controller.getErrorReporter().reportError(
                nameExc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_INVLD_RSC_NAME);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(nameExc.getMessage());
            entry.putVariable(KEY_RSC_NAME, rscNameStr);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (ValueOutOfRangeException valOORangeExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String errorMessage;
            if (rscDfn == null)
            { // handle valOORangeExc1
                errorMessage = String.format(
                    "The specified tcp port %d is invalid.",
                    portInt
                );
                entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_INVLD_RSC_PORT);
            }
            else
            if (volNr == null)
            { // handle valOORangeExc1
                errorMessage = String.format(
                    "The specified volume number %d is invalid.",
                    currentVlmDfnApi.getVolumeNr()
                );
                entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_INVLD_VLM_NR);
            }
            else
            { // handle valOORangeExc2
                errorMessage = String.format(
                    "The specified minor number %d is invalid.",
                    currentVlmDfnApi.getMinorNr()
                );
                entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_INVLD_MINOR_NR);
                entry.putVariable(KEY_MINOR_NR, Integer.toString(currentVlmDfnApi.getVolumeNr()));
            }
            controller.getErrorReporter().reportError(
                valOORangeExc,
                accCtx,
                client,
                errorMessage
            );
            entry.setMessageFormat(errorMessage);
            entry.putVariable(KEY_VLM_NR, Integer.toString(currentVlmDfnApi.getVolumeNr()));
            entry.putVariable(KEY_RSC_DFN, rscNameStr);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
            entry.putObjRef(KEY_VLM_NR, Integer.toString(currentVlmDfnApi.getVolumeNr()));

            apiCallRc.addEntry(entry);
        }
        catch (MdException metaDataExc)
        {
            // handle mdExc1 and mdExc2
            String errorMessage = String.format(
                "The specified volume size %d is invalid.",
                currentVlmDfnApi.getSize()
            );
            controller.getErrorReporter().reportError(
                metaDataExc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_INVLD_VLM_SIZE);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(metaDataExc.getMessage());
            entry.putVariable(KEY_VLM_NR, Integer.toString(currentVlmDfnApi.getVolumeNr()));
            entry.putVariable(KEY_RSC_DFN, rscNameStr);
            entry.putVariable(KEY_VLM_SIZE, Long.toString(currentVlmDfnApi.getSize()));
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
            entry.putObjRef(KEY_VLM_NR, Integer.toString(currentVlmDfnApi.getVolumeNr()));

            apiCallRc.addEntry(entry);
        }
        catch (LinStorDataAlreadyExistsException alreadyExistsExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String errorMsg;
            if (rscDfn == null)
            {
                // handle alreadyExists1
                errorMsg = String.format(
                    "A resource definition with the name '%s' already exists.",
                    rscNameStr
                );
                entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_EXISTS_RSC_DFN);
            }
            else
            {
                // handle alreadyExists2
                errorMsg = String.format(
                    "A volume definition with the numer %d already exists in resource definition '%s'.",
                    currentVlmDfnApi.getVolumeNr(),
                    rscNameStr
                );
                entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_EXISTS_VLM_DFN);
                entry.putVariable(KEY_VLM_NR, Integer.toString(currentVlmDfnApi.getVolumeNr()));
                entry.putVariable(KEY_MINOR_NR, Integer.toString(currentVlmDfnApi.getMinorNr()));
                entry.putObjRef(KEY_VLM_NR, Integer.toString(currentVlmDfnApi.getVolumeNr()));
            }
            controller.getErrorReporter().reportError(
                alreadyExistsExc,
                accCtx,
                client,
                errorMsg
            );
            entry.setMessageFormat(errorMsg);
            entry.setCauseFormat(alreadyExistsExc.getMessage());
            entry.putVariable(KEY_RSC_NAME, rscNameStr);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
            apiCallRc.addEntry(entry);
        }
        catch (Exception | ImplementationError exc)
        {
            // handle any other exception
            String errorMessage = String.format(
                "An exception occured while creating a resource definition '%s'. ",
                rscNameStr
            );
            controller.getErrorReporter().reportError(
                exc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_UNKNOWN_ERROR);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(exc.getMessage());
            entry.putVariable(KEY_RSC_NAME, rscNameStr);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

            apiCallRc.addEntry(entry);
        }

        if (transMgr != null)
        {
            if (transMgr.isDirty())
            {
                // not committed -> error occurred
                try
                {
                    transMgr.rollback();
                }
                catch (SQLException sqlExc)
                {
                    String errorMessage = String.format(
                        "A database error occured while trying to rollback the creation of " +
                            "resource definition '%s'.",
                        rscNameStr
                    );
                    controller.getErrorReporter().reportError(
                        sqlExc,
                        accCtx,
                        client,
                        errorMessage
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(RC_RSC_DFN_CRT_FAIL_SQL_ROLLBACK);
                    entry.setMessageFormat(errorMessage);
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putObjRef(KEY_RSC_DFN, rscNameStr);

                    apiCallRc.addEntry(entry);
                }
            }
            controller.dbConnPool.returnConnection(transMgr);
        }
        return apiCallRc;
    }

    static VolumeNumber getVlmNr(VlmDfnApi vlmDfnApi, ResourceDefinition rscDfn, AccessContext accCtx) throws ValueOutOfRangeException
    {
        Integer vlmNrRaw = vlmDfnApi.getVolumeNr();
        VolumeNumber vlmNr;
        if (vlmNrRaw == null)
        {
            try
            {
                Iterator<VolumeDefinition> vlmDfnIt = rscDfn.iterateVolumeDfn(accCtx);
                TreeSet<Integer> occupiedVlmNrs = new TreeSet<>();
                while (vlmDfnIt.hasNext())
                {
                    VolumeDefinition vlmDfn = vlmDfnIt.next();
                    occupiedVlmNrs.add(vlmDfn.getVolumeNumber().value);
                }
                for (int idx = 0; idx < occupiedVlmNrs.size(); ++idx)
                {
                    if (!occupiedVlmNrs.contains(idx))
                    {
                        vlmNrRaw = idx;
                        break;
                    }
                }
                if (vlmNrRaw == null)
                {
                    vlmNrRaw = occupiedVlmNrs.size();
                }

                vlmNr = new VolumeNumber(vlmNrRaw);
            }
            catch (AccessDeniedException accDeniedExc)
            {
                throw new ImplementationError(
                    "ApiCtx does not have enough privileges to iterate vlmDfns",
                    accDeniedExc
                );
            }
            catch (ValueOutOfRangeException valueOutOfrangeExc)
            {
                throw new ImplementationError(
                    "Failed to find an unused valid volume number. Tried: '" + vlmNrRaw + "'",
                    valueOutOfrangeExc
                );
            }
        }
        else
        {
            vlmNr = new VolumeNumber(vlmNrRaw);
        }
        return vlmNr;
    }

    static MinorNumber getMinor(VlmDfnApi vlmDfnApi) throws ValueOutOfRangeException
    {
        Integer minor = vlmDfnApi.getMinorNr();
        MinorNumber minorNr;
        if (minor == null)
        {
            minor = MINOR_GEN.incrementAndGet(); // FIXME: instead of atomicInt use the poolAllocator
            try
            {
                minorNr = new MinorNumber(minor);
            }
            catch (ValueOutOfRangeException valueOutOfRangExc)
            {
                throw new ImplementationError(
                    "Failed to generate a valid minor number. Tried: '" + minor + "'",
                    valueOutOfRangExc
                );
            }
        }
        else
        {
            minorNr = new MinorNumber(minor);
        }
        return minorNr;
    }

    public ApiCallRc modifyRscDfn(
        AccessContext accCtx,
        Peer client,
        UUID rscDfnUuid,
        String rscNameStr,
        Integer portInt,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (AbsApiCallHandler basicallyThis = setCurrent(
                accCtx,
                client,
                ApiCallType.MODIFY,
                apiCallRc,
                null, // create new
                rscNameStr
            )
        )
        {
            ResourceName rscName = toRscName(rscNameStr);
            ResourceDefinitionData rscDfn = loadRscDfn(rscName);
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
                TcpPortNumber port = toTcpPort(portInt);
                rscDfn.setPort(accCtx, port);
            }
            if (!overrideProps.isEmpty() || !deletePropKeys.isEmpty())
            {
                Map<String, String> map = getProps(rscDfn).map();
                map.putAll(overrideProps);
                for (String delKey : deletePropKeys)
                {
                    map.remove(delKey);
                }
            }

            notifySatellites(rscDfn);
            success("Resource definition '" + rscNameStr + "' modified.");
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // failure was reported and added to returning apiCallRc
            // this is only for flow-control.
        }
        catch (Exception exc)
        {
            report(
                exc,
                "An unknown exception was thrown while modifying the resource definition '" + rscNameStr + "'.",
                ApiConsts.FAIL_UNKNOWN_ERROR
            );
        }
        catch (ImplementationError implErr)
        {
            report(
                implErr,
                "An implementation error was thrown while modifying the resource definition '" + rscNameStr + "'.",
                ApiConsts.FAIL_IMPL_ERROR
            );
        }

        return apiCallRc;
    }

    public ApiCallRc deleteResourceDefinition(AccessContext accCtx, Peer client, String rscNameStr)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        TransactionMgr transMgr = null;
        ResourceName resName = null;
        ResourceDefinitionData resDfn = null;
        Iterator<Resource> rscIterator = null;

        try
        {
            controller.rscDfnMapProt.requireAccess(accCtx, AccessType.CHANGE); // accDeniedExc1
            transMgr = new TransactionMgr(controller.dbConnPool.getConnection()); // sqlExc1

            resName = new ResourceName(rscNameStr); // invalidNameExc1
            resDfn = ResourceDefinitionData.getInstance( // accDeniedExc2, sqlExc2, dataAlreadyExistsExc1
                accCtx,
                resName,
                null, // port only needed if we want to persist this entry
                null, // rscFlags only needed if we want to persist this entry
                null, // secret only needed if we want to persist this object
                null, // transportType only needed if we want to persist this object
                transMgr,
                false, // do not persist this entry
                false // do not throw exception if the entry exists
            );

            if (resDfn != null)
            {
                resDfn.setConnection(transMgr);
                resDfn.markDeleted(accCtx); // accDeniedExc3, sqlExc3

                rscIterator = resDfn.iterateResource(apiCtx); // accDeniedExc4
                while (rscIterator.hasNext())
                {
                    Resource rsc = rscIterator.next();
                    rsc.setConnection(transMgr);
                    rsc.markDeleted(apiCtx); // accDeniedExc5, sqlExc4
                }

                transMgr.commit(); // sqlExc5

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(ApiConsts.RC_RSC_DFN_DELETED);
                entry.setMessageFormat(
                    String.format(
                        "Resource definition '%s' successfully deleted",
                        rscNameStr
                    )
                );
                entry.putObjRef(KEY_RSC_DFN, rscNameStr);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
                apiCallRc.addEntry(entry);

                // TODO: tell satellites to remove all the corresponding resources
                // TODO: if satellites are finished (or no satellite had such a resource deployed)
                //       remove the rscDfn from the DB and from controller.rscDfnMap
                controller.getErrorReporter().logInfo(
                    "Resource definition '%s' marked to be deleted",
                    rscNameStr
                );
            }
            else
            {
                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(ApiConsts.RC_RSC_DFN_DEL_WARN_NOT_FOUND);
                entry.setMessageFormat(
                    String.format(
                        "Resource definition '%s' was not deleted as it was not found",
                        rscNameStr
                    )
                );
                entry.putObjRef(KEY_RSC_DFN, rscNameStr);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
                apiCallRc.addEntry(entry);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            String errorMessage;
            Throwable exc;
            if (rscIterator == null)
            { // accDeniedExc1 && accDeniedExc2 && accDeniedExc3
                errorMessage = String.format(
                    "The access context (user: '%s', role: '%s') has no permission to " +
                        "delete the resource definition '%s'.",
                    accCtx.subjectId.name.displayValue,
                    accCtx.subjectRole.name.displayValue,
                    rscNameStr
                );
                exc = accDeniedExc;
            }
            else
            { // accDeniedExc4 && accDeniedExc5
                errorMessage = String.format(
                    "The resources depending on the resource definition '%s' could not be marked for "+
                        "deletion due to an implementation error.",
                    rscNameStr
                );
                exc = new ImplementationError(
                    "ApiContext does not haven sufficent permission to mark resources as deleted",
                    accDeniedExc
                );
            }
            controller.getErrorReporter().reportError(
                exc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DFN_DEL_FAIL_ACC_DENIED_RSC_DFN);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(accDeniedExc.getMessage());
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
            entry.putVariable(KEY_RSC_NAME, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (SQLException sqlExc)
        {
            String errorMessge = String.format(
                "A database error occured while deleting the resource definition '%s'.",
                rscNameStr
            );
            controller.getErrorReporter().reportError(
                sqlExc,
                accCtx,
                client,
                errorMessge
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DFN_DEL_FAIL_SQL);
            entry.setMessageFormat(errorMessge);
            entry.setCauseFormat(sqlExc.getMessage());
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
            entry.putVariable(KEY_RSC_DFN, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (InvalidNameException invalidNameExc)
        { // handle invalidNameExc1
            String errorMessage = String.format(
                "The given resource name '%s' is invalid.",
                rscNameStr
            );
            controller.getErrorReporter().reportError(
                invalidNameExc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DFN_DEL_FAIL_INVLD_RSC_NAME);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(invalidNameExc.getMessage());

            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
            entry.putVariable(KEY_RSC_NAME, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        { // handle LinStorAlreadyExistsExc1
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    String.format(
                        ".getInstance was called with failIfExists=false, still threw an AlreadyExistsException "+
                            "(Resource name: '%s')",
                        rscNameStr
                    ),
                    dataAlreadyExistsExc
                )
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DFN_DEL_FAIL_IMPL_ERROR);
            entry.setMessageFormat(
                String.format(
                    "Failed to delete the resource definition '%s' due to an implementation error.",
                    rscNameStr
                )
            );
            entry.setCauseFormat(dataAlreadyExistsExc.getMessage());
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
            entry.putVariable(KEY_RSC_NAME, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (Exception | ImplementationError exc)
        {
            // handle any other exception
            String errorMessage = String.format(
                "An unknown exception occured while deleting a resource definition '%s'. ",
                rscNameStr
            );
            controller.getErrorReporter().reportError(
                exc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DFN_DEL_FAIL_UNKNOWN_ERROR);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(exc.getMessage());
            entry.putVariable(KEY_RSC_NAME, rscNameStr);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

            apiCallRc.addEntry(entry);
        }

        if (transMgr != null)
        {
            if (transMgr.isDirty())
            {
                try
                {
                    transMgr.rollback();
                }
                catch (SQLException sqlExc)
                {
                    String errorMessage = String.format(
                        "A database error occured while trying to rollback the deletion of "+
                            "resource definition '%s'.",
                        rscNameStr
                    );
                    controller.getErrorReporter().reportError(
                        sqlExc,
                        accCtx,
                        client,
                        errorMessage
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(RC_RSC_DFN_DEL_FAIL_SQL_ROLLBACK);
                    entry.setMessageFormat(errorMessage);
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putObjRef(KEY_RSC_DFN, rscNameStr);

                    apiCallRc.addEntry(entry);
                }
            }
            controller.dbConnPool.returnConnection(transMgr);
        }
        return apiCallRc;
    }

    byte[] listResourceDefinitions(int msgId, AccessContext accCtx, Peer client)
    {
        ArrayList<ResourceDefinitionData.RscDfnApi> rscdfns = new ArrayList<>();
        try
        {
            controller.rscDfnMapProt.requireAccess(accCtx, AccessType.VIEW);// accDeniedExc1
            for (ResourceDefinition rscdfn : controller.rscDfnMap.values())
            {
                try
                {
                    rscdfns.add(rscdfn.getApiData(accCtx));
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

        try
        {
            return rscDfnListSerializer.getListMessage(msgId, rscdfns);
        }
        catch (IOException e)
        {
            controller.getErrorReporter().reportError(
                e,
                null,
                client,
                "Could not complete list message due to an IOException"
            );
        }

        return null;
    }

    private void notifySatellites(ResourceDefinitionData rscDfn)
    {
        try
        {
            // notify all peers that (at least one of) their resource has changed
            Iterator<Resource> rscIterator = rscDfn.iterateResource(apiCtx);
            while (rscIterator.hasNext())
            {
                Resource currentRsc = rscIterator.next();
                Peer peer = currentRsc.getAssignedNode().getPeer(apiCtx);

                if (peer.isConnected())
                {
                    Message message = peer.createMessage();
                    byte[] data = rscSerializer.getChangedMessage(currentRsc);
                    message.setData(data);
                    peer.sendMessage(message);
                }
                else
                {
                    String nodeName = currentRsc.getAssignedNode().getName().displayValue;
                    addAnswer(
                        "No active connection to satellite '" + nodeName + "'",
                        null,
                        "The satellite was added and the controller tries to (re-) establish connection to it." +
                        "The controller stored the new Resource and as soon the satellite is connected, it will " +
                        "receive this update.",
                        null,
                        ApiConsts.WARN_NOT_CONNECTED
                    );
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    "Failed to contact all satellites about a resource change",
                    accDeniedExc
                )
            );
        }
        catch (IllegalMessageStateException illegalMessageStateExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    "Controller could not send send a message to target node",
                    illegalMessageStateExc
                )
            );
        }
    }

    private short getAsShort(Map<String, String> props, String key, short defaultValue)
    {
        short ret = defaultValue;
        String value = props.get(key);
        if (value != null)
        {
            try
            {
                ret = Short.parseShort(value);
            }
            catch (NumberFormatException numberFormatExc)
            {
                // ignore and return the default value
            }
        }
        return ret;
    }

    private int getAsInt(Map<String, String> props, String key, int defaultValue)
    {
        int ret = defaultValue;
        String value = props.get(key);
        if (value != null)
        {
            try
            {
                ret = Integer.parseInt(value);
            }
            catch (NumberFormatException numberFormatExc)
            {
                // ignore and return the default value
            }
        }
        return ret;
    }

    private long getAsLong(Map<String, String> props, String key, long defaultValue)
    {
        long ret = defaultValue;
        String value = props.get(key);
        if (value != null)
        {
            try
            {
                ret = Long.parseLong(value);
            }
            catch (NumberFormatException numberFormatExc)
            {
                // ignore and return the default value
            }
        }
        return ret;
    }

    @Override
    protected TransactionMgr createNewTransMgr() throws ApiCallHandlerFailedException
    {
        try
        {
            return new TransactionMgr(controller.dbConnPool);
        }
        catch (SQLException sqlExc)
        {
            throw sqlExc(
                sqlExc,
                "create a transaction manager"
            );
        }
    }

    @Override
    protected void rollbackIfDirty()
    {
        TransactionMgr transMgr = currentTransMgr.get();
        if (transMgr != null)
        {
            if (transMgr.isDirty())
            {
                // not committed -> error occurred
                try
                {
                    transMgr.rollback();
                }
                catch (SQLException sqlExc)
                {
                    report(
                        sqlExc,
                        "A database error occured while trying to rollback the " +
                        getAction("creation", "deletion", "modification") +
                        " of resource definition '" + currentRscNameStr.get() + "'.",
                        ApiConsts.FAIL_SQL_ROLLBACK
                    );
                }
            }
            controller.dbConnPool.returnConnection(transMgr);
        }
    }

    private AbsApiCallHandler setCurrent(
        AccessContext accCtx,
        Peer peer,
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        TransactionMgr transMgr,
        String rscNameStr
    )
    {
        super.setCurrent(accCtx, peer, type, apiCallRc, transMgr);

        currentRscNameStr.set(rscNameStr);

        Map<String, String> objRefs = currentObjRefs.get();
        objRefs.clear(); // just to be sure
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);

        Map<String, String> vars = currentVariables.get();
        vars.clear();
        vars.put(ApiConsts.KEY_RSC_NAME, rscNameStr);

        return this;
    }

    @Override
    public void close() throws Exception
    {
        super.close();
        currentRscNameStr.set(null);
    }


    private ResourceName toRscName(String rscNameStr)
    {
        try
        {
            return new ResourceName(rscNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            report(
                invalidNameExc,
                "The given resource name ('" + rscNameStr + "') is invalid.",
                ApiConsts.FAIL_INVLD_RSC_NAME
            );
            throw new ApiCallHandlerFailedException();
        }
    }

    private ResourceDefinitionData loadRscDfn(ResourceName rscName)
    {
        try
        {
            return ResourceDefinitionData.getInstance(
                currentAccCtx.get(),
                rscName,
                null, // port only needed if we want to persist this entry
                null, // rscFlags only needed if we want to persist this entry
                null, // secret only needed if we want to persist this object
                null, // transportType only needed if we want to persist this object
                currentTransMgr.get(),
                false,
                false
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw accDeniedExc(
                accDeniedExc,
                "load resource definition '" + rscName.displayValue + "'.",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ImplementationError(
                "Loading resource definition caused DataAlreadyExistsExc.",
                dataAlreadyExistsExc
            );
        }
        catch (SQLException sqlExc)
        {
            throw sqlExc(sqlExc, "loading resource definition '" + rscName.displayValue + "'.");
        }
    }

    private TcpPortNumber toTcpPort(int port)
    {
        try
        {
            return new TcpPortNumber(port);
        }
        catch (ValueOutOfRangeException e)
        {
            throw exc(
                e,
                "The given port number '" + port + "' is not valid.",
                ApiConsts.FAIL_INVLD_RSC_PORT
            );
        }
    }

    private Props getProps(ResourceDefinition rscDfn)
    {
        try
        {
            return rscDfn.getProps(currentAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw accDeniedExc(
                accDeniedExc,
                "access resource definition '" + rscDfn.getName().displayValue +
                "'s properties",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
    }
}
