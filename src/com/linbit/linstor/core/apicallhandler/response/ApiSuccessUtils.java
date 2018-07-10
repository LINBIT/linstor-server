package com.linbit.linstor.core.apicallhandler.response;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;

import java.util.UUID;

import static com.linbit.utils.StringUtils.firstLetterCaps;

public class ApiSuccessUtils
{
    public static ApiCallRc.RcEntry defaultCreatedEntry(UUID uuid, String objectDescriptionInline)
    {
        return ApiCallRcImpl
            .entryBuilder(
                ApiConsts.CREATED,
                "New " + objectDescriptionInline + " created."
            )
            .setDetails(
                firstLetterCaps(objectDescriptionInline) + " UUID is: " + uuid.toString()
            )
            .putObjRef(ApiConsts.KEY_UUID, uuid.toString())
            .build();
    }

    public static ApiCallRc.RcEntry defaultModifiedEntry(UUID uuid, String objectDescriptionInline)
    {
        return ApiCallRcImpl
            .entryBuilder(
                ApiConsts.MODIFIED,
                firstLetterCaps(objectDescriptionInline) + " modified."
            )
            .setDetails(
                firstLetterCaps(objectDescriptionInline) + " UUID is: " + uuid.toString()
            )
            .putObjRef(ApiConsts.KEY_UUID, uuid.toString())
            .build();
    }

    public static ApiCallRc.RcEntry defaultDeletedEntry(UUID uuid, String objectDescriptionInline)
    {
        return ApiCallRcImpl
            .entryBuilder(
                ApiConsts.DELETED,
                firstLetterCaps(objectDescriptionInline) + " deleted."
            )
            .setDetails(
                firstLetterCaps(objectDescriptionInline) + " UUID was: " + uuid.toString()
            )
            .putObjRef(ApiConsts.KEY_UUID, uuid.toString())
            .build();
    }

    private ApiSuccessUtils()
    {
    }
}
