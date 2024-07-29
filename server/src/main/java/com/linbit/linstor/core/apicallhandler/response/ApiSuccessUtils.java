package com.linbit.linstor.core.apicallhandler.response;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.EntryBuilder;
import com.linbit.linstor.api.ApiConsts;

import static com.linbit.utils.StringUtils.firstLetterCaps;

import java.util.UUID;

public class ApiSuccessUtils
{
    /**
     * The term 'created' is used for Linstor internal constructs without any corresponding external entity.
     * This means that the creation process is complete when the entity has been saved to the DB.
     */
    public static ApiCallRc.RcEntry defaultCreatedEntry(UUID uuid, String objectDescriptionInline)
    {
        return defaultNewEntry(uuid, objectDescriptionInline, "created");
    }

    /**
     * The term 'registered' is used for entities with a corresponding external entity.
     * This means that the external entity still needs to be created (e.g. resources),
     * or that the external entity already exists and just needs to be used (e.g. storage pools).
     */
    public static ApiCallRc.RcEntry defaultRegisteredEntry(UUID uuid, String objectDescriptionInline)
    {
        return defaultNewEntry(uuid, objectDescriptionInline, "registered");
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

    public static ApiCallRc.RcEntry defaultDeletedEntry(@Nullable UUID uuid, String objectDescriptionInline)
    {
        EntryBuilder entryBuilder = ApiCallRcImpl.entryBuilder(
            ApiConsts.DELETED,
            firstLetterCaps(objectDescriptionInline) + " deleted."
        );
        if (uuid != null)
        {
            entryBuilder = entryBuilder.setDetails(
                firstLetterCaps(objectDescriptionInline) + " UUID was: " + uuid.toString()
            )
                .putObjRef(ApiConsts.KEY_UUID, uuid.toString());
        }
        return entryBuilder.build();
    }

    private static ApiCallRc.RcEntry defaultNewEntry(
        UUID uuid, String objectDescriptionInline, String operationPastTense)
    {
        return ApiCallRcImpl
            .entryBuilder(
                ApiConsts.CREATED,
                "New " + objectDescriptionInline + " " + operationPastTense + "."
            )
            .setDetails(
                firstLetterCaps(objectDescriptionInline) + " UUID is: " + uuid.toString()
            )
            .putObjRef(ApiConsts.KEY_UUID, uuid.toString())
            .build();
    }

    private ApiSuccessUtils()
    {
    }
}
