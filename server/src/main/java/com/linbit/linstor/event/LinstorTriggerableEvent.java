package com.linbit.linstor.event;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;

public interface LinstorTriggerableEvent<T> extends LinstorEvent<T>
{
    void triggerEvent(ObjectIdentifier objectIdentifier, @Nullable T value);

    void closeStream(ObjectIdentifier objectIdentifier);

    void closeStreamNoConnection(ObjectIdentifier objectIdentifier);

    /**
     * This extra method is just a temporary solution. The better option would be if the nullable-annotations on generic
     * types work correctly in spotbugs, but until then this will have to be enough.
     *
     * @param objectIdentifier
     * @param eventStreamAction
     */
    default void forwardEvent(ObjectIdentifier objectIdentifier, String eventStreamAction)
    {
        switch (eventStreamAction)
        {
            case InternalApiConsts.EVENT_STREAM_VALUE:
                throw new ImplementationError("Calling EVENT_STREAM_VALUE without a value");
            case InternalApiConsts.EVENT_STREAM_CLOSE_REMOVED:
                closeStream(objectIdentifier);
                break;
            case InternalApiConsts.EVENT_STREAM_CLOSE_NO_CONNECTION:
                closeStreamNoConnection(objectIdentifier);
                break;
            default:
                throw new ImplementationError("Unknown event action '" + eventStreamAction + "'");
        }
    }

    default void forwardEvent(ObjectIdentifier objectIdentifier, String eventStreamAction, T value)
    {
        switch (eventStreamAction)
        {
            case InternalApiConsts.EVENT_STREAM_VALUE:
                triggerEvent(objectIdentifier, value);
                break;
            case InternalApiConsts.EVENT_STREAM_CLOSE_REMOVED:
                closeStream(objectIdentifier);
                break;
            case InternalApiConsts.EVENT_STREAM_CLOSE_NO_CONNECTION:
                closeStreamNoConnection(objectIdentifier);
                break;
            default:
                throw new ImplementationError("Unknown event action '" + eventStreamAction + "'");
        }
    }
}
