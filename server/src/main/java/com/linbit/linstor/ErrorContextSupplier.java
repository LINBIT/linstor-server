package com.linbit.linstor;

import javax.annotation.Nullable;

public interface ErrorContextSupplier
{
    /**
     * Adds the returned String (if not null) to the ErrorReport.
     */
    @Nullable String getErrorContext();

    boolean hasErrorContext();
}
