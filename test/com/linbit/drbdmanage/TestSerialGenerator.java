package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.linbit.drbdmanage.propscon.SerialGenerator;

public class TestSerialGenerator implements SerialGenerator {
        
    private AtomicLong serial = new AtomicLong(0);
    private AtomicBoolean closeGen = new AtomicBoolean(false);
    
    @Override
    public synchronized long peekSerial() throws SQLException {
        return serial.get();
    }
    
    @Override
    public synchronized long newSerial() throws SQLException {
        if (closeGen.getAndSet(false))
        {
            return serial.incrementAndGet();
        }
        return serial.get();
    }
    
    @Override
    public synchronized void closeGeneration() {
        closeGen.set(true);
    }
}
