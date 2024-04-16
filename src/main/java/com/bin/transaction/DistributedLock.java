package com.bin.transaction;

/**
 *  锁住的时间,1分钟
 */
public interface DistributedLock {

    boolean isLock();




    DistributedLock EMPTY = new DistributedLock() {
        @Override
        public boolean isLock() {
            return true;
        }
    };
}
