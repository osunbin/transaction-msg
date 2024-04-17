package com.bin.transaction;

/**
 *  锁住的时间,1分钟
 */
public interface DistributedLock {

    boolean lock();


    void unLock();

    DistributedLock EMPTY = new DistributedLock() {
        @Override
        public boolean lock() {
            return true;
        }

        @Override
        public void unLock() {

        }
    };
}
