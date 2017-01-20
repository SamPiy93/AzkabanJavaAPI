package locks;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Implementation of ReadWriteLockProvider interface
 *
 */
public class ReEntrantReadWriteLockProvider implements ReadWriteLockProvider {
    private ReentrantReadWriteLock reentrantReadWriteLock;
    private Lock readLock;
    private Lock writeLock;

    /**
     * Default constructor to instantiate ReEntrantReadWriteLockProvider
     */
    public ReEntrantReadWriteLockProvider() {
        this.reentrantReadWriteLock = new ReentrantReadWriteLock();
        this.readLock = reentrantReadWriteLock.readLock();
        this.writeLock = reentrantReadWriteLock.writeLock();
    }

    /**
     * Acquire read lock
     */
    @Override
    public void acquireReadLock() {
        readLock.lock();
    }

    /**
     * Release read lock
     */
    @Override
    public void releaseReadLock() {
        readLock.unlock();
    }

    /**
     * Acquire write lock
     */
    @Override
    public void acquireWriteLock() {
        writeLock.lock();
    }

    /**
     * Release write lock
     */
    @Override
    public void releaseWriteLock() {
        writeLock.unlock();
    }

    /**
     * Method to check if write lock has been acquired by current thread
     *
     * @return Whether Write Lock id acquired by current thread
     */
    @Override
    public boolean isWriteLockAcquiredByCurrentThread() {
        return reentrantReadWriteLock.isWriteLockedByCurrentThread();
    }
}

