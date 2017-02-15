package com.colobu.zkrecipe.lock;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.framework.recipes.locks.Revoker;

/**
 * 可重入锁Shared Reentrant Lock
 * http://colobu.com/2014/12/12/zookeeper-recipes-by-example-2/
 */
public class ExampleClientReentrantLocks {
    /**
     * 将上面的InterProcessMutex换成不可重入锁InterProcessSemaphoreMutex,
     * 如果再运行上面的代码，结果就会发现线程被阻塞再第二个acquire上。
     * 也就是此锁不是可重入的。
     */
    private final InterProcessSemaphoreMutex lock;
    //private final InterProcessMutex lock;
    private final FakeLimitedResource resource;
    private final String clientName;

    public ExampleClientReentrantLocks(CuratorFramework client, String lockPath, FakeLimitedResource resource, String clientName) {
        this.resource = resource;
        this.clientName = clientName;
        //lock = new InterProcessMutex(client, lockPath);
        lock = new InterProcessSemaphoreMutex(client, lockPath);
    }

    public void doWork(long time, TimeUnit unit) throws Exception {
        if (!lock.acquire(time, unit)) {
            throw new IllegalStateException(clientName + " could not acquire the lock");
        }
        System.out.println(clientName + " has the lock");
        if (!lock.acquire(time, unit)) {
            throw new IllegalStateException(clientName + " could not acquire the lock");
        }
        System.out.println(clientName + " has the lock again");

        try {
            resource.use(); //access resource exclusively
        } finally {
            System.out.println(clientName + " releasing the lock");
            lock.release(); // always release the lock in a finally block
            // 如果少调用一次release，则此线程依然拥有锁。
            lock.release(); // always release the lock in a finally block
            System.out.println(lock.isAcquiredInThisProcess());
        }
    }
}