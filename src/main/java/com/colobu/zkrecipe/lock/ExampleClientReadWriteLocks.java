package com.colobu.zkrecipe.lock;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;

/**
 * 可重入读写锁Shared Reentrant Read Write Lock
 * http://colobu.com/2014/12/12/zookeeper-recipes-by-example-2/
 *
 * 类似JDK的ReentrantReadWriteLock.
 * 一个读写锁管理一对相关的锁。 一个负责读操作，另外一个负责写操作。
 * 读操作在写锁没被使用时可同时由多个进程使用，而写锁使用时不允许读 (阻塞)。
 * 此锁是可重入的。一个拥有写锁的线程可重入读锁，但是读锁却不能进入写锁。
 * 这也意味着写锁可以降级成读锁， 比如请求写锁 --->读锁 ---->释放写锁。
 * 从读锁升级成写锁是不成的。
 */
public class ExampleClientReadWriteLocks {
	private final InterProcessReadWriteLock lock;
	private final InterProcessMutex readLock;
	private final InterProcessMutex writeLock;
	private final FakeLimitedResource resource;
	private final String clientName;

	public ExampleClientReadWriteLocks(CuratorFramework client, String lockPath, FakeLimitedResource resource, String clientName) {
		this.resource = resource;
		this.clientName = clientName;
		lock = new InterProcessReadWriteLock(client, lockPath);
		readLock = lock.readLock();
		writeLock = lock.writeLock();
	}

	public void doWork(long time, TimeUnit unit) throws Exception {
		if (!writeLock.acquire(time, unit)) {
			throw new IllegalStateException(clientName + " could not acquire the writeLock");
		}
		System.out.println(clientName + " has the writeLock");
		
		if (!readLock.acquire(time, unit)) {
			throw new IllegalStateException(clientName + " could not acquire the readLock");
		}
		System.out.println(clientName + " has the readLock too");
		
		try {			
			resource.use(); //access resource exclusively
		} finally {
			System.out.println(clientName + " releasing the lock");
			readLock.release(); // always release the lock in a finally block
			writeLock.release(); // always release the lock in a finally block
		}
	}
}