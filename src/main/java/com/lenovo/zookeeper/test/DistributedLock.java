package com.lenovo.zookeeper.test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NotEmptyException;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;

public class DistributedLock implements Lock{

	final static AtomicInteger index = new AtomicInteger(0);
	final static HashedWheelTimer scheduler = new HashedWheelTimer();
	private Object localLock;
	private static final String LOCK = "lock";
	private static final String LOCK_PATH = "/" + LOCK;
	private ZkClient client;
	private final static ThreadLocal<String> currentPath = new ThreadLocal<String>();;
	
	private String lockIndex;
	private String domain;
	private String lockPath;
	
	public DistributedLock(String connectionString)throws Exception{
		this.client = new ZkClient(connectionString);
		this.localLock = new Object();
		this.lockIndex =  index.getAndIncrement()+"";
		try {
			client.createPersistent(LOCK_PATH);
		} catch (ZkNodeExistsException e) {
		}
		domain = LOCK_PATH +"/"+ lockIndex;
		try {
			client.delete(domain);
		} catch (Exception e1) {
		}
		try {
			client.createPersistent(domain);
		} catch (ZkNodeExistsException e) {
		}
		lockPath = domain + LOCK_PATH;
		
	}
	public void lock() {
			synchronized(localLock) {				
				currentPath.set(client.createEphemeralSequential(lockPath, ""));
				while(true){
					List<String> paths = client.getChildren(domain);
					String minPath = getMinPath(LOCK, paths);
					if(minPath != null && currentPath.get() != null && currentPath.get().endsWith(minPath)){
						break;
					}else{
						try {
							localLock.wait(100);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
				
			}

	}

	private static final String getMinPath(String domainPath, List<String> paths){
		if(domainPath == null || paths == null)return null;
		int minIndex = 0;
		long firstPath  = Long.parseLong(paths.get(minIndex).replace(domainPath, ""));
		for(int i=1; i < paths.size(); i++){
			String path = paths.get(i);
			path = path.replace(domainPath, "");
			long nextPath = Long.parseLong(path);
			if(nextPath < firstPath){
				minIndex = i;
			}
		}
		return paths.get(minIndex);
	}
	
	public void lockInterruptibly() throws InterruptedException {
		synchronized(localLock) {
			currentPath.set(client.createEphemeralSequential(lockPath, ""));
			while(true){
				List<String> paths = client.getChildren(domain);
				String minPath = getMinPath(LOCK ,paths);
				if(minPath != null && currentPath.get() != null && currentPath.get().endsWith(minPath)){
					break;
				}else{
					try {
						localLock.wait(100);
					} catch (InterruptedException e) {
						throw e;
					}
				}
			}
		}

	}

	public boolean tryLock() {
		boolean flag = false;
		synchronized(localLock) {
			currentPath.set(client.createEphemeralSequential(lockPath, ""));
			List<String> paths = client.getChildren(domain);
			String minPath = getMinPath(LOCK, paths);
			if(minPath != null && currentPath.get() != null && currentPath.get().endsWith(minPath)){
				flag = true;
			}
		}
		return flag;
	}

	public boolean tryLock(long time, TimeUnit unit)
			throws InterruptedException {
		boolean flag = false;
		synchronized(localLock) {
			currentPath.set(client.createEphemeralSequential(lockPath, ""));
			final AtomicBoolean running = new AtomicBoolean(true);
			TimerTask task = new TimerTask(){
				public void run(Timeout timeout) throws Exception {
					running.compareAndSet(true, false);
				}
			};
			scheduler.newTimeout(task, time, unit);
			boolean fisrtRunning = true;//第一次可以进入查找
			while(fisrtRunning || running.get()){
				fisrtRunning = false;
				List<String> paths = client.getChildren(domain);
				String minPath = getMinPath(LOCK, paths);
				if(minPath != null && currentPath.get() != null && currentPath.get().endsWith(minPath)){
					flag = true;
					break;
				}
				try {
					localLock.wait(100);
				} catch (InterruptedException e) {
					throw e;
				}
			}
		}
		return flag;
		
	}

	public void unlock() {
		synchronized(localLock){
			client.delete(currentPath.get());
			localLock.notifyAll();
		}

	}

	public Condition newCondition() {
		return null;
	}
	@Override
	protected void finalize() throws Throwable {
		try {
			super.finalize();
		} catch (Exception e) {
			throw e;
		}finally{
			this.client.delete(domain);
		}
		
	}
	
	

}
