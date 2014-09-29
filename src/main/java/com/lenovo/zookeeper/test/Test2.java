package com.lenovo.zookeeper.test;

import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.ZooKeeper;

public class Test2 extends Thread{
	DistributedLock lock;
	
	public Test2() {
		try {
			lock = new DistributedLock("103.249.129.24:2181,103.249.129.25:2181,103.249.129.26:2181","test");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public void run(){
		lock.lock();
		System.out.println("Thread.currentThread = " + Runtime.getRuntime() + " == "+Thread.currentThread().getId());
		try {
			Thread.currentThread().sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		lock.unlock();
	}
	
	public static void main(String[] args) {
		
		Test2 t = new Test2();
		Thread t1 = new Thread(t);
		Thread t2 = new Thread(t);
		t1.start();t2.start();
	}
}
