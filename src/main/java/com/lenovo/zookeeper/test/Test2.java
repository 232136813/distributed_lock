package com.lenovo.zookeeper.test;

import org.I0Itec.zkclient.ZkClient;

public class Test2 {
	public static void main(String[] args) {
		ZkClient client = new ZkClient("103.249.129.24:2181,103.249.129.25:2181,103.249.129.26:2181");
		String path = client.createEphemeralSequential("/data", "");
//		System.out.println(client.getChildren("/"));
//		client.delete(path);
//		System.out.println(client.getChildren("/"));
		client.delete("/op");
	}
}
