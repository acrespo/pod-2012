package ar.edu.itba.pod.impl;

import java.rmi.Remote;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import ar.edu.itba.pod.api.SignalProcessor;

public class SideBySideKikisTest extends SideBySideExampleTester {

	// Jorrible, but works
	private static boolean rmiServerStarted = false;

	@Override
	protected SignalProcessor init() throws Exception {
		if (!rmiServerStarted) {
			System.out.println("Starting rmi");
			Registry reg = LocateRegistry.createRegistry(20000);
			SignalProcessor sp = new StandaloneSignalProcessor();
			Remote proxy = UnicastRemoteObject.exportObject(sp, 0);
			reg.bind("SignalProcessor", proxy);
			reg.bind("SPNode", proxy);
			rmiServerStarted = true;
		}

		return super.init();
	}

}
