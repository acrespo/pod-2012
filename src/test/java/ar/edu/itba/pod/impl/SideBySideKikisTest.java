package ar.edu.itba.pod.impl;

import java.rmi.Remote;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import ar.edu.itba.pod.api.SignalProcessor;
import ar.edu.itba.pod.legajo51190.impl.MultiThreadedSignalProcessor;

public class SideBySideKikisTest extends SideBySideExampleTester {

	// Jorrible, but works
	private static boolean rmiServerStarted = false;

	@Override
	protected SignalProcessor init() throws Exception {
		if (!rmiServerStarted) {
			System.out
					.println("Starting Own SignalProcessor and showing via RMI...");
			Registry reg = LocateRegistry.createRegistry(20000);
			SignalProcessor sp = new MultiThreadedSignalProcessor(2);
			Remote proxy = UnicastRemoteObject.exportObject(sp, 0);
			reg.bind("SignalProcessor", proxy);
			reg.bind("SPNode", proxy);
			rmiServerStarted = true;
		}

		return super.init();
	}

}
