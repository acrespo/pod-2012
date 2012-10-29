package ar.edu.itba.pod.impl;

import ar.edu.itba.pod.api.SignalProcessor;
import ar.edu.itba.pod.legajo51190.impl.MultiThreadedDistributedSignalProcessor;

/**
 * Side by side test without rmi for inspecting the performance of the internal
 * methods.
 * 
 * @author cris
 */
public class MyImplVsStandardImplTest extends SideBySideTester {

	private static SignalProcessor sp;

	@Override
	protected SignalProcessor init() throws Exception {
		if (sp == null) {
			System.out.println("Using "
					+ Runtime.getRuntime().availableProcessors()
					+ " processors");
			sp = new MultiThreadedDistributedSignalProcessor(Runtime.getRuntime()
					.availableProcessors());
		}
		((MultiThreadedDistributedSignalProcessor) sp).exit();
		return sp;
	}
}
