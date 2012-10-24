package ar.edu.itba.pod.impl;

import ar.edu.itba.pod.api.SignalProcessor;
import ar.edu.itba.pod.legajo51190.impl.MultiThreadedSignalProcessor;

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
			sp = new MultiThreadedSignalProcessor(Runtime.getRuntime()
					.availableProcessors());
		}
		((MultiThreadedSignalProcessor) sp).exit();
		return sp;
	}
}
