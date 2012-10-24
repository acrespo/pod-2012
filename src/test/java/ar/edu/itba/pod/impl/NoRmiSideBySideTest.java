package ar.edu.itba.pod.impl;

import ar.edu.itba.pod.api.SignalProcessor;
import ar.edu.itba.pod.legajo51190.impl.MultiThreadedSignalProcessor;

/**
 * Side by side test without rmi for inspecting the performance of the internal
 * methods.
 * 
 * @author cris
 */
public class NoRmiSideBySideTest extends SideBySideTester {

	private static SignalProcessor sp;

	@Override
	protected SignalProcessor init() throws Exception {
		if (sp == null) {
			sp = new MultiThreadedSignalProcessor(2);
		}
		((MultiThreadedSignalProcessor) sp).exit();
		return sp;
	}

}
