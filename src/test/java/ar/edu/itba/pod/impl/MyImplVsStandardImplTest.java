package ar.edu.itba.pod.impl;

import java.util.HashSet;
import java.util.Set;

import org.jgroups.ChannelListener;

import ar.edu.itba.pod.api.SignalProcessor;
import ar.edu.itba.pod.legajo51190.impl.CompositeTestableSignalNode;
import ar.edu.itba.pod.legajo51190.impl.MultiThreadedDistributedSignalProcessor;
import ar.edu.itba.pod.legajo51190.impl.NodeTestController;
import ar.edu.itba.pod.legajo51190.impl.SignalNode;
import ar.edu.itba.pod.legajo51190.impl.SignalNodeTestFactory;
import ar.edu.itba.pod.legajo51190.impl.SyncListener;

/**
 * Side by side test without rmi for inspecting the performance of the internal
 * methods.
 * 
 * @author cris
 */
public class MyImplVsStandardImplTest extends SideBySideTester {

	private static NodeTestController controller;

	@Override
	protected SignalProcessor init() throws Exception {
		if (controller == null) {
			controller = new NodeTestController(new SignalNodeTestFactory() {
				@Override
				public SignalNode getNewSignalNode(final SyncListener listener) {
					try {
						Set<ChannelListener> listeners = new HashSet<>();
						listeners.add(listener);
						return new CompositeTestableSignalNode(
								new MultiThreadedDistributedSignalProcessor(1,
										listeners, listener), listener);
					} catch (Exception e) {
						e.printStackTrace();
						return null;
					}
				}
			});
			controller.addNewNodes(5);
		} else {
			controller.addNewNodes(5);
		}
		return controller.getNodesToTest().getFirst();
	}

	@Override
	protected void clear() {
		controller.disconnectAllNodesFromChannel();
		controller.getNodesToTest().clear();
	}
}
