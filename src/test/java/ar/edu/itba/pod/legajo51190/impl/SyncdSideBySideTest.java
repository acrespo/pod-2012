package ar.edu.itba.pod.legajo51190.impl;

import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Set;

import org.jgroups.ChannelListener;

import ar.edu.itba.pod.api.Signal;
import ar.edu.itba.pod.api.SignalProcessor;
import ar.edu.itba.pod.impl.SideBySideTester;

public class SyncdSideBySideTest extends SideBySideTester {

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

	@Override
	protected void addNoise(final int amount) throws RemoteException {
		SignalNode target = controller.getNodesToTest().getLast();
		controller.prepareNodeForSendingSignals(target);
		for (int i = 0; i < amount; i++) {
			Signal s = src.next();
			reference.add(s);
			controller.addSignalToNode(target, s);
		}
		controller.awaitSignalsSentToNode(target);
	}
}
