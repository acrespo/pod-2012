package ar.edu.itba.pod.legajo51190.impl;

import java.util.HashSet;
import java.util.Set;

import org.jgroups.ChannelListener;

public class MyDistributedNodeTest extends AbstractDistributedNodeTest {

	@Override
	public SignalNode createNewSignalNode(final SyncListener listener) {

		Set<ChannelListener> listeners = new HashSet<>();

		listeners.add(listener);

		MultiThreadedDistributedSignalProcessor processor;
		try {
			processor = new MultiThreadedDistributedSignalProcessor(2, listeners, listener);
			return new CompositeTestableSignalNode(processor, listener);
		} catch (Exception e) {
			return null; // Critical error
		}
	}

}
