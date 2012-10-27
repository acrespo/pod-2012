package ar.edu.itba.pod.legajo51190.impl;

import java.util.HashSet;
import java.util.Set;

import org.jgroups.ChannelListener;

import ar.edu.itba.pod.legajo51190.api.CompositeTestableSignalNode;
import ar.edu.itba.pod.legajo51190.api.SignalNode;

public class MyDistributedNodeTest extends AbstractDistributedNodeTest {

	@Override
	public SignalNode createNewSignalNode(final CountdownSyncListener listener) {

		Set<ChannelListener> listeners = new HashSet<>();

		listeners.add(listener);

		MultiThreadedSignalProcessor processor;
		try {
			processor = new MultiThreadedSignalProcessor(2, listeners);
			return new CompositeTestableSignalNode(processor, processor,
					listener);
		} catch (Exception e) {
			return null; // Critical error
		}
	}

}
