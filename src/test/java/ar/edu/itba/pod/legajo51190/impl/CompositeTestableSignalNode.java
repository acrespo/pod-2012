package ar.edu.itba.pod.legajo51190.impl;

import java.rmi.RemoteException;

import ar.edu.itba.pod.api.NodeStats;
import ar.edu.itba.pod.api.Result;
import ar.edu.itba.pod.api.Signal;
import ar.edu.itba.pod.api.SignalProcessor;

/**
 * SignalNode implementation for testing a SPNode and SignalProcessor Requires
 * the use of a {@link SyncListener} passed as parameter into the
 * {@link SignalProcessor} in order to listen to the connection and
 * disconnection of a channel. Otherwise, join and exit methods MUST be
 * blocking.
 * 
 * @author cris
 * 
 */
public class CompositeTestableSignalNode implements SignalNode {

	private final JGroupSignalProcessor processor;
	private final SyncListener injectedListener;

	public CompositeTestableSignalNode(final JGroupSignalProcessor processor,
			final SyncListener injectedListener) {
		this.processor = processor;
		this.injectedListener = injectedListener;
	}

	@Override
	public void add(final Signal signal) throws RemoteException {
		processor.add(signal);
	}

	@Override
	public Result findSimilarTo(final Signal signal) throws RemoteException {
		return processor.findSimilarTo(signal);
	}

	@Override
	public void join(final String clusterName) throws RemoteException {
		processor.join(clusterName);
	}

	@Override
	public void exit() throws RemoteException {
		processor.exit();
	}

	@Override
	public NodeStats getStats() throws RemoteException {
		return processor.getStats();
	}

	@Override
	public JGroupNode getJGroupNode() {
		return processor.getJGroupNode();
	}

	@Override
	public SyncListener getInjectedListener() {
		return injectedListener;
	}
}
