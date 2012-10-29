package ar.edu.itba.pod.legajo51190.impl;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.jgroups.ChannelListener;

import ar.edu.itba.pod.api.NodeStats;
import ar.edu.itba.pod.api.Result;
import ar.edu.itba.pod.api.Result.Item;
import ar.edu.itba.pod.api.SPNode;
import ar.edu.itba.pod.api.Signal;
import ar.edu.itba.pod.api.SignalProcessor;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * SignalProcessor implementation
 * 
 * @author cris
 */
public class MultiThreadedDistributedSignalProcessor implements
		JGroupSignalProcessor, SignalProcessor, SPNode {

	private final ListeningExecutorService localProcessingService;
	private final ExecutorService remoteProcessingService;
	private final int threads;
	private final NodeReceiver networkState;
	private final Node node;
	@SuppressWarnings("unused")
	private final NodeLogger nodeLogger;

	public MultiThreadedDistributedSignalProcessor(final int threads)
			throws Exception {
		this(threads, null, null);
	}

	public MultiThreadedDistributedSignalProcessor(final int threads,
			final Set<ChannelListener> listeners,
			final NodeListener nodeListener) throws Exception {
		remoteProcessingService = Executors.newCachedThreadPool();
		this.threads = threads;
		localProcessingService = MoreExecutors.listeningDecorator(Executors
				.newFixedThreadPool(threads));
		node = new Node(nodeListener);
		networkState = new NodeReceiver(node);
		node.getChannel().setDiscardOwnMessages(true);

		if (networkState != null) {
			node.getChannel().setReceiver(networkState);
			node.getChannel().addChannelListener(networkState);
		}

		if (listeners != null) {
			for (ChannelListener channelListener : listeners) {
				node.getChannel().addChannelListener(channelListener);
			}
		}

		nodeLogger = new NodeLogger(node);
	}

	@Override
	public void join(final String clusterName) throws RemoteException {
		try {
			node.joinChannel(clusterName);
		} catch (Exception e) {
			throw new RemoteException(e.getMessage());
		}
	}

	@Override
	public void exit() throws RemoteException {
		node.exit();
	}

	@Override
	public NodeStats getStats() throws RemoteException {
		return node.getStats();
	}

	@Override
	public void add(final Signal signal) throws RemoteException {
		synchronized (node.getToDistributeSignals()) {
			node.getToDistributeSignals().add(signal);
		}
	}

	@Override
	public Result findSimilarTo(final Signal signal) throws RemoteException {
		if (signal == null) {
			throw new IllegalArgumentException("Signal cannot be null");
		}

		Result result = new Result(signal);

		final BlockingQueue<Signal> querySignals = buildQuerySignalSet();

		List<LocalSearchCall> queries = new ArrayList<>();
		for (int i = 0; i < threads; i++) {
			queries.add(new LocalSearchCall(querySignals, signal));
		}

		try {
			List<Future<List<Item>>> results = localProcessingService
					.invokeAll(queries);

			for (Future<List<Item>> future : results) {
				for (Item item : future.get()) {
					result = result.include(item);
				}
			}

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

		return result;
	}

	private BlockingQueue<Signal> buildQuerySignalSet() {
		final BlockingQueue<Signal> querySignals = new LinkedBlockingQueue<>();

		synchronized (node.getToDistributeSignals()) {
			synchronized (node.getLocalSignals()) {
				querySignals.addAll(node.getToDistributeSignals());
				querySignals.addAll(node.getLocalSignals());
			}
		}
		return querySignals;
	}

	@Override
	public JGroupNode getJGroupNode() {
		return node;
	}
}
