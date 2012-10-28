package ar.edu.itba.pod.legajo51190.impl;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.jgroups.ChannelListener;
import org.jgroups.JChannel;

import ar.edu.itba.pod.api.NodeStats;
import ar.edu.itba.pod.api.Result;
import ar.edu.itba.pod.api.Result.Item;
import ar.edu.itba.pod.api.SPNode;
import ar.edu.itba.pod.api.Signal;
import ar.edu.itba.pod.api.SignalProcessor;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class MultiThreadedSignalProcessor implements SignalProcessor, SPNode {
	private final Set<Signal> signals = Collections
			.newSetFromMap(new ConcurrentHashMap<Signal, Boolean>());
	private final Set<Signal> toDistributeSignals = Collections
			.newSetFromMap(new ConcurrentHashMap<Signal, Boolean>());
	private final ListeningExecutorService localProcessingService;
	private final int threads;
	private final JChannel channel;
	private final NodeReceiver networkState;
	private final Node node;
	private final NodeLogger nodeLogger;

	public MultiThreadedSignalProcessor(final int threads) throws Exception {
		this(threads, null);
	}

	public MultiThreadedSignalProcessor(final int threads,
			final Set<ChannelListener> listeners) throws Exception {
		this.threads = threads;
		localProcessingService = MoreExecutors.listeningDecorator(Executors
				.newFixedThreadPool(threads));
		channel = new JChannel("udp-largecluster.xml");
		node = new Node(signals, channel, toDistributeSignals);
		networkState = new NodeReceiver(node);
		channel.setDiscardOwnMessages(true);

		if (networkState != null) {
			channel.setReceiver(networkState);
			channel.addChannelListener(networkState);
		}

		if (listeners != null) {
			for (ChannelListener channelListener : listeners) {
				channel.addChannelListener(channelListener);
			}
		}

		nodeLogger = new NodeLogger(node);
	}

	@Override
	public void join(final String clusterName) throws RemoteException {
		try {
			channel.connect(clusterName);
		} catch (Exception e) {
			throw new RemoteException(e.getMessage());
		}
	}

	@Override
	public void exit() throws RemoteException {
		signals.clear();
		toDistributeSignals.clear();
		channel.close();
	}

	@Override
	public NodeStats getStats() throws RemoteException {
		return node.getStats();
	}

	@Override
	public void add(final Signal signal) throws RemoteException {
		synchronized (toDistributeSignals) {
			toDistributeSignals.add(signal);
		}
	}

	@SuppressWarnings("unused")
	@Override
	public Result findSimilarTo(final Signal signal) throws RemoteException {
		if (signal == null) {
			throw new IllegalArgumentException("Signal cannot be null");
		}

		Result result = new Result(signal);

		final BlockingQueue<Signal> querySignals = new LinkedBlockingQueue<>();

		synchronized (toDistributeSignals) {
			synchronized (signals) {
				querySignals.addAll(toDistributeSignals);
				querySignals.addAll(signals);
			}
		}

		List<LocalSearchCall> queries = new ArrayList<>();

		for (int i = 0; i < threads; i++) {
			queries.add(new LocalSearchCall(querySignals, signal));
		}

		long t1, t2, t3;

		try {
			t1 = System.currentTimeMillis();
			List<Future<List<Item>>> results = localProcessingService
					.invokeAll(queries);
			t2 = System.currentTimeMillis();

			for (Future<List<Item>> future : results) {
				for (Item item : future.get()) {
					result = result.include(item);
				}
			}
			t3 = System.currentTimeMillis();

			// System.out.println("---- Multithreaded time: " + (t2 - t1));
			// System.out.println("---- Join time: " + (t3 - t2));
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

		return result;
	}

	JGroupNode getJGroupNode() {
		return node;
	}
}
