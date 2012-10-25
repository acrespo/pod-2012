package ar.edu.itba.pod.legajo51190.impl;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

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
	private final BlockingQueue<Signal> signals = new LinkedBlockingQueue<>();
	private final ListeningExecutorService localProcessingService;
	private final int threads;
	private final JChannel channel;
	private final NodeReceiver networkState;
	private final Node node;

	public MultiThreadedSignalProcessor(final int threads) throws Exception {
		this.threads = threads;
		localProcessingService = MoreExecutors.listeningDecorator(Executors
				.newFixedThreadPool(threads));
		channel = new JChannel();
		node = new Node(signals, channel);
		networkState = new NodeReceiver(node);

		if (networkState != null) {
			channel.setReceiver(networkState);
			channel.addChannelListener(networkState);
		}
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
	}

	@Override
	public NodeStats getStats() throws RemoteException {
		throw new RemoteException("Not yet implemented :D");
	}

	@Override
	public void add(final Signal signal) throws RemoteException {
		synchronized (signals) {
			signals.add(signal);
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

		synchronized (signals) {
			querySignals.addAll(signals);
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

}
