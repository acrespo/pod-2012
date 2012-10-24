package ar.edu.itba.pod.legajo51190.impl;

import java.io.InputStream;
import java.io.OutputStream;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.ChannelListener;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.View;

import ar.edu.itba.pod.api.NodeStats;
import ar.edu.itba.pod.api.Result;
import ar.edu.itba.pod.api.Result.Item;
import ar.edu.itba.pod.api.SPNode;
import ar.edu.itba.pod.api.Signal;
import ar.edu.itba.pod.api.SignalProcessor;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class MultiThreadedSignalProcessor implements SignalProcessor, SPNode,
		Receiver, ChannelListener {
	private final BlockingQueue<Signal> signals = new LinkedBlockingQueue<>();
	private final ListeningExecutorService service;
	private final int threads;
	private final JChannel channel;

	public MultiThreadedSignalProcessor(final int threads) throws Exception {
		this.threads = threads;
		service = MoreExecutors.listeningDecorator(Executors
				.newFixedThreadPool(threads));
		channel = new JChannel();
	}

	private static class SearchCallable implements Callable<List<Result.Item>> {

		private final BlockingQueue<Signal> querySignals;
		private final Signal signal;

		public SearchCallable(final BlockingQueue<Signal> querySignals,
				final Signal signal) {
			super();
			this.querySignals = querySignals;
			this.signal = signal;
		}

		@Override
		public List<Item> call() throws Exception {
			List<Item> items = new ArrayList<>();
			while (!querySignals.isEmpty()) {
				Signal toAnalyze = querySignals.poll();
				if (toAnalyze != null) {
					items.add(new Result.Item(toAnalyze, signal
							.findDeviation(toAnalyze)));
				}
			}
			return items;
		}
	}

	@Override
	public void join(final String clusterName) throws RemoteException {
		try {
			channel.connect(clusterName);
		} catch (Exception e) {
			throw new RemoteException(e.getMessage());
		}
		channel.setReceiver(this);
		channel.addChannelListener(this);
		System.out.println("Joining cluster " + clusterName);
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

		List<SearchCallable> queries = new ArrayList<>();

		for (int i = 0; i < threads; i++) {
			queries.add(new SearchCallable(querySignals, signal));
		}

		long t1, t2, t3;

		try {
			t1 = System.currentTimeMillis();
			List<Future<List<Item>>> results = service.invokeAll(queries);
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

	@Override
	public void receive(final Message msg) {
	}

	@Override
	public void getState(final OutputStream output) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void setState(final InputStream input) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void viewAccepted(final View new_view) {
		System.out.println(new_view);
	}

	@Override
	public void suspect(final Address suspected_mbr) {
		System.out.println(suspected_mbr);
	}

	@Override
	public void block() {
		// TODO Auto-generated method stub

	}

	@Override
	public void unblock() {
		// TODO Auto-generated method stub

	}

	@Override
	public void channelConnected(final Channel channel) {
		System.out.println("Connected to " + channel);
	}

	@Override
	public void channelDisconnected(final Channel channel) {
		System.out.println("Disconected from " + channel);
	}

	@Override
	public void channelClosed(final Channel channel) {
		System.out.println("Channel closed " + channel);
	}
}
