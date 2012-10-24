package ar.edu.itba.pod.legajo51190.impl;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
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
	private final ListeningExecutorService service;
	private final int threads;

	public MultiThreadedSignalProcessor(final int threads) {
		this.threads = threads;
		service = MoreExecutors.listeningDecorator(Executors
				.newFixedThreadPool(threads));
	}

	private static class SearchCallable implements Callable<List<Result.Item>> {

		private final Iterator<Signal> querySignals;
		private final Signal signal;

		public SearchCallable(final Iterator<Signal> querySignals,
				final Signal signal) {
			this.querySignals = querySignals;
			this.signal = signal;
		}

		@Override
		public List<Item> call() throws Exception {
			List<Item> items = new ArrayList<>();
			while (querySignals.hasNext()) {
				try {
					Signal toAnalyze = querySignals.next();
					if (toAnalyze != null) {
						items.add(new Result.Item(toAnalyze, signal
								.findDeviation(toAnalyze)));
					}
				} catch (NoSuchElementException e) {

				}
			}
			return items;
		}
	}

	@Override
	public void join(final String clusterName) throws RemoteException {
		throw new NotImplementedException();
	}

	@Override
	public void exit() throws RemoteException {
		signals.clear();
	}

	@Override
	public NodeStats getStats() throws RemoteException {
		throw new NotImplementedException();
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

		final Iterator<Signal> querySignals = signals.iterator();

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
}
