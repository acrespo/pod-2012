package ar.edu.itba.pod.legajo51190.impl;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
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
		signals.add(signal);
	}

	@Override
	public Result findSimilarTo(final Signal signal) throws RemoteException {
		if (signal == null) {
			throw new IllegalArgumentException("Signal cannot be null");
		}

		Result result = new Result(signal);

		final BlockingQueue<Signal> querySignals = new LinkedBlockingQueue<>();

		querySignals.addAll(signals);

		List<SearchCallable> queries = new ArrayList<>();

		for (int i = 0; i < threads; i++) {
			queries.add(new SearchCallable(querySignals, signal));
		}

		try {
			List<Future<List<Item>>> results = service.invokeAll(queries);
			System.out.println("Comparing future sizes");
			for (Future<List<Item>> future : results) {
				System.out.println(future.get().size());
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
}
