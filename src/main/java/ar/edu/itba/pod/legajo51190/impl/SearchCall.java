package ar.edu.itba.pod.legajo51190.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import ar.edu.itba.pod.api.Result;
import ar.edu.itba.pod.api.Result.Item;
import ar.edu.itba.pod.api.Signal;

/**
 * Callable for making a search inside a processor.
 * 
 * @author cris
 */
public class SearchCall implements Callable<List<Result.Item>> {

	private final BlockingQueue<Signal> querySignals;
	private final Signal signal;

	public SearchCall(final BlockingQueue<Signal> querySignals,
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