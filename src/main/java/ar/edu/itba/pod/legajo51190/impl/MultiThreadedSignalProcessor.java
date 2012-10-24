package ar.edu.itba.pod.legajo51190.impl;

import java.rmi.RemoteException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import ar.edu.itba.pod.api.NodeStats;
import ar.edu.itba.pod.api.Result;
import ar.edu.itba.pod.api.SPNode;
import ar.edu.itba.pod.api.Signal;
import ar.edu.itba.pod.api.SignalProcessor;

public class MultiThreadedSignalProcessor implements SignalProcessor, SPNode {
	private final BlockingQueue<Signal> signals = new LinkedBlockingQueue<>();

	public MultiThreadedSignalProcessor(final int threads) {

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

		for (Signal cmp : signals) {
			Result.Item item = new Result.Item(cmp, signal.findDeviation(cmp));
			result = result.include(item);
		}
		return result;
	}

}
