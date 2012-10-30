package ar.edu.itba.pod.legajo51190.impl;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.Address;
import org.jgroups.ChannelListener;
import org.jgroups.Message;

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

	private static class RemoteQuery {
		private final CountDownLatch awaits;
		private final Set<Address> remoteNodes;
		private final Set<Result> results;

		public RemoteQuery(final Set<Address> allButMe) {
			remoteNodes = allButMe;
			results = new HashSet<>();
			awaits = new CountDownLatch(allButMe.size());
		}

		public CountDownLatch getAwaits() {
			return awaits;
		}

		public Set<Address> getRemoteNodes() {
			return remoteNodes;
		}

		public Set<Result> getResults() {
			return results;
		}
	}

	private final ListeningExecutorService localProcessingService;
	private final ExecutorService requestProcessingService;
	private final int threads;
	private final NodeReceiver networkState;
	private final Node node;
	private final ConcurrentHashMap<Integer, RemoteQuery> queries;
	private final AtomicInteger queryIdGenerator = new AtomicInteger(0);
	@SuppressWarnings("unused")
	private final NodeLogger nodeLogger;

	public MultiThreadedDistributedSignalProcessor(final int threads)
			throws Exception {
		this(threads, null, null);
	}

	public MultiThreadedDistributedSignalProcessor(final int threads,
			final Set<ChannelListener> listeners,
			final NodeListener nodeListener) throws Exception {
		this.threads = threads;
		queries = new ConcurrentHashMap<>();
		requestProcessingService = Executors.newCachedThreadPool();
		localProcessingService = MoreExecutors.listeningDecorator(Executors
				.newFixedThreadPool(threads));
		node = new Node(nodeListener);
		networkState = new NodeReceiver(node, this);
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
	public void add(final Signal signal) throws RemoteException {
		synchronized (node.getToDistributeSignals()) {
			node.getToDistributeSignals().add(signal);
		}
	}

	private void askRemoteQueries(final Signal signal, final int queryId) {

		Set<Address> allButMe = new HashSet<>(node.getAliveNodes());
		allButMe.remove(node.getAddress());
		queries.put(queryId, new RemoteQuery(allButMe));
		Message msg = new Message(null, new QueryNodeMessage(signal, queryId));
		if (node.isOnline() && node.getChannel().isConnected()) {
			try {
				node.getChannel().send(msg);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

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
	public void exit() throws RemoteException {
		node.exit();
	}

	@Override
	public Result findSimilarTo(final Signal signal) throws RemoteException {
		if (signal == null) {
			throw new IllegalArgumentException("Signal cannot be null");
		}

		Result result = new Result(signal);

		int queryId = queryIdGenerator.getAndIncrement();

		askRemoteQueries(signal, queryId);

		result = resolveLocalQueries(signal, result);

		result = awaitRemoteAnswers(result, queryId);

		return result;
	}

	private Result awaitRemoteAnswers(Result result, final int queryId) {
		try {
			RemoteQuery query = queries.get(queryId);
			if (!query.getAwaits().await(100000, TimeUnit.MILLISECONDS)) {
				nodeLogger.log("GOT NO! ANSWER!");
			}

			for (Result res : query.getResults()) {
				for (Item item : res.items()) {
					result = result.include(item);
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public JGroupNode getJGroupNode() {
		return node;
	}

	@Override
	public NodeStats getStats() throws RemoteException {
		return node.getStats();
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
	public void onQueryReception(final QueryNodeMessage query,
			final Address from) {
		requestProcessingService.submit(new Runnable() {
			@Override
			public void run() {
				Result result = new Result(query.getSignal());
				result = resolveLocalQueries(query.getSignal(), result);
				Message msg = new Message(from, new QueryResultNodeMessage(
						query.getQueryId(), result));

				if (node.getChannel().isConnected()) {
					try {
						node.getChannel().send(msg);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		});

	}

	@Override
	public void onResultReception(final QueryResultNodeMessage answer,
			final Address from) {
		RemoteQuery query = queries.get(answer.getMessageId());
		query.getResults().add(answer.getResult());
		query.getRemoteNodes().remove(from);
		query.getAwaits().countDown();
	}

	private Result resolveLocalQueries(final Signal signal, Result result) {
		final BlockingQueue<Signal> querySignals = buildQuerySignalSet();

		List<SearchCall> queries = new ArrayList<>();
		for (int i = 0; i < threads; i++) {
			queries.add(new SearchCall(querySignals, signal));
		}

		try {
			List<Future<List<Item>>> results = localProcessingService
					.invokeAll(queries);

			for (Future<List<Item>> future : results) {
				for (Item item : future.get()) {
					result = result.include(item);
				}
			}

		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		return result;
	}
}
