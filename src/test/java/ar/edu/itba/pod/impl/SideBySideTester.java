package ar.edu.itba.pod.impl;

import static ar.edu.itba.pod.signal.source.SignalBuilder.constant;
import static ar.edu.itba.pod.signal.source.SignalBuilder.flux;
import static ar.edu.itba.pod.signal.source.SignalBuilder.modulate;
import static ar.edu.itba.pod.signal.source.SignalBuilder.rotate;
import static ar.edu.itba.pod.signal.source.SignalBuilder.sine;
import static ar.edu.itba.pod.signal.source.SignalBuilder.square;
import static ar.edu.itba.pod.signal.source.SignalBuilder.triangle;
import static org.junit.Assert.assertEquals;

import java.rmi.RemoteException;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ar.edu.itba.pod.api.Result;
import ar.edu.itba.pod.api.Signal;
import ar.edu.itba.pod.api.SignalProcessor;
import ar.edu.itba.pod.signal.source.RandomSource;
import ar.edu.itba.pod.signal.source.Source;

/**
 * Test suite that runs the same tests agains an implementation and compares the
 * result against a reference.
 * 
 * Note that this class is abstract. Concrete suites are created by subclassing
 * this and overriding the init() method.
 */
public abstract class SideBySideTester {
	protected SignalProcessor reference;
	protected SignalProcessor toTest;
	protected Source src;

	@Before
	public void setup() throws Exception {
		reference = new StandaloneSignalProcessor();
		toTest = init();
		src = new RandomSource(12345);
	}

	@After
	public void disconnect() throws Exception {
		clear();
	}

	/**
	 * Initialize or get a reference to the signal processor to be tested.
	 */
	abstract protected SignalProcessor init() throws Exception;

	abstract protected void clear() throws Exception;

	@Test
	public void test01() throws RemoteException {
		addNoise(100);
		assertFind(src.next());
	}

	@Test
	public void test02() throws RemoteException {
		addNoise(10000);
		add(constant((byte) 4));
		add(constant((byte) 10));
		assertFind(constant((byte) 6));
	}

	@Test
	public void test03() throws RemoteException {
		addNoise(1000);
		Signal r = src.next();
		add(rotate(r, 5));
		add(rotate(r, 10));
		add(rotate(r, 50));
		add(rotate(r, 100));
		add(rotate(r, 200));
		assertFind(r);
	}

	@Test
	public void test04() throws RemoteException {
		add(sine());
		add(square());
		add(modulate(square(), 0.9));
		assertFind(modulate(sine(), 0.9));
	}

	@Test
	public void test05() throws RemoteException {
		addNoise(10);
		Signal r = src.next();
		add(rotate(r, 5));
		add(rotate(r, 10));
		add(constant((byte) 10));
		assertFind(r);
	}

	@Test
	public void test06() throws RemoteException {
		addNoise(1000);
		Signal r = triangle();
		add(r);
		assertFind(r);
	}

	@Test
	public void test07() throws RemoteException {
		addNoise(10);
		Signal r = src.next();
		add(rotate(r, 5));
		add(rotate(r, 10));
		add(constant((byte) 10));
		add(triangle());
		assertFind(r);

	}

	@Test
	public void test08() throws RemoteException {
		addNoise(1500);
		add(square());
		add(sine());
		add(triangle());
		assertFind(triangle());
	}

	@Test
	public void test09() throws RemoteException {
		addNoise(150);
		byte[] flux = Arrays.copyOfRange(triangle().content(), 3, 50);
		Signal r = flux((byte) 10, 50, flux);
		add(r);
		assertFind(r);

	}

	private void assertFind(final Signal s) throws RemoteException {
		assertEquals(1, 1);
		long t1, t2, t3;
		t1 = System.currentTimeMillis();
		Result first = toTest.findSimilarTo(s);
		t2 = System.currentTimeMillis();
		Result second = reference.findSimilarTo(s);
		t3 = System.currentTimeMillis();
		assertEquals(second, first);
		System.out.println("Time total to find impl:" + (t2 - t1));
		System.out.println("Time total to find reference:" + (t3 - t2));
	}

	protected void add(final Signal s) throws RemoteException {
		reference.add(s);
		toTest.add(s);
	}

	protected void addNoise(final int amount) throws RemoteException {
		for (int i = 0; i < amount; i++) {
			Signal s = src.next();
			reference.add(s);
			toTest.add(s);
		}
	}
}
