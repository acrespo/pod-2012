package ar.edu.itba.pod.legajo51190;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.AlreadyBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import ar.edu.itba.pod.legajo51190.impl.MultiThreadedSignalProcessor;

/**
 * Hello world!
 * 
 */
public class Server {
	private final int port;
	private final int nthreads;

	public Server(final int port, final int nthreads) {
		this.port = port;
		this.nthreads = nthreads;
	}

	public static void main(final String[] args) {
		if (args.length < 1) {
			System.out
					.println("Command line parameters: Server <port> <nthreads> ");
			return;
		}

		int port;
		int nthreads;
		try {
			port = Integer.parseInt(args[0]);
		} catch (Exception e) {
			System.out.println("Invalid port");
			return;
		}
		try {
			nthreads = Integer.parseInt(args[1]);
		} catch (Exception e) {
			System.out.println("Using default processors configuration: "
					+ Runtime.getRuntime().availableProcessors());
			nthreads = Runtime.getRuntime().availableProcessors();
		}
		new Server(port, nthreads).start();
	}

	private void start() {
		Registry reg;
		try {
			reg = LocateRegistry.createRegistry(port);

			MultiThreadedSignalProcessor impl = new MultiThreadedSignalProcessor(
					nthreads);
			Remote proxy = UnicastRemoteObject.exportObject(impl, 0);

			reg.bind("SignalProcessor", proxy);
			reg.bind("SPNode", proxy);
			System.out.println("Server started and listening on port " + port);
			System.out.println("Press <enter> to quit");
			new BufferedReader(new InputStreamReader(System.in)).readLine();

		} catch (RemoteException e) {
			System.out.println("Unable to start local server on port " + port);
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Unexpected i/o problem");
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
			System.out
					.println("Unable to register remote objects. Perhaps another instance is running on the same port?");
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
