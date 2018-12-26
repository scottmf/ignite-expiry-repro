package com.example.demo;

import static java.util.Arrays.asList;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class IgniteExpiryReproApplication {

	public static void main(String[] args) throws InterruptedException {
        System.setProperty(IgniteSystemProperties.IGNITE_QUIET, "true");
        System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, "false");
//        SpringApplication.run(IgniteExpiryReproApplication.class, args);
        int discoveryPort1 = findFreePort();
        int discoveryPort2 = findFreePort();
        TcpDiscoveryVmIpFinder igniteIpFinder = igniteIpFinder(discoveryPort1, discoveryPort2);

        int host1 = findFreePort();
        int host2 = findFreePort();
        CommunicationSpi communicationSpi1 = communicationSpi(host1);
        CommunicationSpi communicationSpi2 = communicationSpi(host2);
        TcpDiscoveryVmIpFinder ipFinder = igniteIpFinder(discoveryPort1);
        TcpDiscoverySpi tcpDiscoverySpi1 = tcpDiscoverySpi(ipFinder, discoveryPort1);
        TcpDiscoverySpi tcpDiscoverySpi2 = tcpDiscoverySpi(ipFinder, discoveryPort2);
        IgniteConfiguration igniteConfiguration1 = igniteConfiguration(tcpDiscoverySpi1, communicationSpi1, "host1");
        IgniteConfiguration igniteConfiguration2 = igniteConfiguration(tcpDiscoverySpi2, communicationSpi2, "host2");
        Ignition.start(igniteConfiguration1);
        Ignite ignite = Ignition.start(igniteConfiguration2);
        CacheConfiguration<Object, Object> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName("replicated");
        cacheConfiguration.setCacheMode(CacheMode.REPLICATED);
        cacheConfiguration.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(Duration.ONE_MINUTE));
        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(cacheConfiguration);
        while (true) {
            cache.put("key", System.currentTimeMillis());
            while (true) {
                Long o = (Long) cache.get("key");
                if (o != null) {
                    if (o < (System.currentTimeMillis() - 60000)) {
                        System.err.println("ERROR: cached={" + 
                            DateFormat.getDateTimeInstance().format(o) + "} vs now={" +
                            DateFormat.getDateTimeInstance().format(System.currentTimeMillis()) + "}");
                    } else {
                        System.out.println(DateFormat.getDateTimeInstance().format(o));
                    }
                    Thread.sleep(60001);
                } else {
                    System.out.println("EXPIRED");
                    break;
                }
            }
        }
	}

    public static TcpDiscoverySpi tcpDiscoverySpi(TcpDiscoveryIpFinderAdapter igniteIpFinder, int port) {
        TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();
        tcpDiscoverySpi.setIpFinder(igniteIpFinder);
        tcpDiscoverySpi.setLocalPortRange(0);
        tcpDiscoverySpi.setLocalPort(port);
        String loopbackAddress = InetAddress.getLoopbackAddress().getHostAddress();
        tcpDiscoverySpi.setLocalAddress(loopbackAddress);
        return tcpDiscoverySpi;
    }

    public static TcpDiscoveryVmIpFinder igniteIpFinder(Integer ... ports) {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        List<String> addrs = new ArrayList<>();
        asList(ports).forEach(port -> addrs.add("127.0.0.1:" + port));
        ipFinder.setAddresses(addrs);
        return ipFinder;
    }

    public static CommunicationSpi communicationSpi(int port) {
        TcpCommunicationSpi tcpCommunicationSpi = new TcpCommunicationSpi();
        tcpCommunicationSpi.setLocalPort(port);
        return tcpCommunicationSpi;
    }

    public static IgniteConfiguration igniteConfiguration(TcpDiscoverySpi tcpDiscoverySpi, CommunicationSpi communicationSpi, String instanceName) {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIgniteInstanceName(instanceName);
        cfg.setIgniteHome("/var/tmp/");
        cfg.setPeerClassLoadingEnabled(true);
        cfg.setIncludeEventTypes(new int[0]);
//        cfg.setGridLogger(new Slf4jLogger());
        cfg.setDiscoverySpi(tcpDiscoverySpi);
        cfg.setCommunicationSpi(communicationSpi);
        cfg.setClientMode(false);
        ConnectorConfiguration connectorConfiguration = cfg.getConnectorConfiguration();
        String loopbackAddress = InetAddress.getLoopbackAddress().getHostAddress();
        connectorConfiguration.setHost(loopbackAddress);
        return cfg;
    }

    public static int findFreePort() {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
            socket.setReuseAddress(true);
            int port = socket.getLocalPort();
            closeQuietly(socket);
            return port;
        } catch (IOException e) {
        } finally {
            if (socket != null) {
                closeQuietly(socket);
            }
        }
        throw new IllegalStateException();
    }

    private static void closeQuietly(ServerSocket socket) {
        try {
            socket.close();
        } catch (IOException e) {
        }
    }
}