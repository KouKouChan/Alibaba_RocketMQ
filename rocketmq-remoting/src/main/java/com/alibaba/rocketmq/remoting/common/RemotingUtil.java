/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.remoting.common;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;


/**
 * 网络相关方法
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-13
 */
public class RemotingUtil {

    private static final Logger log = LoggerFactory.getLogger(RemotingHelper.RemotingLogName);

    public static final String OS_NAME = System.getProperty("os.name");

    private static boolean isLinuxPlatform = false;

    private static boolean isWindowsPlatform = false;

    /**
     * Refer to http://en.wikipedia.org/wiki/Reserved_IP_addresses
     */
    private static final String[] PRIVATE_NETWORK_CIDR = {
            "0.0.0.0/8"
            ,"10.0.0.0/8"
            ,"100.64.0.0/10"
            ,"127.0.0.0/8"
            ,"169.254.0.0/16"
            ,"172.16.0.0/12"
            ,"192.0.0.0/24"
            ,"192.0.2.0/24"
            ,"192.88.99.0/24"
            ,"192.168.0.0/16"
            ,"198.18.0.0/15"
            ,"198.51.100.0/24"
            ,"203.0.113.0/24"
            ,"224.0.0.0/4"
            ,"240.0.0.0/4"
            ,"255.255.255.255/32"};

    private static final Subnet[] PRIVATE_SUBNET = new Subnet[PRIVATE_NETWORK_CIDR.length];

    public static final List<Subnet> CURRENT_HOST_SUBNETS = new ArrayList<Subnet>();

    static {
        if (OS_NAME != null && OS_NAME.toLowerCase().contains("linux")) {
            isLinuxPlatform = true;
        }

        if (OS_NAME != null && OS_NAME.toLowerCase().contains("windows")) {
            isWindowsPlatform = true;
        }

        for (int i = 0; i < PRIVATE_NETWORK_CIDR.length; i++) {
            PRIVATE_SUBNET[i] = new Subnet(PRIVATE_NETWORK_CIDR[i]);
        }

        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();
                List<InterfaceAddress> interfaceAddresses = networkInterface.getInterfaceAddresses();
                for (InterfaceAddress interfaceAddress : interfaceAddresses) {
                    InetAddress inetAddress = interfaceAddress.getAddress();
                    if (!inetAddress.isLoopbackAddress() && inetAddress instanceof  Inet4Address) {
                        CURRENT_HOST_SUBNETS.add(new Subnet(interfaceAddress.getAddress().getHostAddress() + "/"
                                + interfaceAddress.getNetworkPrefixLength()));
                    }
                }

            }
        } catch (SocketException e) {
            log.error("", e);
        }
    }


    public static boolean isLinuxPlatform() {
        return isLinuxPlatform;
    }


    public static boolean isWindowsPlatform() {
        return isWindowsPlatform;
    }


    public static Selector openSelector() throws IOException {
        Selector result = null;
        // 在linux平台，尽量启用epoll实现
        if (isLinuxPlatform()) {
            try {
                final Class<?> providerClazz = Class.forName("sun.nio.ch.EPollSelectorProvider");
                if (providerClazz != null) {
                    try {
                        final Method method = providerClazz.getMethod("provider");
                        if (method != null) {
                            final SelectorProvider selectorProvider = (SelectorProvider) method.invoke(null);
                            if (selectorProvider != null) {
                                result = selectorProvider.openSelector();
                            }
                        }
                    }
                    catch (final Exception e) {
                        // ignore
                    }
                }
            }
            catch (final Exception e) {
                // ignore
            }
        }

        if (result == null) {
            result = Selector.open();
        }

        return result;
    }

    public static String getLocalAddress(boolean needPublicIP) {
        if (needPublicIP) {
            //Check if the current host is an AWS EC2 instance.
            String ec2PublicIPv4 = CloudUtil.awsEC2QueryPublicIPv4();
            if (null != ec2PublicIPv4) {
                return ec2PublicIPv4;
            }

            return getLocalAddress();
        }

        return getLocalAddress();
    }

    public static boolean isPrivateIPv4Address(String ip) {
        if (null == ip || ip.isEmpty()) {
            log.error("Cannot determine IP is private or not when it's null or empty");
            throw new RuntimeException("IP cannot be null or empty");
        }

        if (ip.startsWith("127.") || ip.startsWith("10.") || ip.startsWith("192.168.")) {
            return true;
        } else {
            for (Subnet subnet : PRIVATE_SUBNET) {
                if (subnet.compareAddressToSubnet(ip)) {
                    return true;
                }
            }
        }
        return false;
    }


    public static String getLocalAddress() {
        try {
            // 遍历网卡，查找一个非回路ip地址并返回
            Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
            ArrayList<String> ipv4Result = new ArrayList<String>();
            ArrayList<String> ipv6Result = new ArrayList<String>();
            while (enumeration.hasMoreElements()) {
                final NetworkInterface networkInterface = enumeration.nextElement();
                final Enumeration<InetAddress> en = networkInterface.getInetAddresses();
                while (en.hasMoreElements()) {
                    final InetAddress address = en.nextElement();
                    if (!address.isLoopbackAddress()) {
                        if (address instanceof Inet6Address) {
                            ipv6Result.add(normalizeHostAddress(address));
                        }
                        else {
                            ipv4Result.add(normalizeHostAddress(address));
                        }
                    }
                }
            }

            // 优先使用ipv4
            if (!ipv4Result.isEmpty()) {
                for (String ip : ipv4Result) {
                    if (isPrivateIPv4Address(ip)) {
                        continue;
                    }

                    return ip;
                }

                // 取最后一个
                return ipv4Result.get(ipv4Result.size() - 1);
            }
            // 然后使用ipv6
            else if (!ipv6Result.isEmpty()) {
                return ipv6Result.get(0);
            }
            // 然后使用本地ip
            final InetAddress localHost = InetAddress.getLocalHost();
            return normalizeHostAddress(localHost);
        }
        catch (SocketException e) {
            e.printStackTrace();
        }
        catch (UnknownHostException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static String normalizeHostAddress(final InetAddress localHost) {
        if (localHost instanceof Inet6Address) {
            return "[" + localHost.getHostAddress() + "]";
        }
        else {
            return localHost.getHostAddress();
        }
    }


    /**
     * IP1,IP2,...,IPn:PORT
     */
    public static SocketAddress string2SocketAddress(final String addr) {
        String[] s = addr.split(":");
        InetSocketAddress isa = new InetSocketAddress(RemotingHelper.filterIP(s[0]), Integer.valueOf(s[1]));
        return isa;
    }


    public static String socketAddress2String(final SocketAddress addr) {
        StringBuilder sb = new StringBuilder();
        InetSocketAddress inetSocketAddress = (InetSocketAddress) addr;
        sb.append(inetSocketAddress.getAddress().getHostAddress());
        sb.append(":");
        sb.append(inetSocketAddress.getPort());
        return sb.toString();
    }


    public static SocketChannel connect(SocketAddress remote) {
        return connect(remote, 1000 * 5);
    }


    public static SocketChannel connect(SocketAddress remote, final int timeoutMillis) {
        SocketChannel sc = null;
        try {
            sc = SocketChannel.open();
            sc.configureBlocking(true);
            sc.socket().setSoLinger(false, -1);
            sc.socket().setTcpNoDelay(true);
            sc.socket().setReceiveBufferSize(1024 * 64);
            sc.socket().setSendBufferSize(1024 * 64);
            sc.socket().connect(remote, timeoutMillis);
            sc.configureBlocking(false);
            return sc;
        }
        catch (Exception e) {
            if (sc != null) {
                try {
                    sc.close();
                }
                catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }

        return null;
    }


    public static void closeChannel(Channel channel) {
        final String addrRemote = RemotingHelper.parseChannelRemoteAddr(channel);
        channel.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                log.info("closeChannel: close the connection to remote address[{}] result: {}", addrRemote,
                    future.isSuccess());
            }
        });
    }

    public static List<InetAddress> gatherInetAddresses(boolean onlyIPv4) {
        List<InetAddress> inetAddressList = new ArrayList<InetAddress>();
        try {
            Enumeration<NetworkInterface> networks = NetworkInterface.getNetworkInterfaces();
            while (networks.hasMoreElements()) {
                NetworkInterface network = networks.nextElement();
                Enumeration<InetAddress> addresses = network.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (!address.isLoopbackAddress()) {
                        if (onlyIPv4) {
                            if (address instanceof Inet4Address) {
                                inetAddressList.add(address);
                            }
                        } else {
                            inetAddressList.add(address);
                        }
                    }
                }
            }
        } catch (SocketException e) {
            log.error("Error gathering InetAddress", e);
        }

        return inetAddressList;
    }


    public static void main(String[] args) throws SocketException {
        System.out.println(RemotingHelper.filterIP("192.168.50.54,8.8.8.8"));
    }
}
