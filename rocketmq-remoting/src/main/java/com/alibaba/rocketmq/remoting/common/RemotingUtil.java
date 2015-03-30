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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Enumeration;


/**
 * 网络相关方法
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-13
 */
public class RemotingUtil {
    private static final String CLASS_NAME = RemotingUtil.class.getName();

    private static final Logger log = LoggerFactory.getLogger(RemotingHelper.RemotingLogName);
    public static final String OS_NAME = System.getProperty("os.name");

    private static boolean isLinuxPlatform = false;
    private static boolean isWindowsPlatform = false;

    private static final int TIME_OUT = 3000;

    public static final String WS_DOMAIN_NAME = System.getProperty("rocketmq.namesrv.domain", "config.graphene.spellso.com");
    public static final String WS_IP_MAPPING_ADDR = "http://" + WS_DOMAIN_NAME + ":80/rocketmq/ip?innerIP=";
    private static final int MINIMAL_IPV4_LENGTH = 7;


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

    static {
        if (OS_NAME != null && OS_NAME.toLowerCase().indexOf("linux") >= 0) {
            isLinuxPlatform = true;
        }

        if (OS_NAME != null && OS_NAME.toLowerCase().indexOf("windows") >= 0) {
            isWindowsPlatform = true;
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

    public static String queryPublicIP(String innerIP) {
        final String signature = CLASS_NAME + "#queryPublicIP(innerIP: {})";
        log.debug("Enter " + signature, innerIP);

        if (!isPrivateIPv4Address(innerIP)) {
            return innerIP;
        } else {
            URL url = null;
            try {
                url = new URL(WS_IP_MAPPING_ADDR + innerIP);
                URLConnection urlConnection = url.openConnection();
                if (urlConnection instanceof HttpURLConnection) {
                    HttpURLConnection httpURLConnection = (HttpURLConnection) urlConnection;
                    httpURLConnection.setConnectTimeout(TIME_OUT);
                    httpURLConnection.setReadTimeout(TIME_OUT);
                    httpURLConnection.connect();
                    InputStream inputStream = httpURLConnection.getInputStream();
                    BufferedReader bufferedReader = null;
                    StringBuilder stringBuilder = new StringBuilder();
                    String line = "";
                    try {
                        bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                        while (null != (line = bufferedReader.readLine())) {
                            stringBuilder.append(line).append(System.getProperty("line.separator"));
                        }
                        return stringBuilder.substring(0, stringBuilder.length() - 1);
                    } finally {
                        if (null != bufferedReader) {
                            bufferedReader.close();
                        }

                        if (null != inputStream) {
                            inputStream.close();
                        }
                    }
                } else {
                    log.error("Unknown protocol to query public IP for the given inner IP");
                }
            } catch (MalformedURLException e) {
                log.error("Error while querying public IP", e);
            } catch (IOException e) {
                log.error("Error while querying public IP", e);
            }
        }

        return null;
    }

    public static String getLocalAddress(boolean needQueryPublicIP) {
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

            //If deployed in cloud environment and elastic IP is used, we need a public IP Address here to handle
            // scenarios which deployment of this app spans several data centers and VPC cannot communicate.
            if ("true".equals(System.getProperty("use_elastic_ip")) || "true".equals(System.getenv("ROCKETMQ_USE_ELASTIC_IP"))) {
                String elasticIP = System.getProperty("elastic_ip");
                if (null != elasticIP && elasticIP.trim().length() >= MINIMAL_IPV4_LENGTH) {
                    return elasticIP;
                }

                if (!ipv4Result.isEmpty()) {
                    for (String ip : ipv4Result) {
                        if (ip.startsWith("127.0")) {
                            continue;
                        }

                        if (isPrivateIPv4Address(ip) && needQueryPublicIP) {
                            String publicIp = queryPublicIP(ip);

                            if (null != publicIp) {
                                return publicIp;
                            }
                        }

                    }
                }
            }


            // 优先使用ipv4
            if (!ipv4Result.isEmpty()) {
                for (String ip : ipv4Result) {
                    if (ip.startsWith("127.0") || ip.startsWith("192.168")) {
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

    public static String getLocalAddress() {
        return getLocalAddress(true);
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
     * IP:PORT
     */
    public static SocketAddress string2SocketAddress(final String addr) {
        String[] s = addr.split(":");
        InetSocketAddress isa = new InetSocketAddress(s[0], Integer.valueOf(s[1]));
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

}
