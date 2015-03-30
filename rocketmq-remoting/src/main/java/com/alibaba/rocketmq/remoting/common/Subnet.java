package com.alibaba.rocketmq.remoting.common;

import java.net.InetAddress;
import java.net.UnknownHostException;

/*****************************************************************************
 * Objects that represent subnets (internet addresses and mask).
 * <P>
 * Subnets are represented as a string in the format "xxx.xxx.xxx.xxx/yy"
 * where "xxx.xxx.xxx.xxx" is the address and "yy" is the bitLength.
 * <P>
 * @author Dan Lipofsky<BR>
 *	Copyright &copy; 1999 Cycorp, Inc.  All rights reserved.
 *****************************************************************************/
public class Subnet {
    private int address;        // The 4 byte internet address
    private int bitLength;    // The number of meaningful bits
    private int mask;        // The mask used to select the meaningful bits

    /**
     * Constructs a Subnet with the given address and bitLength.
     * This is a private constructor.  We want to hide how the integers
     * correspond to addresses.  Only byte arrays or strings get passed in.
     * @param address IPv4 address
     * @param bitLength bit length
     */
    private Subnet(int address, int bitLength) {
        if (bitLength < 0 || bitLength > 32)
            throw new IllegalArgumentException("Illegal bitLength " + bitLength);
        this.address = address;
        this.bitLength = bitLength;
        this.mask = makeMask(bitLength);
    }

    /**
     * Constructs a Subnet with the given address and bitLength.
     */
    public Subnet(byte addr[], int bitLength) {
        this(arrayToInt(addr), bitLength);
    }

    /**
     * Constructs a subnet with given string representation.
     *
     * @param subnet Subnet string, with form of 192.168.0.0/16
     */
    public Subnet(String subnet) {
        if (null == subnet || subnet.trim().isEmpty()
                || !subnet.contains("/") // Separate IP with bit length with '/'
                || subnet.indexOf("/") != subnet.lastIndexOf("/")  //Only one '/' is allowed.
                || subnet.length() < 9) {
            throw new IllegalArgumentException("Illegal subnet format");
        }

        String[] cidrSegments = subnet.split("/");
        assert  cidrSegments.length == 2;
        int bitLength = Integer.parseInt(cidrSegments[1]);
        String[] ipSegments = cidrSegments[0].split("\\.");
        assert ipSegments.length == 4;
        byte[] address = new byte[4];
        for (int i = 0; i < ipSegments.length; i++) {
            address[i] = (byte)Integer.parseInt(ipSegments[i], 10);
        }
        this.address = arrayToInt(address);
        this.bitLength = bitLength;
        this.mask = makeMask(bitLength);
    }

    /******************************************************************
     ********************* Public Methods******************************
     *****************************************************************/

    /**
     * Compares two Subnet objects.
     *
     * @return true if address and bitLength are equal.
     */
    public boolean equals(Object obj) {
        if (obj instanceof Subnet) {
            Subnet other = (Subnet) obj;
            return ((address == other.address) && (bitLength == other.bitLength));
        }
        return false;
    }

    /**
     * Determines if an address is in a Subnet.
     *
     * @return true if address is in the Subnet.
     */
    public boolean compareAddressToSubnet(InetAddress a) {
        int tmp_address = arrayToInt(a.getAddress());
        return ((tmp_address & mask) == (address & mask));
    }

    /**
     * Determines if an address is in a Subnet.
     *
     * @return true if address is in the Subnet.
     */
    public boolean compareAddressToSubnet(String host) {
        int tmp_address = arrayToInt(fastSplitIP(host));
        return ((tmp_address & mask) == (address & mask));
    }

    /**
     * Converts a Subnet to a string
     */
    public String toString() {
        return (String) (((address >>> 24) & 0xFF) + "." +
                ((address >>> 16) & 0xFF) + "." +
                ((address >>> 8) & 0xFF) + "." +
                (address & 0xFF) + "/" +
                bitLength);
    }

    /******************************************************************
     ******************** Private Methods******************************
     *****************************************************************/

    /**
     * Creates a mask from a bitLength.  The most significant bitLength
     * bits are 1, while the rest are 0.
     */
    private static int makeMask(int bitLength) {
        int mask = 0;
        for (int i = 0; i < bitLength; i++) {
            mask = (mask << 1) | 1;
        }
        mask = mask << (32 - bitLength);
        return mask;
    }

    /**
     * Converts an array of 4 bytes to an int
     */
    private static int arrayToInt(byte address[]) {
        return (int) (address[3] & 0xFF
                | ((address[2] << 8) & 0xFF00)
                | ((address[1] << 16) & 0xFF0000)
                | ((address[0] << 24) & 0xFF000000));
    }

    /**
     * Converts an int to an array of 4 bytes
     */
    private static byte[] intToArray(int address) {
        byte[] addr = new byte[4];
        addr[0] = (byte) ((address >>> 24) & 0xFF);
        addr[1] = (byte) ((address >>> 16) & 0xFF);
        addr[2] = (byte) ((address >>> 8) & 0xFF);
        addr[3] = (byte) (address & 0xFF);
        return addr;
    }

    static public void main(String argv[]) throws UnknownHostException {
        byte[] address = new byte[]{(byte) 207, (byte) 207, (byte) 8, (byte) 0};
        Subnet s = new Subnet(address, 30);
        InetAddress a1 = InetAddress.getByName("207.207.8.3");
        InetAddress a2 = InetAddress.getByName("207.207.8.4");
        System.out.println(s + " " + a1 + " " + s.compareAddressToSubnet(a1));
        System.out.println(s + " " + a2 + " " + s.compareAddressToSubnet(a2));

        Subnet subnet = new Subnet("172.16.0.0/12");
        final int MAX = 10000000;
        final String ip = "172.131.186.78";


        long start = System.currentTimeMillis();
        for (int i = 0; i < MAX; i++) {
            subnet.compareAddressToSubnet(InetAddress.getByName(ip));
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);


        start = System.currentTimeMillis();
        for (int i = 0; i < MAX; i++) {
            subnet.compareAddressToSubnet(ip);
        }
        end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    private static final char IP_SEPARATOR = '.';

    private static byte[] fastSplitIP(String ip) {
        byte[] bytes = new byte[4];
        int[] array = new int[3];
        int count = 0;
        char c;
        int result = 0;
        int i = 0;
        final int len = ip.length();
        for (int index = 0; index < len; index++) {
            c = ip.charAt(index);
            if (c == IP_SEPARATOR || index == len - 1) {

                if (index == len - 1) {
                    array[count++] = c - '0';
                }

                for (int j = 0; j < count; j++) {
                    result = result * 10 + array[j];
                }
                bytes[i++] = (byte)result;
                count = 0;
                result = 0;
            } else {
                array[count] = c - '0';
                count++;
            }
        }
        return bytes;
    }
}
