import java.io.*;
import java.net.*;
import java.lang.Math;
import java.sql.Timestamp;
import java.util.*;

public class Sender2b
{
    static final int header_length = 3;
    private static final int max_payload_length = 1024;

    private static String filename;
    private static byte[] data;
    static short window_size;
    private static byte[][] data_packets;
    static int timeout;
    private static int receiverPortNumber;
    private static InetAddress receiver_address;

    private static DatagramSocket sender_socket;

    public static void main(String[] args) {
        // === Parse command-line arguments ================================
        String receiverHostname = args[0];
        receiverPortNumber = Integer.parseInt(args[1]);
        filename = args[2];
        timeout = Integer.parseInt(args[3]);
        window_size = (short) Integer.parseInt(args[4]);
        if (window_size > 16383 || window_size <= 0) {
            System.out.println("Please enter a window size N such that 0 < N <= 16383.");
            System.exit(0);
        }

        try {
            receiver_address = InetAddress.getByName(receiverHostname);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            System.out.println("Error retrieving receiver's IP address.");
            System.exit(0);
        }

        load_file();

        try {
            sender_socket = new DatagramSocket();
        } catch (SocketException e) {
            e.printStackTrace();
            System.out.println("Error initializing sender socket.");
            quit();
        }

        // Sequence numbers are in range [1,32767]
        // Absolute numbers are in range [0, # bytes]
        // Window positions are in range [1, window_size]

        // PacketTracker contains all required information for a given packet (sequence number, window position, etc.)
        // Constructor: new PacketTracker(seqNum, absNum, windowPos, DatagramPacket)
        PacketTracker next_packet = new PacketTracker((short) 1, 0, 1, assemblePacket((short) 1, 0));
        next_packet.windowPosition = 1;

        // The next packet to be retransmitted
        PacketTracker next_retransmit;

        // Maps sequence numbers to PacketTrackers. Contains all packets in the current window:
        LinkedHashMap<Short, PacketTracker> window_packets = new LinkedHashMap<Short, PacketTracker>(window_size);

        // Keeps track of the order in which packets should be retransmitted. Contains only packets
        // in the window yet to be acknowledged:
        LinkedList<PacketTracker> to_be_acked = new LinkedList<PacketTracker>();

        // Variables for handling received ACKS
        DatagramPacket ack = new DatagramPacket(new byte[header_length + 3], header_length + 3);
        int time_until_retransmit;
        short receivedSeq;
        int new_send_base_pos;
        short new_baseseqnum;

        //
        boolean last_packet_received = false;

        // Contains sequence number of the last packet added to the window
        short last_seqnum;
        boolean is_window_space;

        //
        long start_time = System.currentTimeMillis();
        int num_retransmissions = 0;

        // Debug
        int counter = 0;

        System.out.println("Sending data now.");
        while (true) {

            // Debug
            for (PacketTracker t : window_packets.values()) {
                if (t.windowPosition > window_size) break;
            }

            // === Double check rest of loop working okay here =============================
            if (to_be_acked.isEmpty() && !window_packets.isEmpty() && window_packets.size() == window_size) {
                boolean all = true;
                for (PacketTracker t : window_packets.values()) {
                    if (!t.received) {
                        all = false;
                        to_be_acked.add(t);
                    }
                }
                if (all) window_packets.clear();
            }

            // Debug
            showWindow(window_packets, to_be_acked);
//            if (counter > 10) break;
//            else counter++;
//            if (counter > 2) break;
//            else if (window_packets.size() == window_size) counter++;
//            else counter = 0;

            // === Only add another packet if window isn't full ========================================================
            // === And if all packets aren't already in pipeline/acknowledged ==========================================
            if (!window_packets.isEmpty()) {
                last_seqnum = PacketTracker.seqNumAddition(window_packets.keySet().iterator().next(), window_packets.size() - 1);
                is_window_space = window_packets.get(last_seqnum).windowPosition < window_size;
            } else is_window_space = true;

            if (next_packet.absoluteNumber != data_packets.length - 1 && is_window_space) {
                System.out.println("Adding packet " + next_packet.sequenceNumber);

                // Send next_packet for first time
                send(next_packet.packet);

                window_packets.put(next_packet.sequenceNumber, next_packet);

                // Determine packet's position in the retransmission queue, set its timestamp,
                // then add to retransmission queue
                next_packet.timestamp = new Timestamp(System.currentTimeMillis() + timeout);
                to_be_acked.add(next_packet);

                // Update next_packet
                next_packet = next_packet.nextPacket();
                continue;
            }

            // If no packets are waiting to be acknowledged, continue (so packets can be added to pipeline)
            if (to_be_acked.isEmpty()) continue;

            // Retrieve next packet to be retransmitted, get how long until it times out
            next_retransmit = to_be_acked.peek();
            System.out.println("Waiting on ACK for " + next_retransmit.sequenceNumber);
            time_until_retransmit = (int)(next_retransmit.timestamp.getTime() - System.currentTimeMillis());
            if (time_until_retransmit <= 0) time_until_retransmit = 1;

            try {
                sender_socket.setSoTimeout(time_until_retransmit);
                sender_socket.receive(ack);
            } catch (SocketTimeoutException e) {
                // TIMEOUT: re-send next_retransmit
                System.out.println("Retransmitting packet " + next_retransmit.sequenceNumber);
                num_retransmissions++;
                try {
                    sender_socket.send(next_retransmit.packet);
                } catch (IOException ex) {
                    ex.printStackTrace();
                    System.out.println("IO/Error retransmitting packet.");
                    quit();
                }
                // Reset timestamp and move the retransmitted packet to the end of the queue
                to_be_acked.peek().timestamp.setTime(System.currentTimeMillis() + timeout);
                to_be_acked.add(to_be_acked.poll());

                continue;

            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("IO/Error receiving ACK.");
                quit();
            }

            // === ACK RECEIVED: =======================
            receivedSeq = (short)((ack.getData()[1] << 8) | ack.getData()[2] & 0xff);
            System.out.println("ACK received for packet " + receivedSeq);

            // Check EOF bit
            if (ack.getData()[0] == 1) last_packet_received = true;

            // Mark packet as received, remove it from to_be_acked
            // Ignore ACKs for old packets
            try {
                window_packets.get(receivedSeq).received = true;
                to_be_acked.removeFirst();
            } catch (NullPointerException e) {
                // ACK was for old packet; ignore
            }

            // If to_be_acked is empty and the last packet's ACK has been received, then we're done
            if (to_be_acked.isEmpty() && last_packet_received) break;

            // If received packet ACK isn't for first packet in window, then no need to shift the window
            if (window_packets.get(window_packets.keySet().iterator().next()).sequenceNumber != receivedSeq) continue;

            // Else, shift the window by finding the first unacknowledged packet in the window, removing all
            // packets from before it, and updating remaining packets' window positions to reflect the window shift
            window_packets.remove(receivedSeq);
            new_send_base_pos = 2;
            new_baseseqnum = PacketTracker.nextSequenceNumber(receivedSeq);
            for (int i = 2; i <= window_packets.size(); i++) {
                if (window_packets.get(new_baseseqnum).received) {
                    window_packets.remove(new_baseseqnum);

                    new_send_base_pos++;
                    new_baseseqnum = PacketTracker.nextSequenceNumber(new_baseseqnum);
                } else break;
            }
            for (short seqNum : window_packets.keySet()) {
                window_packets.get(seqNum).windowPosition -= (new_send_base_pos - 1);
            }

            // Also update window position of next_packet
            next_packet.windowPosition -= (new_send_base_pos - 1);

        }

        double trans_time = (System.currentTimeMillis() - start_time) / 1000.0;

        System.out.println("All data has been sent and accounted for.\n");
        System.out.println("There were " + num_retransmissions + " retransmissions in total.");
        System.out.println("Total transmission time: " + trans_time + " seconds.");
        System.out.println("Average throughput: " + (data_packets.length / trans_time) + " KB/s.");

    }

    /**
     * This method reads the specified file in 'filename' into byte array 'data', and then splits that into
     * packet-sized chunks and stores those in byte-array array 'data_packets'.
     */
    private static byte[][] load_file() {

        try {
            File file = new File(filename);
            data = new byte[(int) file.length()];
            FileInputStream fis = new FileInputStream(file);
            fis.read(data);
        } catch (FileNotFoundException e) {
            try {
                File file = new File("/mnt/shared/" + filename);
                data = new byte[(int) file.length()];
                FileInputStream fis = new FileInputStream(file);
                fis.read(data);
            } catch (FileNotFoundException e2) {
                // FOR TESTING OUTSIDE VM
                try {
                    File file = new File("/afs/inf.ed.ac.uk/user/s14/s1455790/Desktop/" + filename);
                    data = new byte[(int) file.length()];
                    FileInputStream fis = new FileInputStream(file);
                    fis.read(data);
                } catch (FileNotFoundException e3) {
                    e3.printStackTrace();
                    System.out.println("File was not found. Please specify its full path, or put the file in mnt/shared and pass only the filename as an argument.");
                    System.exit(0);
                } catch (IOException i3) {
                    i3.printStackTrace();
                    System.exit(0);
                }
            } catch (IOException i2) {
                i2.printStackTrace();
                System.exit(0);
            }
        } catch (IOException i) {
            i.printStackTrace();
            System.exit(0);
        }

        // Break data down into packet-sized chunks (each with maximum size max_payload_length) and store
        // the chunks in data_packets (storing the same data twice, but whatever).
        data_packets = new byte[(int)Math.ceil(data.length / (float)max_payload_length)][max_payload_length];
        for (int packetNum = 0; packetNum < (data_packets.length - 1); packetNum++) {
            data_packets[packetNum] = Arrays.copyOfRange(data, packetNum*max_payload_length, max_payload_length*(packetNum+1));
        }
        // avoid ArrayIndexOutOfBoundsException, do last packet separately
        data_packets[data_packets.length-1] = Arrays.copyOfRange(data, max_payload_length*(data_packets.length-1), data.length);

        System.out.println("\nSender has loaded the file and is ready to send.");
        System.out.println("File size: " + data.length + " bytes, consisting of " + data_packets.length + " packets.\n");
        return data_packets;
    }

    /**
     * Assemble packet with sequence number seqNum, using data from data_packets[absNum].
     * @param absNum
     * @param seqNum
     * @return
     */
    static DatagramPacket assemblePacket(short seqNum, int absNum) {
        byte[] pck_data = new byte[header_length + data_packets[absNum].length];
        System.arraycopy(data_packets[absNum], 0, pck_data, header_length, data_packets[absNum].length);

        // Assign the packet its sequence number.
        pck_data[1] = (byte) ((seqNum >> 8) & 0xff);
        pck_data[2] = (byte) (seqNum & 0xff);

        // Set last-message bit if applicable
        if (seqNum == data_packets.length - 1) pck_data[0] = 1;

        return new DatagramPacket(pck_data, pck_data.length, receiver_address, receiverPortNumber);
    }

    /** Close socket and exit */
    private static void quit() {
        try {
            sender_socket.close();
        } catch (NullPointerException e) {
            // Socket wasn't initialized
        }
        System.exit(0);
    }

    /** Wrapper function for sending DatagramPacket */
    private static void send(DatagramPacket p) {
        try {
            sender_socket.send(p);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error sending packet.");
            quit();
        }
    }

    /** Prints debug information */
    private static void showWindow(LinkedHashMap<Short, PacketTracker> window_packets, LinkedList<PacketTracker> to_be_acked) {
        System.out.println("===========================================================");
        if (window_packets.isEmpty()) System.out.println("Window is currently empty.");
        else {
            System.out.println("Window packets: ");
            for (PacketTracker t : window_packets.values()) {
                String status = (t.received) ? "acknowledged" : "not acknowledged";
                System.out.println("seqNum: " + t.sequenceNumber + ", windowPos: " + t.windowPosition + ", " + status);
            }
        }

        if (to_be_acked.isEmpty()) System.out.println("No packets waiting to be acknowledged.");
        else {
            System.out.println("Packets waiting to be acknowledged: ");
            for (PacketTracker t : to_be_acked) {
                int retrans = (int) (t.timestamp.getTime() - System.currentTimeMillis());
                if (retrans < 0) retrans = 0;
                System.out.println("seqNum: " + t.sequenceNumber + ", retr: " + retrans + " ms");
            }
        }
    }

    /**
     * A class that keep tracks of a packet's:
     *      -sequence number (and handles related overflows)
     *      -absolute number
     *      -window position
     *      -receipt status
     *      -time at which it should be re-transmitted
     */
    private static class PacketTracker implements Cloneable {

        short sequenceNumber;
        int absoluteNumber;
        int windowPosition;
        boolean received;
        boolean lastPacket;
        Timestamp timestamp;
        DatagramPacket packet;

        PacketTracker(short sequenceNumber, int absoluteNumber, int windowPosition, DatagramPacket packet) {
            this.absoluteNumber = absoluteNumber;
            this.sequenceNumber = sequenceNumber;
            this.windowPosition = windowPosition;
            this.received = false;
            this.lastPacket = false;
            this.packet = packet;
        }

        /** Returns a new PacketTracker with incremented sequence number, absolute number, and windowPosition for
         * the next DatagramPacket in the list */
        PacketTracker nextPacket() {
            short nextseqnum = PacketTracker.nextSequenceNumber(this.sequenceNumber);
            DatagramPacket nextPacket = Sender2b.assemblePacket(nextseqnum, this.absoluteNumber + 1);
            return new PacketTracker(nextseqnum, this.absoluteNumber + 1, (short)(this.windowPosition + 1), nextPacket);
        }

        /** Wrapper method for handling sequence number overflows */
        static short nextSequenceNumber(short seqNum) {
            int overflowed_seqnum = (int) seqNum + 1;
            short nextseqnum = (overflowed_seqnum > 32767) ? 1 : (short) overflowed_seqnum;
            return nextseqnum;
        }

        /** Wrapper method for handling sequence number overflows.
         * This one adds a number to given sequence number, instead of
         * just incrementing it.
         */
        static short seqNumAddition(short seqNum, int i) {
            int overflowed = (int) seqNum + i;
            short ret = (overflowed > 32767) ? (short)(overflowed - 32767) : (short) overflowed;
            return ret;
        }
    }
}


