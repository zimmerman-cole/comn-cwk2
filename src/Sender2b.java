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

        // "socket_thread" is responsible for (re)transmitting packets and receiving ACKs.
        // The main thread's job is to add packets for socket_thread to send and monitor when it
        // detects there's space in the window.
        SocketThread socket_thread = new SocketThread();
        new Thread(socket_thread).start();

        // =============================================================================================================
        // === Start sending the data ==================================================================================
        // =============================================================================================================
        System.out.println("Sending data now.");

        //                          new PacketTracker(sequenceNumber, absoluteNumber, windowPosition, Timestamp, DatagramPacket)
        PacketTracker firstPacket = new PacketTracker((short) 0, 0, (short) 0, new Timestamp(0), assemblePacket((short) 0, 0));
        // Add first packet to pipeline:
        socket_thread.addPacket(firstPacket);

        long start_time = System.currentTimeMillis();

        // Loops through each packet to be sent
        while (true) {

            // Contains all information (seqNum, absNum, windowPosition, etc.) about last sent packet.
            PacketTracker last_sent_packet = socket_thread.lastPacketSent();

            // All packets have been added to the pipeline. Wait for socket_thread to receive all ACKs.
            if (last_sent_packet.absoluteNumber == data_packets.length - 1) break;

            // If space in window, add another packet to the pipeline:
            if (last_sent_packet.windowPosition < (window_size - 1)) {
                socket_thread.addPacket(last_sent_packet.nextPacket());
            }
        }

        // Wait for socket_thread to finish.
        while (true) {
            if (socket_thread.isFinished()) break;
        }

        double trans_time = (System.currentTimeMillis() - start_time) / 1000.0;

        System.out.println("All data has been sent and accounted for.\n");
        System.out.println("There were " + socket_thread.numRetransmissions() + " retransmissions in total.");
        System.out.println("Total transmission time: " + trans_time + " seconds.");
        System.out.println("Average throughput: " + (data_packets.length / trans_time) + " KB/s.");

    }

    /**
     * This method reads the specified file in 'filename' into byte array 'data', and then splits that into
     * packet-sized chunks and stores those in byte-array array 'data_packets'.
     */
    private static void load_file() {

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


}

/**
 * A class that keep tracks of a packet's:
 *      -sequence number (and handles related overflows)
 *      -absolute number
 *      -window position
 *      -receipt status
 *      -time at which it should be re-transmitted
 */
class PacketTracker implements Cloneable {

    short sequenceNumber;
    int absoluteNumber;
    short windowPosition;
    boolean received;
    boolean lastPacket;
    Timestamp timestamp;
    DatagramPacket packet;

    PacketTracker(short sequenceNumber, int absoluteNumber, short windowPosition, Timestamp timestamp, DatagramPacket packet) {
        this.absoluteNumber = absoluteNumber;
        this.sequenceNumber = sequenceNumber;
        this.windowPosition = windowPosition;
        this.received = false;
        this.lastPacket = false;
        this.timestamp = timestamp;
        this.packet = packet;
    }

    PacketTracker nextPacket() {
        short seqnum = (this.sequenceNumber == 32767) ? 0 : (short)(this.sequenceNumber + 1);
        DatagramPacket nextPacket = Sender2b.assemblePacket(seqnum, this.absoluteNumber + 1);
        return new PacketTracker(seqnum, this.absoluteNumber + 1, (short)(this.windowPosition + 1), new Timestamp(System.currentTimeMillis()), nextPacket);
    }

    /** clone() */
    PacketTracker duplicate() {
        return new PacketTracker(this.sequenceNumber, this.absoluteNumber, this.windowPosition, this.timestamp, this.packet);
    }

}

/**
 * Responsible for sending, receiving ACKS for, and re-transmitting packets. Adds packet to pipeline when told to by
 * main thread.
 */
class SocketThread implements Runnable {

    private final boolean debug = true;
    private final boolean promptEachTime = true;
    private final boolean lock_debug = true;

    // Maps window positions to packet trackers. This only contains
    // packets yet to be acknowledged in the window:
    HashMap<Short, PacketTracker> to_be_acked;
    // Maps sequence numbers to packet trackers. This contains all
    // packets (ACKed and unACKED) in the window:
    HashMap<Short, PacketTracker> window_packets;
    // The window position of the next packet to be re-transmitted:
    private short next_retransmission = 0;

    private final Object last_packet_lock = new Object();
    private PacketTracker last_sent_packet = null;

    private final Object window_lock = new Object();
    private final Object socket_lock = new Object();
    private DatagramSocket socket;
    private DatagramPacket ack;

    private final Object finished_lock = new Object();
    private boolean finished;
    private boolean last_packet_received;

    private int num_retransmissions = 0;

    SocketThread() {
        this.to_be_acked = new HashMap<Short, PacketTracker>();
        this.window_packets = new HashMap<Short, PacketTracker>();
        try {
            this.socket = new DatagramSocket();
        } catch (SocketException e) {
            e.printStackTrace();
            System.out.println("Error initializing sender socket.");
            this.quit();
        }
        this.ack = new DatagramPacket(new byte[Sender2b.header_length + 3], Sender2b.header_length + 3);
        this.finished = false;
        this.last_packet_received = false;
    }

    boolean isFinished() {
        boolean f = false;
        synchronized (this.finished_lock) {
            if (this.lock_debug) System.out.println("Finished lock acquired by isFinished().");
            f = this.finished;
        }
        if (this.lock_debug) System.out.println("Finished lock released by isFinished().");
        return f;
    }

    int numRetransmissions() {
        return this.num_retransmissions;
    }

    PacketTracker lastPacketSent() {
        PacketTracker tracker = null;
        synchronized (this.last_packet_lock) {
            //if (this.lock_debug) System.out.println("Last packet lock acquired in lastPacketSent().");
            tracker = this.last_sent_packet.duplicate();
        }
        //if (this.lock_debug) System.out.println("Last packet lock released by lastPacketSent().");
        return tracker;
    }

    /**
     * Add packet to pipeline: send packet then add it to window_packets and to_be_acked.
     * @param tracker
     */
    void addPacket(PacketTracker tracker) {
        // Send packet
        synchronized (this.socket_lock) {
            if (this.lock_debug) System.out.println("Socket lock acquired to send packet " + tracker.sequenceNumber + ", absNum: " + tracker.absoluteNumber + ".");
            this.sendPacket(tracker);
        }
        if (this.lock_debug) System.out.println("Socket lock released.");

        // Add packet to the list to be monitored/re-transmitted if necessary
        synchronized (this.window_lock) {
            if (this.lock_debug) System.out.println("Window lock acquired to add packet " + tracker.sequenceNumber + ", absNum: " + tracker.absoluteNumber + " to pipeline.");
            tracker.timestamp.setTime(System.currentTimeMillis() + Sender2b.timeout);
            this.to_be_acked.put(tracker.windowPosition, tracker);
            this.window_packets.put(tracker.sequenceNumber, tracker);
        }
        if (this.lock_debug) System.out.println("Window lock released.");

        synchronized (this.last_packet_lock) {
            if (this.lock_debug) System.out.println("Last packet lock acquired for packet " + tracker.sequenceNumber + ", absNum: " + tracker.absoluteNumber + ".");
            this.last_sent_packet = tracker;
        }
        if (this.lock_debug) System.out.println("Last packet lock released.");
    }

    @Override
    public void run() {
        while (true) {
            PacketTracker next_to_be_retransmitted;
            synchronized (this.window_lock) {
                if (this.lock_debug) System.out.println("Window lock acquired for socket_thread while loop.");
                this.printDebug();

                // Retrieves the packet with the lowest time until timeout
                next_to_be_retransmitted = this.to_be_acked.get(this.next_retransmission);

                if (next_to_be_retransmitted == null) continue;

                synchronized (this.socket_lock) {
                    if (this.lock_debug) System.out.println("Socket lock acquired for socket_thread while loop.");
                    int time_until_retransmission = (int) (next_to_be_retransmitted.timestamp.getTime() - System.currentTimeMillis());
                    if (time_until_retransmission <= 5) {
                        this.sendPacket(next_to_be_retransmitted);
                    } else {
                        try {
                            this.socket.setSoTimeout(time_until_retransmission);
                            this.socket.receive(this.ack);
                        } catch (SocketTimeoutException e) {
                            // TIMEOUT: re-send packet and reset its timer
                            this.num_retransmissions++;
                            try {
                                this.socket.send(next_to_be_retransmitted.packet);
                            } catch (IOException ex) {
                                ex.printStackTrace();
                                System.out.println("IO/Error retransmitting packet " + next_to_be_retransmitted.absoluteNumber + ", sequence number: " + next_to_be_retransmitted.sequenceNumber);
                                this.quit();
                            }
                            // Update next_to_be_retransmitted's timestamp, update next_retransmission
                            this.to_be_acked.get(this.next_retransmission).timestamp.setTime(System.currentTimeMillis() + Sender2b.timeout);
                            this.next_retransmission = (this.next_retransmission == Sender2b.window_size - 1) ? 0 : this.next_retransmission++;
                            continue;

                        } catch (IOException e) {
                            e.printStackTrace();
                            System.out.println("Error receiving ACK.");
                            this.quit();
                        }
                    }
                } // Release socket lock
                if (this.lock_debug) System.out.println("Socket lock released by socket_thread while loop.");

                // =====================================================================================================
                // === ACK RECEIVED: ===================================================================================
                // =====================================================================================================
                short receivedSeqNum = (short) ((this.ack.getData()[1] << 8) | this.ack.getData()[2] & 0xff);

                // Reset next_to_be_transmitted's timestamp, update next_retransmission
                this.to_be_acked.get(this.next_retransmission).timestamp.setTime(System.currentTimeMillis() + Sender2b.timeout);
                this.next_retransmission = (this.next_retransmission == Sender2b.window_size - 1) ? 0 : this.next_retransmission++;

                // Check if duplicate ACK, ignore if so
                PacketTracker receivedPacket = this.window_packets.get(receivedSeqNum);
                if (receivedPacket == null || receivedPacket.received) continue;

                // ACK is not a duplicate; mark as received, remove from to_be_acked and
                // adjust window if necessary
                this.to_be_acked.remove(receivedPacket.windowPosition);
                if (receivedPacket.windowPosition != 0) continue;
                else this.window_packets.remove(receivedSeqNum);

                // Set last_packet_received=true if this ACK's EOF bit=1
                if (this.ack.getData()[0] == 1) this.last_packet_received = true;


                // === Adjust window =================================================
                // find the current window position of the new send_base; remove packets from before new send_base
                short new_send_base_seqnum = this.nextSequenceNumber(receivedSeqNum);
                short new_send_base_pos = 1;
                for (short i = 1; i < this.window_packets.size(); i++) {
                    if (window_packets.get(i).received) {
                        // Remove packets from before new send_base from window
                        this.window_packets.remove(new_send_base_seqnum);

                        new_send_base_pos++;
                        new_send_base_seqnum = this.nextSequenceNumber(new_send_base_seqnum);
                    } else break;
                }

                // If last packet has been received and window is empty,
                // then all packets have been accounted for; exit thread
                if (this.last_packet_received && this.window_packets.size() == 0) {
                    synchronized (this.finished_lock) {
                        if (this.lock_debug) System.out.println("Finished lock acquired by socket_thread while loop.");
                        this.finished = true;
                    }
                    if (this.lock_debug) System.out.println("Finished lock released by socket_thread while loop.");
                    break;
                }

                // Update window positions in to_be_acked
                for (short pos : this.to_be_acked.keySet()) {
                    this.to_be_acked.put((short)(pos - new_send_base_pos), this.to_be_acked.get(pos));
                }

                // Update window positions in window_packets
                for (short pos : this.window_packets.keySet()) {
                    PacketTracker oldTracker = this.window_packets.get(pos);
                    oldTracker.windowPosition -= new_send_base_pos;
                }

                // Update next_retransmission's window position to reflect the window shift
                this.next_retransmission -= new_send_base_pos;

                // Update last_packet_sent's window position to reflect the window shift
                synchronized (this.last_packet_lock) {
                    if (this.lock_debug) System.out.println("Last packet lock acquired by socket_thread while loop.");
                    this.last_sent_packet.windowPosition -= new_send_base_pos;
                }
                if (this.lock_debug) System.out.println("Last packet lock released by socket_thread while loop.");
                // =====================================================================================================
                // =====================================================================================================
                // =====================================================================================================

            } // Release window lock
            if (this.lock_debug) System.out.println("Window lock released by socket_thread while loop.");

        } // End of while loop
    }

    /**
     * Print debug information.
     * @param
     */
    private void printDebug() {
        if (this.promptEachTime) {
            try {
                new BufferedReader(new InputStreamReader(System.in)).readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (this.debug) {
            System.out.println("=============================================================");
            if (this.to_be_acked.size() == 0) {
                System.out.println("NO PACKETS WAITING TO BE ACKED.");
            } else {
                System.out.println("PACKETS IN WINDOW NOT ACKED:");
                System.out.println("(seqNum, absNum) ==> (windowPos, timeUntilRetransmission)");
                for (PacketTracker packet : this.to_be_acked.values()) {
                    int retransmit_time = (int)(packet.timestamp.getTime() - System.currentTimeMillis());
                    System.out.println("(" + packet.sequenceNumber + ", " + packet.absoluteNumber + ") ==> (" + packet.windowPosition + ", " + retransmit_time + ")");
                }
            }
            System.out.println();
            if (this.window_packets.size() == 0) {
                System.out.println("NO PACKETS CURRENTLY IN WINDOW.");
            } else {
                System.out.println("PACKETS IN WINDOW:");
                System.out.println("(seqNum, absNum) ==> (windowPos, isReceived)");
                for (PacketTracker packet : this.window_packets.values()) {
                    System.out.println("(" + packet.sequenceNumber + ", " + packet.absoluteNumber + ") ==> (" + packet.windowPosition + ", " + packet.received + ")");
                }
            }
            System.out.println();
            if (this.last_sent_packet != null) {
                System.out.println("LAST SENT PACKET: ");
                System.out.println("seqNum: " + this.last_sent_packet.sequenceNumber + ", absNum: " + this.last_sent_packet.absoluteNumber + ", windowPos: " +
                    this.last_sent_packet.windowPosition + ".");
            } else {
                System.out.println("NO PACKET HAS BEEN SENT YET.");
            }
            System.out.println("=============================================================");

        }

    }

    /**
     * Wrapper method for handling overflows when incrementing sequence numbers.
     * @param seqnum
     * @return
     */
    private short nextSequenceNumber(short seqnum) {
        int overflowed_seqnum = (int) seqnum + 1;
        short seq = (overflowed_seqnum > 32767) ? (short)(overflowed_seqnum - 32767 - 1) : (short) overflowed_seqnum;
        return seq;
    }

    /**
     * Closes socket if it exists.
     */
    private void quit() {
        synchronized (socket_lock) {
            try {
                this.socket.close();
                System.exit(0);
            } catch (NullPointerException e) {
                System.exit(0);
            }
        }
    }

    /**
     * A wrapper function for sending packets.
     * @param tracker
     */
    private void sendPacket(PacketTracker tracker) {
        try {
            this.socket.send(tracker.packet);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("I/O error sending packet " + tracker.absoluteNumber + ", sequence number: " + tracker.sequenceNumber + ".");
            this.quit();
        }
    }

}
