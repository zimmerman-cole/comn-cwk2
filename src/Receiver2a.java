import java.net.*;
import java.io.*;

public class Receiver2a
{
    private static final int header_length = 3;
    private static final int max_payload_length = 1024;

    // DEBUG OPTIONS:
    private static final boolean debug = false;
    private static final boolean dup_debug = false;

    private static DatagramSocket receiver_socket;
    private static String file_to_write;
    private static byte[] received_data = new byte[0];
    private static int num_duplicates = 0;


    public static void main(String[] args) {

        int portNumber = Integer.parseInt(args[0]);
        file_to_write = args[1];
        short window_size = (short) Integer.parseInt(args[2]);
        if (window_size <= 0 || window_size >= 32767) {
            System.out.println("Please enter a window size N such that 0 < N < 32767.");
            System.exit(0);
        }
        try {
            receiver_socket = new DatagramSocket(portNumber);
        } catch (SocketException e) {
            e.printStackTrace();
            System.out.println("SocketException occurred while initializing receiver socket (port # already in use?).");
            quit();
        }

        // Last-message byte is stored in position 0 in the header.
        // Sequence numbers for this part are stored in packet.getData()[1,2]
        // Sequence numbers are in range [1, 32767]
        short expectedseqnum = 1;
        int numRcv = 0;
        byte[] ack_data = new byte[6];
        System.arraycopy("ACK".getBytes(), 0, ack_data, 3, 3);

        // === Start receiving packets ============================
        try {
            System.out.println("Waiting for data now.");
            while (true) {
                DatagramPacket received_packet = new DatagramPacket(new byte[header_length + max_payload_length], header_length + max_payload_length);
                receiver_socket.receive(received_packet);


                short receivedSeq = (short)((received_packet.getData()[1] << 8) | received_packet.getData()[2] & 0xff);
                if (debug) {
                    System.out.println("==================================================");
                    if (expectedseqnum != receivedSeq) System.out.println("DUPLICATE/OUT OF ORDER RECEIVED:");
                    System.out.println("Exp seq: " + expectedseqnum);
                    System.out.println("Rcv seq: " + receivedSeq);
                } else if (dup_debug && expectedseqnum != receivedSeq) {
                    System.out.println("==================================================");
                    System.out.println("DUPLICATE/OUT OF ORDER RECEIVED:");
                    System.out.println("Exp seq: " + expectedseqnum);
                    System.out.println("Rcv seq: " + receivedSeq);
                }

                // Received packet is not a duplicate/out of order; store it and increment expected sequence number
                if (receivedSeq == expectedseqnum) {
                    // Store data:
                    byte[] tmp = new byte[received_data.length + received_packet.getLength() - header_length];
                    System.arraycopy(received_data, 0, tmp, 0, received_data.length);
                    System.arraycopy(received_packet.getData(), header_length, tmp, received_data.length, received_packet.getLength() - header_length);
                    received_data = tmp;

                    // Send ACK:
                    ack_data[0] = received_packet.getData()[0]; ack_data[1] = received_packet.getData()[1]; ack_data[2] = received_packet.getData()[2];
                    DatagramPacket ack = new DatagramPacket(ack_data, ack_data.length, received_packet.getAddress(), received_packet.getPort());
                    receiver_socket.send(ack);

                    // Increment expected sequence number:
                    expectedseqnum = (expectedseqnum == 32767) ? 1 : (short)(expectedseqnum + 1);
                    numRcv++;

                    // Received packet is duplicate/out of order; re-send ACK for last received packet
                } else {
                    short lastSeq = (expectedseqnum == 1) ? 32767 : (short)(expectedseqnum - 1);
                    ack_data[0] = received_packet.getData()[0];
                    ack_data[1] = (byte) ((lastSeq >> 8) & 0xff);
                    ack_data[2] = (byte) (lastSeq & 0xff);
                    DatagramPacket ack = new DatagramPacket(ack_data, ack_data.length, received_packet.getAddress(), received_packet.getPort());
                    receiver_socket.send(ack);
                    num_duplicates++;
                }

                // Exit loop upon receipt of last packet.
                if (received_packet.getData()[0] == 1) {
                    if (debug) System.out.println("==================================================");
                    System.out.println("Last packet received.\n");
                    break;
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("I/OError occurred while receiving a packet.");
            quit();
        }

        writeFile();

        System.out.println(numRcv + " packets were received in total.");
        System.out.println("Received file is " + received_data.length + " bytes.");
        System.out.println(num_duplicates + " duplicate/out of order packets were received in total.");


    }

    /**
     * Closes socket (if it exists) and exits.
     */
    private static void quit() {
        try {
            receiver_socket.close();
            System.exit(0);
        } catch (NullPointerException e) {
            System.exit(0);
        }
    }

    private static void writeFile() {
        try {
            File outputImage = new File(file_to_write);
            OutputStream out = new FileOutputStream(outputImage);
            out.write(received_data);
            out.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("Error finding .jpg file to write to.");
            quit();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("I/OError occurred writing image file.");
            quit();
        }
    }

}
