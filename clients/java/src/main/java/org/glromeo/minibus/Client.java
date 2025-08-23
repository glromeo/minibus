package org.glromeo.minibus;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;

import java.io.*;
import java.net.Socket;
import java.util.Base64;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Client {

  public static void connect(String host, int port) throws IOException {
    Socket socket = new Socket(host, port);
    PrintWriter out = new PrintWriter(socket.getOutputStream());
    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    handshake(host, out);

    // read HTTP response headers
    String line;
    while (!(line = in.readLine()).isEmpty()) {
      System.out.println(line);
    }

    Gson gson = new GsonBuilder()
        .registerTypeAdapter(
            UUID.class,
            (JsonDeserializer<UUID>) (json, typeOfT, ctx) -> UUID.fromString(json.getAsString())
        )
        .create();

    // --- Send a single text frame ---
    String msg = "Hello WebSocket!";
    byte[] data = msg.getBytes(UTF_8);
    OutputStream rawOut = socket.getOutputStream();

    // Frame header (FIN + text frame opcode)
    rawOut.write(0x81);
    // client-to-server frames must be masked
    byte[] mask = {1, 2, 3, 4};
    rawOut.write(0x80 | data.length);
    rawOut.write(mask);

    for (int i = 0; i < data.length; i++) {
      rawOut.write(data[i] ^ mask[i % 4]);
    }
    rawOut.flush();

    // --- Read echoed frame (simplified, assumes small text) ---
    InputStream rawIn = socket.getInputStream();
    int b1 = rawIn.read(); // FIN+opcode
    int b2 = rawIn.read(); // MASK+len
    int len = b2 & 0x7F;
    byte[] payload = new byte[len];
    rawIn.read(payload);

    System.out.println("Echo: " + new String(payload, UTF_8));

    socket.close();
  }

  private static void handshake(String host, PrintWriter out) {
    String key = Base64.getEncoder().encodeToString("randomkey".getBytes());
    out.print("GET /minibus HTTP/1.1\r\n");
    out.print("Host: " + host + "\r\n");
    out.print("Upgrade: websocket\r\n");
    out.print("Connection: Upgrade\r\n");
    out.print("Sec-WebSocket-Key: " + key + "\r\n");
    out.print("Sec-WebSocket-Version: 13\r\n");
    out.print("\r\n");
    out.flush();
  }
}
