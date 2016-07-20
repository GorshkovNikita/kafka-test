package diploma.producer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Nikita on 19.06.2016.
 */
public class Reading implements Runnable {
    private Path path;
    private BlockingQueue<String> queue;

    public Reading(Path path, BlockingQueue<String> queue) {
        this.path = path;
        this.queue = queue;
    }

    public void run() {
        try (BufferedReader br = new BufferedReader(new FileReader(path.toString()))) {
            String line = null;
            int i = 0;
            do {
                line = br.readLine();
                if (line != null) {
                    queue.add(line);
                }
            } while (line != null);
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
