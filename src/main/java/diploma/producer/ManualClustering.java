package diploma.producer;

import com.mysql.jdbc.exceptions.MySQLIntegrityConstraintViolationException;
import diploma.statistics.dao.*;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Никита
 */
public class ManualClustering {
    /**
     * args[0] - файл-источник с твитами
     * @param args
     */
    public static void main(String[] args) {
        if (args.length < 1) return;
        Map<String, Integer> topWords = new HashMap<>();
        Map<String, Integer> topBigrams = new HashMap<>();
        int numberOfDocuments = 0;
        TweetDao tweetDao = new TweetDao();
        TweetTermDao tweetTermDao = new TweetTermDao();
        TweetBigramDao tweetBigramDao = new TweetBigramDao();
        BigramFrequencyDao bigramFrequencyDao = new BigramFrequencyDao();
        TermFrequencyDao termFrequencyDao = new TermFrequencyDao();
        try (BufferedReader br = new BufferedReader(new FileReader(args[0]))) {
            String line = null;
            do {
                line = br.readLine();
                if (line != null && !line.equals("")) {
                    try {
                        numberOfDocuments++;
                        Status status = TwitterObjectFactory.createStatus(line);
                        tweetDao.saveTweet(status);
                        String[] normalizedTerms = TextNormalizer.getInstance().normalizeToString(status.getText()).split(" ");
                        List<String> bigrams = NGrams.nGrams(2, normalizedTerms);
                        for (String term : normalizedTerms) {
                            try {
                                if (topWords.containsKey(term)) topWords.put(term, topWords.get(term) + 1);
                                else topWords.put(term, 1);
                                tweetTermDao.saveTweetTerm(String.valueOf(status.getId()), term);
                            }
                            catch (Exception ex) {
                                String tweetId = String.valueOf(status.getId());
                                System.out.println(ex.getMessage());
                            }
                        }
                        for (String bigram : bigrams) {
                            try {
                                if (topBigrams.containsKey(bigram)) topBigrams.put(bigram, topBigrams.get(bigram) + 1);
                                else topBigrams.put(bigram, 1);
                                tweetBigramDao.saveTweetBigram(String.valueOf(status.getId()), bigram);
                            }
                            catch (Exception ex) {
                                String tweetId = String.valueOf(status.getId());
                                System.out.println(ex.getMessage());
                            }
                        }
                    }
                    catch (TwitterException ex) {
                        System.err.println("can not create tweet for json = " + line);
                    }
                }
            } while (line != null);
            br.close();
        }
        catch (IOException | IllegalArgumentException ex) {
            ex.printStackTrace();
        }
        topBigrams = MapUtil.sortByValue(topBigrams);
        topWords = MapUtil.sortByValue(topWords);
        for (Map.Entry<String, Integer> bigramAndItsFrequency : topBigrams.entrySet())
            bigramFrequencyDao.updateBigramFrequency(bigramAndItsFrequency.getKey(), bigramAndItsFrequency.getValue());
        for (Map.Entry<String, Integer> termAndItsFrequency : topWords.entrySet())
            termFrequencyDao.updateTermFrequency(termAndItsFrequency.getKey(), termAndItsFrequency.getValue());
    }
}
