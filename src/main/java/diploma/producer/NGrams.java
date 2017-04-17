package diploma.producer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Никита on 30.05.2016.
 */
public class NGrams {
    public static List<String> allNGrams(String str) {
        List<String> ngrams = new ArrayList<>();
        String[] words = str.split(" ");
        int n = words.length;
        for (int i = 1; i <= n; i++)
            ngrams.addAll(nGrams(i, words));
        return ngrams;
    }

    public static List<String> nGrams(int n, String[] words) {
        List<String> ngrams = new ArrayList<>();
        for (int i = 0; i < words.length - n + 1; i++)
            ngrams.add(concat(words, i, i+n));
        return ngrams;
    }

    private static String concat(String[] words, int start, int end) {
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < end; i++)
            sb.append(words[i] + " ");
        return sb.toString();
    }

    public static void main(String[] args) {
        long start = System.nanoTime();
        String str = "Журналист Павел Лобков уходит с \"Дождя\" из-за несогласия с новой политикой канала, а потом " +
                "собирается получить работу на яндекс радио для того, чтобы получать деньги";
        for (int i = 0; i < 1000; i++)
            allNGrams(str);//.forEach(System.out::println);
        System.out.println();
        System.out.println("\n" + (System.nanoTime() - start) / 1000000);
    }
}
