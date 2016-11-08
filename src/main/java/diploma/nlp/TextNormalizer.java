package diploma.nlp;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

import java.util.*;

/**
 * @author Никита
 */
public class TextNormalizer {
    private static final Set<String> stopWordList = new HashSet<>(Arrays.asList(
            ",", ":", ".", "!", "?", "\"", "..", "...", "``", "''", ";", "'", "`", "<", ">", "=", "@", "$",
            "~", "==", "===", "-", "+", "_", "__", "#", "^", "*", "(", ")", "{", "}", "[", "]", "%",
            "-lrb-", "-rrb-", "-lsb-", "-rsb-", "\\", "|", "/", "||", "--"));

    private StanfordCoreNLP pipeline;

    public TextNormalizer() {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma");
        pipeline = new StanfordCoreNLP(props);
    }

    public String normalize(String text) {
        String resultString = "";
        Annotation document = new Annotation(text);
        pipeline.annotate(document);

        List<CoreLabel> tokens = document.get(CoreAnnotations.TokensAnnotation.class);

        for(CoreMap token: tokens) {
            String lemma = token.get(CoreAnnotations.LemmaAnnotation.class);
            if (!stopWordList.contains(lemma))
                resultString += lemma + " ";
        }

        return resultString;
    }

    public static void main(String[] args) {
        System.out.println(new TextNormalizer().normalize("iPhone7 Plus)))) (International Giveaway) @had \"have\" :) #iPhone7Plus https://t.co/bork4f2dsk"));
    }
}
