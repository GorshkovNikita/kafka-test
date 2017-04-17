package diploma.producer;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.simple.Document;
import edu.stanford.nlp.simple.Sentence;
import edu.stanford.nlp.util.CoreMap;

import java.util.*;

/**
 * @author Никита
 */
public class TextNormalizer {
    private static final Set<String> stopWordList = new HashSet<>(Arrays.asList(
            ",", ":", ".", "!", "?", "\"", "..", "...", "``", "''", ";", "'", "`", "<", ">", "=", "@", "$",
            "~", "==", "===", "-", "+", "_", "__", "#", "^", "*", "(", ")", "{", "}", "[", "]", "%",
            "-lrb-", "-rrb-", "-lsb-", "-rsb-", "\\", "|", "/", "||", "--", "a", "about", "above", "above", "across", "after", "afterwards",
            "again", "against", "all", "almost", "alone", "along", "already", "also", "although", "always","am","among", "amongst", "amoungst",
            "amount",  "an", "and", "another", "any", "anyhow","anyone","anything","anyway", "anywhere", "are", "around", "as",  "at", "back","be",
            "became", "because","become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides",
            "between", "beyond", "bill", "both", "bottom","but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry",
            "de",  "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven","else", "elsewhere",
            "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill",
            "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give",
            "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him",
            "himself", "his", "how", "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep",
            "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover",
            "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody",
            "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others",
            "otherwise", "our", "ours", "ourselves", "out", "over", "own","part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem",
            "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow",
            "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them",
            "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", "third",
            "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty",
            "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where",
            "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose",
            "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "the","time","today","people","person",
            "year","way","day", "thing","man","world","life","hand","part","child","eye","woman","place","work","week","case","point","government","company",
            "number", "group","problem","fact","follow","follower","love",
//            "weightlifting", "sports", "sport", "football", "cycling", "snooker", "tennis", "hockey", "skating", "superbike"
            "thank", "thanks", "girl", "click", "shit", "bitch", "friend", "game", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "weekend",
            "home", "fuck", "season", "january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december",
            "month", "year", "winter", "autumn", "fall", "spring", "summer", "team","birthday","baby","nigga","tonight", "tomorrow","yesterday", "today","retweet","twitter","tweet","live",
            "morning", "evening", "noon","afternoon","night","photo","news","woman","man","women","men","lmao","dude","congrat","congrats","somebody","everybody","player","white",
            "hour","minute","second","reason","picture","view","head","heart","opening","http","future","free","update","club","watch","state","story","stadium","money",
            "size","word","ticket","games","hope","feeling","account","need","john"
    ));


    private StanfordCoreNLP pipeline;
    private static TextNormalizer textNormalizer;

    public static TextNormalizer getInstance() {
        if (textNormalizer == null)
            textNormalizer = new TextNormalizer();
        return textNormalizer;
    }

    private TextNormalizer() {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma");
        pipeline = new StanfordCoreNLP(props);
    }

    private Annotation normalize(String text) {
        Annotation document = new Annotation(text);
        pipeline.annotate(document);
        return document;
    }

    public String normalizeToString(String text) {
        String resultString = "";
        Annotation document = normalize(text);
        List<CoreLabel> tokens = document.get(CoreAnnotations.TokensAnnotation.class);

        for(CoreMap token: tokens) {
            String lemma = token.get(CoreAnnotations.LemmaAnnotation.class);
            String pos = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);
            lemma = lemma.toLowerCase();
            if (!stopWordList.contains(lemma) && !lemma.startsWith("https://") && !lemma.startsWith("http://") && (lemma.length() >= 4) && isNoun(pos) && !lemma.equals("") && !lemma.startsWith("@"))
                resultString += lemma + " ";
        }

        return resultString;
    }

    public boolean isNoun(String pos) {
        return Arrays.asList("NN", "NNS", "NNP", "NNPS").contains(pos);
    }

    public boolean isVerbNounOrAdjective(String pos) {
        return Arrays.asList("NN", "NNS", "NNP", "NNPS", "JJ", "VB").contains(pos);
    }

    public String simpleNormalize(String text) {
        Document doc = new Document(text);
        String normalizedTest = "";
        for (Sentence sentence: doc.sentences()) {
            for (String lemma: sentence.lemmas()) {
                normalizedTest += lemma;
            }
        }
        return normalizedTest;
    }

    public String deleteStopWords(String text) {
        String textWithoutStopWords = "";
        String[] words = text.split(" ");
        for (String word: words) {
            if (!stopWordList.contains(word) && !word.startsWith("https://"))
                textWithoutStopWords += word + " ";
        }
        return textWithoutStopWords;
    }

}
