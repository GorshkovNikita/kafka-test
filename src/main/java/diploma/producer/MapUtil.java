package diploma.producer;

import java.util.*;

/**
 * @author Никита
 */
public class MapUtil {
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue( Map<K, V> map ) {
        List<Map.Entry<K, V>> list =
                new LinkedList<Map.Entry<K, V>>( map.entrySet() );
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return -1 * (o1.getValue()).compareTo(o2.getValue());
            }
        });

        Map<K, V> result = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            result.put( entry.getKey(), entry.getValue() );
        }
        return result;
    }

    public static <K, V> Map<K, V> putFirstEntries(int max, Map<K,V> source) {
        int count = 0;
        Map<K,V> target = new LinkedHashMap<K,V>();
        for (Map.Entry<K,V> entry:source.entrySet()) {
            if (count >= max) break;
            target.put(entry.getKey(), entry.getValue());
            count++;
        }
        return target;
    }

    public static <V> Map<String, V> mapFromArray(V[] array) {
        Map<String, V> map = new HashMap<>();
        for (int i = 0; i < array.length; i++)
            map.put(String.valueOf(i), array[i]);
        return map;
    }
}
