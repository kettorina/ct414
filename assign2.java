package assignment2;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class assign2 {

    public static void main(String args[]) {


        int count = args.length;


        Map<String, String> input = new HashMap<String, String>();

        List<String> fileNames = new ArrayList<>();
        for(int i = 1; i < count; i++){
            fileNames.add(args[i]);
        }

        for(String name: fileNames){
            System.out.println(name);
        }

        try {

            for (int j = 0; j < fileNames.size(); j++) {
                BufferedReader br = new BufferedReader(new FileReader(fileNames.get(j)));
                    StringBuilder sb = new StringBuilder();
                    String line = br.readLine();

                    while (line != null) {
                        sb.append(line);
                        sb.append(System.lineSeparator());
                        line = br.readLine();
                    }
                    input.put(fileNames.get(j), sb.toString());
            }
        } catch (FileNotFoundException fileNotFoundException){
            System.err.println("File not found");
            fileNotFoundException.printStackTrace();
        } catch (IOException ioException){
            System.err.println("IO Exception");
            ioException.printStackTrace();
        }
        ArrayList times = new ArrayList();

        for(int i = 0; i<10; i++) {

            int startTime = (int)System.currentTimeMillis();
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Integer.parseInt(args[0]));

            // APPROACH #3: Distributed MapReduce
            {
                final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

                // MAP:

                final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

                final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                    @Override
                    public synchronized void mapDone(String file, List<MappedItem> results) {
                        mappedItems.addAll(results);
                    }
                };


                Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
                while (inputIter.hasNext()) {
                    Map.Entry<String, String> entry = inputIter.next();
                    final String file = entry.getKey();
                    final String contents = entry.getValue();

                    executor.submit(() -> {
                        map(file, contents, mapCallback);
                    });
                }


                executor.shutdown();
                while (!executor.isTerminated()) ;

                executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Integer.parseInt(args[0]));

                // GROUP:

                Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

                Iterator<MappedItem> mappedIter = mappedItems.iterator();
                while (mappedIter.hasNext()) {
                    MappedItem item = mappedIter.next();
                    String word = item.getWord();
                    String file = item.getFile();
                    List<String> list = groupedItems.get(word);
                    if (list == null) {
                        list = new LinkedList<String>();
                        groupedItems.put(word, list);
                    }
                    list.add(file);
                }

                // REDUCE:

                final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                    @Override
                    public synchronized void reduceDone(String k, Map<String, Integer> v) {
                        output.put(k, v);
                    }
                };


                Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
                while (groupedIter.hasNext()) {
                    Map.Entry<String, List<String>> entry = groupedIter.next();
                    final String word = entry.getKey();
                    final List<String> list = entry.getValue();

                    executor.submit(() -> {
                        reduce(word, list, reduceCallback);
                    });

                }


                executor.shutdown();
                while (!executor.isTerminated()) ;
                int endTime = (int)System.currentTimeMillis();
                int execution_time = (endTime - startTime);
                times.add(execution_time);
                //System.out.println(output);
                System.out.println("Execution time: " + execution_time);
            }
        }

        int total = 0;
        for(int i = 0; i < times.size(); i++){
            total += (int)times.get(i);
        }
        System.out.println("Average execution time: "+(total/times.size()));
    }
    public static interface MapCallback<E, V> {

        public void mapDone(E key, List<V> values);
    }

    public static void map(String file, String contents, assign2.MapCallback<String, assign2.MappedItem> callback) {
        String[] words = contents.replaceAll("[^a-zA-Z ]", " ").toLowerCase().split("\\s+");
        List<assign2.MappedItem> results = new ArrayList<assign2.MappedItem>(words.length);
        for (String word : words) {
            results.add(new assign2.MappedItem(word, file));
        }
        callback.mapDone(file, results);
    }

//    public  static interface GroupCallback<E, K, V> {
//        public void groupDone(E e, Map<K, V> results);
//    }

    public static interface ReduceCallback<E, K, V> {

        public void reduceDone(E e, Map<K, V> results);
    }

    public static void reduce(String word, List<String> list, assign2.ReduceCallback<String, String, Integer> callback) {

        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for (String file : list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        callback.reduceDone(word, reducedList);
    }

    private static class MappedItem {

        private final String word;
        private final String file;

        public MappedItem(String word, String file) {
            this.word = word;
            this.file = file;
        }

        public String getWord() {
            return word;
        }

        public String getFile() {
            return file;
        }

        @Override
        public String toString() {
            return "[\"" + word + "\",\"" + file + "\"]";
        }
    }


}
