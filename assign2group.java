
package assignment2;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import assignment2.assign2.ReduceCallback;

public class assign2group {

    public static void main(String[] args) {

        ////////////
        // INPUT:
        ///////////



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
        System.out.println("Number of threads: " + Integer.parseInt(args[0]));

        for(int i = 0; i<10; i++) {
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Integer.parseInt(args[0]));

            int startTime = (int) System.currentTimeMillis();

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

                List<Thread> mapCluster = new ArrayList<Thread>(input.size());

                Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
                while (inputIter.hasNext()) {
                    Map.Entry<String, String> entry = inputIter.next();
                    final String file = entry.getKey();
                    final String contents = entry.getValue();

                    Thread t = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            map(file, contents, mapCallback);
                        }
                    });
                    mapCluster.add(t);
                    t.start();
                }

                // wait for mapping phase to be over:
                for (Thread t : mapCluster) {
                    try {
                        t.join();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                // GROUP:
                Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

                final GroupCallback<Map<String, List<String>>> groupCallback = new GroupCallback<Map<String, List<String>>>() {

                    @Override
                    public synchronized void groupDone(String k, List<String> v) {
                        groupedItems.put(k, v);
                    }
                };


                Iterator<MappedItem> mappedIter = mappedItems.iterator();
                while (mappedIter.hasNext()) {
                    MappedItem item = mappedIter.next();
                    String word = item.getWord();
                    String file = item.getFile();
                    List<String> list = groupedItems.get(word);

                    executor.execute(() -> {
                        group(word, file, list, groupCallback);
                    });

                }

                executor.shutdown();
                while (!executor.isTerminated()) ;


                // REDUCE:

                final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                    @Override
                    public synchronized void reduceDone(String k, Map<String, Integer> v) {
                        output.put(k, v);
                    }
                };

                List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());

                Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
                while (groupedIter.hasNext()) {
                    Map.Entry<String, List<String>> entry = groupedIter.next();
                    final String word = entry.getKey();
                    final List<String> list = entry.getValue();

                    Thread t = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            reduce(word, list, reduceCallback);
                        }
                    });
                    reduceCluster.add(t);
                    t.start();
                }

                // wait for reducing phase to be over:
                for (Thread t : reduceCluster) {
                    try {
                        t.join();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                int endTime = (int) System.currentTimeMillis();
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

    public static void map(String file, String contents, List<MappedItem> mappedItems) {
        String[] words = contents.replaceAll("[^a-zA-Z ]", " ").toLowerCase().split("\\s+");
        for(String word: words) {
            mappedItems.add(new MappedItem(word, file));
        }
    }

    public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        output.put(word, reducedList);
    }

    public static interface MapCallback<E, V> {

        public void mapDone(E key, List<V> values);
    }

    public static interface ReduceCallback<E, K, V> {

        public void reduceDone(E e, Map<K,V> results);
    }
    
    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
        String[] words = contents.trim().split("\\s+");
        List<MappedItem> results = new ArrayList<MappedItem>(words.length);
        for(String word: words) {
            results.add(new MappedItem(word, file));
        }
        callback.mapDone(file, results);
    }

    public static interface GroupCallback<E> {
        public void groupDone(String v, List<String> k);
    }

    public static void group(String word, String file,  List<String> list, GroupCallback<Map<String, List<String>>> groupCallback) {

        if (list == null) {
            list = new LinkedList<String>();
        }
        list.add(file);
        groupCallback.groupDone(word, list);
    }

    public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

        Map<String, Integer> reducedList = new HashMap<String, Integer>();

        for(String file: list) {
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

