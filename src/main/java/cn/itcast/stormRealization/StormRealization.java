package cn.itcast.stormRealization;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StormRealization {
        private Random random=new Random();
        private BlockingQueue sentenceQune=new ArrayBlockingQueue(5000);
        private BlockingQueue wordQueue=new ArrayBlockingQueue(5000);

    public BlockingQueue getSentenceQune() {
        return sentenceQune;
    }

    public void setSentenceQune(BlockingQueue sentenceQune) {
        this.sentenceQune = sentenceQune;
    }

    public BlockingQueue getWordQueue() {
        return wordQueue;
    }

    public void setWordQueue(BlockingQueue wordQueue) {
        this.wordQueue = wordQueue;
    }

    Map<String,Integer>counters=new HashMap<String,Integer>();
        public void nextTuple(){
            String[] sentences = new String[]{"the cow jumped over the moon",
                    "the cow jumped over the moon",
                    "the cow jumped over the moon",
                    "the cow jumped over the moon", "the cow jumped over the moon"};
            String sentence=sentences[random.nextInt(sentences.length)];
            try {
                sentenceQune.put(sentence);
                System.out.println("send sentence:"+sentence);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        public void split(String sentence){
            System.out.println("recevice sentence:"+sentence);
            String[] words = sentence.split(" ");
            for (String word:words){
                if (!word.isEmpty()){
                    word=word.toLowerCase();
                    wordQueue.add(word);
                    System.out.println("split word:"+word);
                }
            }
        }
        public void wordCount(String word){
            if(!counters.containsKey(word)){
                counters.put(word,1);
            }else {
                Integer c=counters.get(word)+1;
                counters.put(word,c);

            }
            System.out.println("count word:"+counters);
        }

    public static void main(String[] args) {
        ExecutorService executorService= Executors.newFixedThreadPool(100);
        StormRealization stormRealization = new StormRealization();
        executorService.submit(new MySpout(stormRealization));
        executorService.submit(new MyBoltSplit(stormRealization));
        executorService.submit(new MyBoltCount(stormRealization));
    }
}
class MySpout extends  Thread{
    private StormRealization stormRealization;

    public MySpout(StormRealization stormRealization) {
        this.stormRealization = stormRealization;
    }

    @Override
    public void run() {
        while (true){
            stormRealization.nextTuple();
            try {
                this.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
class MyBoltSplit extends Thread{
    private StormRealization stormRealization;

    public MyBoltSplit(StormRealization stormRealization) {
        this.stormRealization = stormRealization;
    }

    @Override
    public void run() {
        while(true){
            try {
                String sentence =(String) stormRealization.getSentenceQune().take();
                stormRealization.split(sentence);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}
class MyBoltCount extends Thread{
    private StormRealization stormRealization;

    public MyBoltCount(StormRealization stormRealization) {
        this.stormRealization = stormRealization;
    }

    @Override
    public void run() {
        while(true){
            try {
                String word =(String)  stormRealization.getWordQueue().take();
                stormRealization.wordCount(word);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}