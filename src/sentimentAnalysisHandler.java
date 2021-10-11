package com.amazonaws.samples;

import java.util.Properties;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;


public class sentimentAnalysisHandler {
	private static StanfordCoreNLP sentimentPipeline; 

  //Initialization
  public sentimentAnalysisHandler() {
    Properties props = new Properties();
    props.put("annotators", "tokenize, ssplit, parse, sentiment");
     sentimentPipeline = new StanfordCoreNLP(props);
     
  
  }

  //The following code performs sentiment analysis on a review:
  public  int findSentiment(String review) {
    int mainSentiment = 0;
    if (review != null && review.length() > 0) {
      int longest = 0;
      Annotation annotation = sentimentPipeline.process(review);
      for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
        Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
        int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
        String partText = sentence.toString();
        if (partText.length() > longest) {
          mainSentiment = sentiment;
          longest = partText.length();
        }
      }
    }
    return mainSentiment;
  }
}