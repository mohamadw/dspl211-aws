package com.amazonaws.samples;

import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class namedEntityRecognitionHandler {

	private static StanfordCoreNLP NERPipeline;

	// Extract only the following entity types: PERSON, LOCATION, ORGANIZATION.
	public namedEntityRecognitionHandler() {
		Properties props = new Properties();
		props.put("annotators", "tokenize, ssplit, parse, sentiment,lemma,ner");
		NERPipeline = new StanfordCoreNLP(props);
	}
	// The following code extracts named entities from a review
	//
	// :

	public String[] printEntities(String review) {
		String s = "[", num = "";
		String ret[] = new String[2];
		ret[0] = "";
		ret[1] = "";
		// create an empty Annotation just with the given text
		Annotation document = new Annotation(review);
		// run all Annotators on this text
		NERPipeline.annotate(document);
		// these are all the sentences in this document
		// a CoreMap is essentially a Map that uses class objects as keys and has values
		// with custom types
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		for (CoreMap sentence : sentences) {
			// traversing the words in the current sentence
			// a CoreLabel is a CoreMap with additional token-specific methods
			for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
				// this is the text of the token
				String word = token.get(TextAnnotation.class);

				// this is the NER label of the token

				String ne = token.get(NamedEntityTagAnnotation.class);

				if (ne != null && (ne.equals("PERSON") || ne.equals("ORGANAIZATION") || ne.equals("LOCATION"))) {
					s = s + word + ":" + ne + ",";
				}
				if (ne != null && ne.equals("NUMBER")) {
					num = word;
				}
				if (word.equals("Stars") || word.equals("stars")) {
					switch (num) {
					case "Five":
						ret[0] = "5";
						break;
					case "five":
						ret[0] = "5";
						break;
					case "Four":
						ret[0] = "4";
						break;
					case "four":
						ret[0] = "4";
						break;
					case "Three":
						ret[0] = "3";
						break;
					case "three":
						ret[0] = "3";
						break;
					case "Two":
						ret[0] = "2";
						break;
					case "two":
						ret[0] = "2";
						break;
					case "One":
						ret[0] = "1";
						break;
					case "one":
						ret[0] = "1";
						break;

					}
				}
			}
		}
		ret[1]=s + "]";
		return ret;
	}
}