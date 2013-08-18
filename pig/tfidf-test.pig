REGISTER pig/lib/caissepop-*.jar;
REGISTER pig/lib/lucene-core-*.jar;
REGISTER pig/lib/lucene-analyzers-common-*.jar;
REGISTER pig/lib/datafu-*.jar

define TokenizeText com.fujitsu.ca.fic.caissepop.evaluation.TokenizeText();
define TFIDF com.fujitsu.ca.fic.caissepop.evaluation.ComputeTfIdf();
DEFINE ENUMERATE datafu.pig.bags.Enumerate();

relevant_docs = LOAD 'data/test/sieve/pos' USING PigStorage('\n', '-tagsource') AS (doc_id:chararray, text:chararray);
rel_tokens = FOREACH relevant_docs 
                GENERATE CONCAT('data/test/sieve/pos', doc_id) as doc_id:chararray, 
                         0 AS label:int,
                         FLATTEN(TokenizeText(text)) AS token:chararray;                        
rel_tokens = FILTER rel_tokens BY token MATCHES '[a-zA-Z_]+'; -- AND SIZE(token) > 1L;

not_relevant_docs = LOAD 'data/test/sieve/neg' USING PigStorage('\n','-tagsource') AS (doc_id:chararray, text:chararray);
not_tokens = FOREACH not_relevant_docs 
                GENERATE CONCAT('data/test/sieve/neg', doc_id) as doc_id:chararray, 
                         1 AS label:int,
                         FLATTEN(TokenizeText(text)) AS token:chararray;
not_tokens = FILTER not_tokens BY token MATCHES '[a-zA-Z_]+';

spam_docs = LOAD 'data/test/sieve/spam' USING PigStorage('\n','-tagsource') AS (doc_id:chararray, text:chararray);
spam_tokens = FOREACH spam_docs 
                GENERATE CONCAT('data/test/sieve/spam', doc_id) as doc_id:chararray, 
                         2 AS label:int,
                         FLATTEN(TokenizeText(text)) AS token:chararray;
spam_tokens = FILTER spam_tokens BY token MATCHES '[a-zA-Z_]+';


unlabeled_docs = LOAD 'data/test/sieve/unlabeled' USING PigStorage('\n','-tagsource') AS (doc_id:chararray, text:chararray);
unlabeled_tokens = FOREACH unlabeled_docs 
                GENERATE CONCAT('data/test/sieve/unlabeled', doc_id) as doc_id:chararray, 
                         3 AS label:int,
                         FLATTEN(TokenizeText(text)) AS token:chararray;
unlabeled_tokens = FILTER unlabeled_tokens BY token MATCHES '[a-zA-Z_]+';

vocab_union = UNION rel_tokens, not_tokens, spam_tokens, unlabeled_tokens;

all_vocab = FOREACH (GROUP vocab_union ALL) {
    all_tokens = DISTINCT vocab_union.token;
    GENERATE all_tokens;
}
vocab_size = FOREACH all_vocab GENERATE COUNT(all_tokens) AS cardinality;
tokens_indexed = FOREACH all_vocab GENERATE FLATTEN(ENUMERATE(all_tokens)) as(token, index:long);
vocab_union_indexed = JOIN tokens_indexed by token, vocab_union by token;

-- get the count of documents in the corpus
dPipe = FOREACH vocab_union_indexed GENERATE doc_id;
dPipe = DISTINCT dPipe;
dGroups = GROUP dPipe ALL;
dPipe = FOREACH dGroups {
  GENERATE COUNT(dPipe) AS n_docs;
}

tfGroups = GROUP vocab_union_indexed BY (doc_id, tokens_indexed::token);
tfPipe = FOREACH tfGroups 
            GENERATE FLATTEN(group) AS (doc_id, tf_token), 
            COUNT(vocab_union_indexed) AS tf_count:long;


tokenGroups = GROUP vocab_union_indexed BY tokens_indexed::token;
dfPipe = FOREACH tokenGroups {
  dfPipe = distinct vocab_union_indexed.doc_id;
  GENERATE group AS df_token, COUNT(dfPipe) AS df_count;
}

tfidfPipe = JOIN tfPipe BY tf_token, dfPipe BY df_token;
tfidfPipe = FILTER tfidfPipe BY tf_count > 1 AND df_count > 1;

-- compute the tfidf score using UDF from caissepop
tfidfPipe = FOREACH tfidfPipe GENERATE doc_id, tf_token AS token, 
  TFIDF(tf_count, dPipe.n_docs, df_count) as value:double, df_count, tf_count;

-- join the tfidf score we just computed with the list of doc_id, tokens of the entire corpus  
vocab_scored = JOIN vocab_union_indexed BY (vocab_union::token, vocab_union::doc_id), tfidfPipe BY (token, doc_id);
vocab_scored = DISTINCT vocab_scored;

-- Keep only the fields we care about for the output
outPipe = FOREACH vocab_scored 
            GENERATE vocab_union_indexed::vocab_union::doc_id as doc_id, 
                     vocab_union_indexed::vocab_union::label as label, 
                     vocab_union_indexed::tokens_indexed::index as index,
                     tfidfPipe::value as value;

out_grouped = GROUP outPipe BY (doc_id,label);

-- I'm cleaning up the output so we only have a bag of tuples index,score left
out_cleaned = FOREACH out_grouped {                                    
        entries = FOREACH outPipe generate (int)index as index:int, value;                     
        GENERATE group, TOTUPLE(vocab_size.cardinality, entries) as val;
        }

-- shuffle the document vectors before saving them to disk
out_randomized = FOREACH out_cleaned GENERATE *, RANDOM() as random;
out_shuffled = ORDER out_randomized BY random;

out_unlabeled = FILTER out_shuffled BY group.label == 0;
out_spam_vs_rel = FILTER out_shuffled BY group.label == 0 OR group.label == 2;
out_rel_vs_notrel = FILTER out_shuffled BY group.label == 0 OR group.label == 1;

SPLIT out_spam_vs_rel INTO spam_vs_rel_train IF random < 0.6, spam_vs_rel_test OTHERWISE;
SPLIT out_rel_vs_notrel INTO rel_vs_notrel_train IF random < 0.6, rel_vs_notrel_test OTHERWISE;

dump spam_vs_rel_train;