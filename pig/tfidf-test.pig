REGISTER target/caissepop-1.2.jar;
REGISTER pig/lib/lucene-core-4.4.0.jar
REGISTER pig/lib/lucene-analyzers-3.6.2.jar;

define TokenizeText com.fujitsu.ca.fic.caissepop.evaluation.TokenizeText();
define TFIDF com.fujitsu.ca.fic.caissepop.evaluation.ComputeTfIdf();

documents    = LOAD '$INPUT' USING PigStorage('\n', '-tagsource') 
                AS (doc_id:chararray, text:chararray);

-- tokenize using lucene in a UDF. 
tokenPipe = FOREACH documents GENERATE doc_id, 
             FLATTEN(TokenizeText(text)) 
             AS (token:chararray);
tokenPipe = FILTER tokenPipe BY token MATCHES '\\w.*';

-- get the count of documents in the corpus
dPipe = FOREACH tokenPipe GENERATE doc_id;
dPipe = DISTINCT dPipe;
dGroups = GROUP dPipe ALL;
dPipe = FOREACH dGroups {
  GENERATE COUNT(dPipe) AS n_docs;
}

tfGroups = GROUP tokenPipe BY (doc_id, token);
tfPipe = FOREACH tfGroups 
            GENERATE FLATTEN(group) 
            AS (doc_id, tf_token), COUNT(tokenPipe) AS tf_count:long;

-- one branch tallies the token counts for document frequency (DF)
-- note that here, we calculate distinct inside the foreach, whereas
-- for global count, we used the top-level DISTINCT operator.
-- the difference is that one is slower (requires an MR job), but extremely
-- scalable; the other is done in memory, on a reducer, per-group.
-- since here we expect much smaller groups, we favor the method that will
-- be faster and not produce an extra MR job.
tokenGroups = GROUP tokenPipe BY token;
dfPipe = FOREACH tokenGroups {
  dfPipe = distinct tokenPipe.doc_id;
  GENERATE group AS df_token, COUNT(dfPipe) AS df_count;
}

tfidfPipe = JOIN tfPipe BY tf_token, dfPipe BY df_token;

-- Note how we refer to dPipe.n_docs , even though it's a relation we didn't join in!
-- That's a special case for single-tuple relations that allows one to simply treat them as
-- constants. 
tfidfPipe = FOREACH tfidfPipe GENERATE doc_id, tf_token AS token, 
  TFIDF(tf_count, dPipe.n_docs, df_count) as tfidf:double, df_count, tf_count;

out = GROUP tfidfPipe BY doc_id; 
