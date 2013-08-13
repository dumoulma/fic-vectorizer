REGISTER pig/lib/caissepop-1.2.jar;
REGISTER pig/lib/commons-math3-3.2.jar;
REGISTER pig/lib/lucene-*.jar;

--%default MIN_COUNT 2
--%default MAX_VOCAB_SIZE 5000
--%default MIN_BNS_SCORE 0.00001

DEFINE TokenizeText com.fujitsu.ca.fic.caissepop.evaluation.TokenizeText();
DEFINE BNS com.fujitsu.ca.fic.caissepop.evaluation.ComputeBns();

-- Load positive and negative documents, tokenize and tag with doc_id (the document name) 
-- and label (1 for positive, 0 for negative)
positive_docs    = LOAD 'data/test/sieve/pos' USING PigStorage('\n','-tagsource') 
                     AS (doc_id:chararray, text:chararray);

pos_tokens = FOREACH positive_docs 
                GENERATE doc_id, 
                         1 AS label:long,
                         FLATTEN(TokenizeText(text)) AS token:chararray;
    
pos_tokens = FILTER pos_tokens BY token MATCHES '\\w.*';
pos_tokens = FILTER pos_tokens BY SIZE(token) > 1L;

negative_docs = LOAD 'data/test/sieve/neg' USING PigStorage('\n','-tagsource') 
                     AS (doc_id:chararray, text:chararray);
neg_tokens = FOREACH negative_docs 
                GENERATE doc_id, 
                         0 AS label:long,
                         FLATTEN(TokenizeText(text)) AS token:chararray;
neg_tokens = FILTER neg_tokens BY token MATCHES '\\w.*';
neg_tokens = FILTER neg_tokens BY SIZE(token) > 1L;

-- The vocabulary of the corpus is the union of tokens found in the positive documents
-- and the ones in the negative documents.
vocabUnion = UNION pos_tokens, neg_tokens;

-- count the number of positive and negative documents
posDocs = FOREACH pos_tokens GENERATE doc_id;
posDocs = DISTINCT posDocs;
posDocs = FOREACH (GROUP posDocs ALL) 
            GENERATE COUNT(posDocs) AS n_docs;

negDocs = FOREACH neg_tokens GENERATE doc_id;
negDocs = DISTINCT negDocs;
negDocs = FOREACH (GROUP negDocs ALL) 
            GENERATE COUNT(negDocs) AS n_docs;
    
 -- count the true positive and false positive counts for each vocabulary token
posNegGrouped = COGROUP pos_tokens BY token, neg_tokens BY token;
 
-- each token will have a bag of tokens and documents associated
-- for each token, get the count of positive documents and negative documents where the token is used
-- combined with the count of positive and negative documents, we can compute the bns score of the token
-- its easy to get the total count of the token as well, so we compute it here too.
bnsPipe = FOREACH posNegGrouped {
		    pos_tokens = pos_tokens.token;
		    neg_tokens = neg_tokens.token;
		    tp = COUNT(pos_tokens);
		    fp = COUNT(neg_tokens); 
		    all_count = (int)(tp + fp);
		    GENERATE group AS token, 
		             BNS(tp, posDocs.n_docs, fp, negDocs.n_docs) AS bns_score, 
		             all_count AS all_count:int;
		  }

-- Filters, by token frequency and BNS value and vocabulary size		  
bnsPipe = FILTER bnsPipe BY (all_count > 1) OR (bns_score > 0.1);
bnsPipe = ORDER bnsPipe BY bns_score DESC;
bnsPipe = LIMIT bnsPipe 5000;

-- Here we want to group on the doc_id for the last vectorization step
outPipeJoined = JOIN vocabUnion BY token, bnsPipe BY token;
outPipe = FOREACH outPipeJoined 
            GENERATE vocabUnion::doc_id as doc_id, 
                     vocabUnion::label as label, 
                     vocabUnion::token as token, 
                     bnsPipe::bns_score as bns_score;
outPipeGrouped = GROUP outPipe BY (doc_id,label);
outPipeRandom = foreach outPipeGrouped generate *, RANDOM() as random;
outPipeRandom = order outPipeRandom by random;

SPLIT outPipeRandom INTO train IF random < 0.6, train OTHERWISE;

