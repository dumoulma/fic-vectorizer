REGISTER $LIB_DIR/caissepop-1.2.jar;
REGISTER $LIB_DIR/commons-math3-3.2.jar;
REGISTER $LIB_DIR/lucene-*.jar;

--%default MIN_COUNT 2
--%default MAX_VOCAB_SIZE 5000
--%default MIN_BNS_SCORE 0.00001

DEFINE TokenizeText com.fujitsu.ca.fic.caissepop.evaluation.TokenizeText();
DEFINE BNS com.fujitsu.ca.fic.caissepop.evaluation.ComputeBns();

-- Load positive and negative documents, tokenize and tag with doc_id (the document name) 
-- and label (1 for positive, 0 for negative)
positive_docs    = LOAD '$POS_INPUT_DIR' USING PigStorage('\n','-tagsource') 
                     AS (doc_id:chararray, text:chararray);

pos_tokens = FOREACH positive_docs 
                GENERATE doc_id, 
                         1 AS label:long,
                         FLATTEN(TokenizeText(text)) AS token:chararray;
    
pos_tokens = FILTER pos_tokens BY token MATCHES '\\w.*';
pos_tokens = FILTER pos_tokens BY SIZE(token) > 1L;

negative_docs = LOAD '$NEG_INPUT_DIR' USING PigStorage('\n','-tagsource') 
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
            all_count = (tp + fp);
            GENERATE BNS(tp, posDocs.n_docs, fp, negDocs.n_docs) AS bns_score,
                     group AS token, 
                     all_count AS all_count:long;
          }

-- Filters, by token frequency and BNS value and vocabulary size          
bnsPipe = FILTER bnsPipe BY (all_count > $MIN_COUNT) OR (bns_score > $MIN_BNS_SCORE);
bnsPipe = ORDER bnsPipe BY bns_score DESC;
bnsPipe = LIMIT bnsPipe $MAX_VOCAB_SIZE;

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
SPLIT outPipeRandom INTO train IF random < 0.6, test OTHERWISE;

rmf $OUTPUT_DIR/bns-vocab
rmf $OUTPUT_DIR/bns-corpus/test
rmf $OUTPUT_DIR/bns-corpus/train

STORE bnsPipe INTO '$OUTPUT_DIR/bns-vocab' USING PigStorage(',','schema');
STORE test INTO '$OUTPUT_DIR/test' USING PigStorage(',','schema');
STORE train INTO '$OUTPUT_DIR/train' USING PigStorage(',','schema');