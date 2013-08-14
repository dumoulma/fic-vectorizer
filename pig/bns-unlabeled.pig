REGISTER pig/lib/caissepop-1.2.jar;
REGISTER pig/lib/commons-math3-3.2.jar;
REGISTER pig/lib/lucene-*.jar;

--%default MIN_COUNT 2
--%default MAX_VOCAB_SIZE 5000
--%default MIN_BNS_SCORE 0.00001

DEFINE TokenizeText com.fujitsu.ca.fic.caissepop.evaluation.TokenizeText();
DEFINE BNS com.fujitsu.ca.fic.caissepop.evaluation.ComputeBns();

-- Load unlabeled documents, tokenize and tag with doc_id (the document name) 
unlabeled_docs    = LOAD 'data/test/sieve/unlabeled' USING PigStorage('\n','-tagsource') 
                     AS (doc_id:chararray, text:chararray);

unlabeled_tokens = FOREACH unlabeled_docs 
                GENERATE doc_id, 
                         FLATTEN(TokenizeText(text)) AS token:chararray;
    
unlabeled_tokens = FILTER unlabeled_tokens BY token MATCHES '\\w.*';
unlabeled_tokens = FILTER unlabeled_tokens BY SIZE(token) > 1L;

-- Load the vocabulary with BNS values needs to have access to the .pig_schema file!
bns_scored_vocabulary = LOAD 'data/out/bns-corpus/spam-vs-rel/bns-vocab' USING PigStorage('\t','-schema');

outPipeJoined = JOIN bns_scored_vocabulary BY token, unlabeled_tokens BY token;
outPipe = FOREACH outPipeJoined 
            GENERATE unlabeled_tokens::doc_id as doc_id, 
                     bns_scored_vocabulary::token as token, 
                     bns_scored_vocabulary::bns_score as bns_score;
outPipeGrouped = GROUP outPipe BY doc_id;

rmf $OUTPUT_DIR/unlabeled
STORE outPipeGrouped INTO '$OUTPUT_DIR/unlabeled' USING PigStorage('¤','schema');