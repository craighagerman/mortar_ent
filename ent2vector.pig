-- load_metadata

-- USAGE:
--		mortar local:run pigscripts/load_metadata.pig


-- Set input and output path as pig parameters
-- %default INPUT_PATH '/Users/craig/Wattpad/Code_wattpad/similarity/entity_similarity/data_small.csv'
-- %default ENT_INPUT_PATH '../data/entities'
%default ENT_INPUT_PATH  '../data/data_tiny.csv'

--%default DICT_INPUT_PATH '../data/dict/gdict.dict'
%default DICT_INPUT_PATH '../data/dict/gdict_tiny.dict'
%default VECT_OUTPUT_PATH '../data/out/'




-- REGISTER '../udfs/python/extract_ent.py' USING streaming_python as ent_ext;






-- load training data ------------------------------------------------
Entity  = LOAD '$ENT_INPUT_PATH' USING PigStorage(',') AS ( groupid:int, text:chararray );
DESCRIBE Entity;
DUMP Entity;

Ent = FOREACH Entity GENERATE text;


-- load dictionary file ---------------------------------------------
-- DICT = LOAD '$DICT_INPUT_PATH' USING org.apache.pig.piggybank.storage.CSVExcelStorage('\t') 
D = LOAD '$DICT_INPUT_PATH' USING PigStorage('\t') AS ( id:int, dictword:chararray, freq:int );

--DESCRIBE Dict;
--DUMP Dict;
-- get rid of freq column in D ------------------------------
Dict = FOREACH D GENERATE id, dictword;
--DUMP Dict;




-- ------------------------------------------------
-- TweetsRaw = LOAD '...' USING JsonLoader(...);
-- Tweets = FOREACH ... GENERATE TweetID, Text;
-- TokenizedTweets = FOREACH Tweets GENERATE TweetID, Text, FLATTEN(TOKENIZE(Text)) as word;
-- Dictionary = LOAD '...' as (DictWord: chararray, polarity: int);
-- Labeled_Words = JOIN TokenizedTweets BY Word, Dictionary BY DictWord;
-- GroupedSentiment = GROUP Labeled_Words BY TwitterID, Text;
-- Result = FOREACH GroupedSentiment GENERATE FLATTEN(group), SUM(Labeled_Words.polarity) AS rate;
-- DUMP Result;
-- ------------------------------------------------



--  Procedure ------
-- 1.  Do a FOREACH + Flatten to get a list of groupid -> word

-- T1 = FOREACH Entity GENERATE groupid, FLATTEN(STRSPLIT(text)) AS word;
-- DUMP T1;
Token = FOREACH Entity GENERATE groupid, FLATTEN(TOKENIZE(text, ' ')) AS word;
DUMP Token;


-- 2. Do a join with the dictionary on groupid


Token_Dict = JOIN Token BY word, Dict BY dictword;
-- DUMP token_id;

-- xyz = FOREACH token_id GENERATE groupid, id;
tid = FOREACH Token_Dict GENERATE groupid, dictword;
DUMP tid;



-- 3. Do a GROUP BY on groupid + token_id
X = GROUP tid By groupid;
DUMP X;


-- 	4.  Do a word count on unique words in above groupings
UniqeCount = FOREACH X { uniq_sym = DISTINCT dictword;
						GENERATE COUNT(uniq_sym); };
DUMP UniqeCount;














