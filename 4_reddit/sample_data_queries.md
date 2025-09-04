-- Sample data from submissions table
SELECT 
  id,
  author,
  created_utc,
  title,
  selftext,
  subreddit_name_prefixed,
  num_comments,
  score
FROM `handy-implement-454013-q2.reddit.submissions`
LIMIT 10;

id	author	created_utc	title	selftext	subreddit_name_prefixed	num_comments	score
d5e8b	thirdeyeimages	2010-08-25 19:11:25.000000 UTC	A little light can illuminate your world		r/1000words	0	5
d52dl	[deleted]	2010-08-25 00:43:48.000000 UTC	jordan	[deleted]	r/100fm6	0	0
d543o	[deleted]	2010-08-25 03:33:12.000000 UTC	BuzzBot: Great BuzzCrew	[deleted]	r/2008lists	0	1
d54z8	[deleted]	2010-08-25 05:03:06.000000 UTC	My first Fffffuuuuu comic	[deleted]	r/4chan	0	1
d5545	[deleted]	2010-08-25 05:17:44.000000 UTC	Fffffuuuuu Girls destroy your hopes	[deleted]	r/4chan	0	1
d52pk	palyouth	2010-08-25 01:15:03.000000 UTC	مسلسل باب الحارة 5 الحلقة 15		r/7mooode	0	1
d5bi5	palyouth	2010-08-25 15:55:48.000000 UTC	مسلسل اميمة في دار الايتام الحلقة 15		r/7mooode	0	1
d5ago	[deleted]	2010-08-25 14:37:59.000000 UTC	9/11 Planners Confess On Network Television	[deleted]	r/911truth	0	0
d5cr6	[deleted]	2010-08-25 17:23:48.000000 UTC	"""A War Against the West"" - published on 9-11-2001 by Israeli Institute IASPS"	[deleted]	r/911truth	0	7
d5bfr	[deleted]	2010-08-25 15:50:34.000000 UTC	Redditor fist_of_justice interviewed on the Radio in Kansas City after her coupon clip AMA	[deleted]	r/ABFMB	0	1


-- Sample data from comments table
SELECT 
  id,
  author,
  created_utc,
  body,
  score,
  parent_id,
  link_id,
  subreddit_name_prefixed
FROM `handy-implement-454013-q2.reddit.comments`
LIMIT 10;
id	author	created_utc	body	score	parent_id	link_id	subreddit_name_prefixed
c2pzyt	qwe1234	2007-09-16 07:15:55.000000 UTC	python.	-8	t1_c2pygd	t3_2pk74	
c2pzz1	qwe1234	2007-09-16 07:16:39.000000 UTC	"call me back when rails can do its own data storage and caching.

until then, welcome to centralized mysql hell..."	-9	t1_c2pwn6	t3_2pk74	
c2q02r	qwe1234	2007-09-16 07:36:36.000000 UTC	"good c++ code is vastly more maintainable than good python or rails code. (when you use static typing properly it helps *a lot*.)

besides, are you even literate? i think i answered the question of schedule times in my comments above.
"	-7	t1_c2pwsi	t3_2pk74	
c2q03w	qwe1234	2007-09-16 07:42:38.000000 UTC	"i programmed large projects in python before you even knew that computers existed.

give it a rest. you're a noob and a dumbass and you shouldn't talk down to people vastly smarter and more experienced than you are.
"	-10	t1_c2pwr5	t3_2pk74	
c2q04c	qwe1234	2007-09-16 07:45:46.000000 UTC	"1000 times speedup in c++ vs. dynamic languages is a realistic  goal for heavily multithreaded apps. (c++ uses atomic operations extensively and doesn't have any of the 'global locks' that most dynamic languages are full of.)

of course, heavily multithreaded apps are really rare. the only example i can think of offhand would be some externally-facing network service. so this is a fairly specialized domain.
"	-7	t1_c2py8y	t3_2pk74	
c2q09p	qwe1234	2007-09-16 08:16:09.000000 UTC	did you read my comment?	-8	t1_c2q08a	t3_2pk74	
c2q0b6	qwe1234	2007-09-16 08:25:41.000000 UTC	"any other 'tools for the job' that you know?  :) 

(yes, it is a trick question.)

i'm kinda busy to be educating you right now, but if you think about why there aren't any other good off-the-shelf solutions, you will realise why programming isn't just about gluing  together some shit other people wrote.

"	-7	t1_c2q0a6	t3_2pk74	
c2q0a5	qwe1234	2007-09-16 08:21:23.000000 UTC	"lol, off-the-shelf parts from noname manufacturers in china.

they probably make toasters and microwave ovens on the same assembly line. :)"	-10	t1_c2px5h	t3_2prqg	
c2q21y	RaldisPuppet	2007-09-16 14:09:23.000000 UTC	The day I cry about something so minor is the day I jump off a bridge.	-7	t1_c2py1t	t3_2ptll	
c2pxlo	psyne	2007-09-16 00:39:46.000000 UTC	It's actually from a lesser spoken language called Keysmash.	18	t1_c2pvwx	t3_2ptnq	
