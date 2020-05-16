profes = LOAD '/output/' USING PigStorage('\t') as (id:chararray, actor:int, director:int);
names = LOAD '/input/name.tsv' USING PigStorage('\t') as (id:chararray, name:chararray, birth:int,dead:chararray,prof:chararray,title:chararray);
joined = JOIN profes BY id, names BY id;
shorted = FOREACH joined GENERATE name,actor,director,prof;  
directors = FILTER shorted BY (prof  matches '.*director.*'); 
actors = FILTER shorted BY (prof matches '.*(actor|actress).*');
actors_res = FOREACH actors GENERATE name as nazwa,'actor' as zawod ,actor as filmy;
directors_res = FOREACH directors GENERATE name as nazwa,'director' as zawod,director as filmy;
actor_order = ORDER actors_res  by filmy desc;
actor_limit = LIMIT actor_order 3;
directors_order = ORDER directors_res by filmy desc;
directors_limit = LIMIT directors_order 3;
result = UNION actor_limit, directors_limit;
store result into '/pig_out' using JsonStorage();