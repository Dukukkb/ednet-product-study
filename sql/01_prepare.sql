-- Таблицы events и questions загружаются в PostgreSQL из подготовленных файлов.
-- Сохраняем source_row для однозначного порядка при равных отметках времени.
CREATE TABLE bundle_counts AS SELECT bundle_id, count(*) AS expected_questions FROM questions GROUP BY bundle_id;

-- Только соседние события блока enter -> submit с совпадающими идентификаторами.
CREATE TABLE attempts AS
WITH seq AS (
 SELECT *, LEAD(action_type) OVER w next_action, LEAD(item_id) OVER w next_item,
 LEAD(source_row) OVER w submit_row, LEAD(timestamp) OVER w submit_time
 FROM events WHERE item_id LIKE 'b%' AND action_type IN ('enter','submit')
 WINDOW w AS (PARTITION BY user_id ORDER BY timestamp, source_row)
)
SELECT user_id,source_row enter_row,timestamp enter_time,submit_row,submit_time,item_id bundle_id,source
FROM seq WHERE action_type='enter' AND next_action='submit' AND next_item=item_id AND submit_row>source_row;

-- Один финальный ответ на вопрос: последний выбор до отправки блока.
CREATE INDEX attempts_user_row_idx ON attempts(user_id,enter_row);
CREATE TABLE answer_last AS
SELECT user_id,enter_row,question_id,user_answer,correct_answer,answer_bundle FROM (
 SELECT a.user_id,a.enter_row,e.item_id AS question_id,e.user_answer,q.correct_answer,q.bundle_id AS answer_bundle,
 ROW_NUMBER() OVER(PARTITION BY a.user_id,a.enter_row,e.item_id ORDER BY e.timestamp DESC,e.source_row DESC) AS rn
 FROM attempts a JOIN events e ON a.user_id=e.user_id AND e.source_row>a.enter_row AND e.source_row<a.submit_row AND e.action_type='respond'
 LEFT JOIN questions q ON q.question_id=e.item_id
) ranked WHERE rn=1;
CREATE INDEX answer_last_attempt_idx ON answer_last(user_id,enter_row);

CREATE TABLE attempt_status AS
WITH stats AS (
 SELECT user_id,enter_row,count(*) answer_count,
 count(*) FILTER(WHERE user_answer IS NULL OR correct_answer IS NULL OR answer_bundle IS NULL OR answer_bundle<>a.bundle_id) invalid_count,
 count(*) FILTER(WHERE user_answer<>correct_answer AND answer_bundle=a.bundle_id) wrong_count
 FROM answer_last x JOIN attempts a USING(user_id,enter_row) GROUP BY 1,2
)
SELECT a.*, b.expected_questions,coalesce(s.answer_count,0) answer_count,
CASE WHEN b.expected_questions IS NULL OR s.answer_count IS NULL OR s.invalid_count>0 OR s.answer_count<>b.expected_questions THEN 'unknown'
 WHEN s.wrong_count>0 THEN 'wrong' ELSE 'correct' END AS outcome
FROM attempts a LEFT JOIN stats s USING(user_id,enter_row) LEFT JOIN bundle_counts b USING(bundle_id);
