"""Анализ EdNet KT3 в PostgreSQL."""
import argparse
import json
import os
from pathlib import Path
import tempfile

import pandas as pd
import psycopg
from psycopg import sql
import pyarrow.csv as arrow_csv
import pyarrow.parquet as parquet

R = Path(__file__).resolve().parents[1]
p = argparse.ArgumentParser()
p.add_argument('--replace', action='store_true', help='Пересоздать схему ednet_study')
p.add_argument('--output', type=Path, default=R / 'results')
a = p.parse_args()
dsn = os.environ.get('POSTGRES_DSN')
if not dsn:
    raise SystemExit('Укажите строку подключения в POSTGRES_DSN')
DATA = R / 'data'
WORK = R / 'work'
a.output.mkdir(parents=True, exist_ok=True)
WORK.mkdir(exist_ok=True)
with psycopg.connect(dsn) as c:
    exists = c.execute("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname='ednet_study')").fetchone()[0]
    if exists and not a.replace:
        raise SystemExit('Схема ednet_study уже существует. Для пересоздания добавьте --replace.')
    if exists:
        c.execute('DROP SCHEMA ednet_study CASCADE')
    c.execute('CREATE SCHEMA ednet_study')
    c.execute('SET search_path TO ednet_study')
    c.execute('''CREATE TABLE events(user_id text NOT NULL, source_row integer NOT NULL, timestamp bigint NOT NULL,
      action_type text, item_id text, source text, user_answer text, platform text)''')
    c.execute('''CREATE TABLE questions(question_id text, bundle_id text, explanation_id text,
      correct_answer text, part text, tags text, deployed_at text)''')
    print('Загружаем события...', flush=True)
    source = parquet.ParquetFile(DATA / 'events.parquet')
    with tempfile.TemporaryDirectory(prefix='ednet_postgres_') as tmp:
        csvfile = Path(tmp) / 'events.csv'
        with arrow_csv.CSVWriter(csvfile, source.schema_arrow) as w:
            for batch in source.iter_batches(batch_size=100_000):
                w.write_batch(batch)
        for table, path in [('events', csvfile), ('questions', DATA / 'questions.csv')]:
            with c.cursor().copy(sql.SQL('COPY {} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)').format(sql.Identifier(table))) as copy:
                with path.open('rb') as f:
                    while chunk := f.read(1024 * 1024):
                        copy.write(chunk)
    c.execute('CREATE INDEX events_user_row_idx ON events(user_id,source_row)')
    c.execute('CREATE INDEX events_user_time_idx ON events(user_id,timestamp,source_row)')
    c.execute('CREATE INDEX questions_id_idx ON questions(question_id)')
    c.execute('ANALYZE events'); c.execute('ANALYZE questions')
    print('Восстанавливаем попытки и ответы...', flush=True)
    c.execute((R / 'sql/01_prepare.sql').read_text())
    raw = c.execute('SELECT count(*),count(DISTINCT user_id),max(timestamp) FROM events').fetchone()
    endpoint = raw[2]
    rev = c.execute('''WITH t AS (SELECT user_id,timestamp,lag(timestamp) OVER(PARTITION BY user_id ORDER BY source_row) prev FROM events)
      SELECT count(*) FILTER(WHERE timestamp<prev),count(*) FILTER(WHERE timestamp=prev) FROM t''').fetchone()
    if rev[0]: raise ValueError('Нарушен исходный порядок событий')
    quality = dict(zip(('total','unknown','wrong','correct'), c.execute('''SELECT count(*),count(*) FILTER(WHERE outcome='unknown'),
      count(*) FILTER(WHERE outcome='wrong'),count(*) FILTER(WHERE outcome='correct') FROM attempt_status''').fetchone()))
    rows=[]; flow=[]
    for gap in (15,30,60):
        print(f'Считаем сеансы: {gap} минут...', flush=True)
        c.execute('DROP TABLE IF EXISTS pg_temp.metrics, pg_temp.focus, pg_temp.sessions, pg_temp.session_events')
        c.execute('''CREATE TEMP TABLE session_events AS WITH lagged AS (
          SELECT user_id,source_row,timestamp,timestamp-lag(timestamp) OVER(PARTITION BY user_id ORDER BY timestamp,source_row) gap FROM events)
          SELECT *,sum(CASE WHEN gap IS NULL OR gap>%s*60000 THEN 1 ELSE 0 END)
          OVER(PARTITION BY user_id ORDER BY timestamp,source_row ROWS UNBOUNDED PRECEDING) session_no FROM lagged''', (gap,))
        c.execute('CREATE INDEX ON session_events(user_id,source_row)')
        c.execute('CREATE INDEX ON session_events(user_id,session_no,timestamp)')
        c.execute('ANALYZE session_events')
        c.execute('''CREATE TEMP TABLE sessions AS SELECT user_id,session_no,min(timestamp) start_ms,max(timestamp) end_ms
          FROM session_events GROUP BY user_id,session_no''')
        c.execute('CREATE INDEX ON sessions(user_id,session_no)'); c.execute('ANALYZE sessions')
        c.execute('''CREATE TEMP TABLE focus AS WITH errors AS (
          SELECT a.*,row_number() OVER(PARTITION BY a.user_id ORDER BY a.submit_time,a.submit_row) ranking
          FROM attempt_status a JOIN session_events b ON a.user_id=b.user_id AND a.enter_row=b.source_row
          JOIN session_events d ON a.user_id=d.user_id AND a.submit_row=d.source_row
          WHERE b.session_no=1 AND d.session_no=1 AND a.outcome='wrong'),
          first_error AS (SELECT * FROM errors WHERE ranking=1 AND source='diagnosis')
          SELECT f.user_id,f.enter_row,f.submit_row,f.submit_time,s.end_ms,
          EXISTS (SELECT 1 FROM events nx JOIN session_events se ON nx.user_id=se.user_id AND nx.source_row=se.source_row
          WHERE nx.user_id=f.user_id AND nx.source_row>f.submit_row AND nx.action_type='enter' AND nx.item_id LIKE 'b%' AND se.session_no=1) continued
          FROM first_error f JOIN sessions s ON f.user_id=s.user_id AND s.session_no=1''')
        c.execute('CREATE INDEX ON focus(user_id)'); c.execute('ANALYZE focus')
        c.execute('''CREATE TEMP TABLE metrics AS SELECT f.user_id,f.end_ms,f.continued,
          count(*) FILTER(WHERE s.session_no>1 AND s.start_ms>=f.end_ms+86400000 AND s.start_ms<f.end_ms+2*86400000)>0 d1,
          count(*) FILTER(WHERE s.session_no>1 AND s.start_ms>=f.end_ms+7*86400000 AND s.start_ms<f.end_ms+8*86400000)>0 d7
          FROM focus f JOIN sessions s USING(user_id) GROUP BY f.user_id,f.end_ms,f.continued''')
        flow.append({'session_gap_min':gap,'users_first_diagnosis_error':c.execute('SELECT count(*) FROM focus').fetchone()[0]})
        for offset in (0,30,60):
            for day in (1,7):
                q=sql.SQL('SELECT continued,count(*),count(*) FILTER(WHERE {}) FROM metrics WHERE end_ms+%s*86400000<=%s GROUP BY continued ORDER BY continued').format(sql.Identifier(f'd{day}'))
                for cont,n,ret in c.execute(q,(day+1,endpoint-offset*86400000)).fetchall():
                    rows.append({'session_gap_min':gap,'boundary_offset_days':offset,'metric':f'D{day}',
                      'continued':bool(cont),'users':n,'returned':ret,'percent':100*ret/n})
        if gap==30:
            cur=c.execute('SELECT user_id,end_ms,continued,d1,d7 FROM metrics')
            pd.DataFrame(cur.fetchall(),columns=[col.name for col in cur.description]).to_parquet(WORK/'user_metrics.parquet',index=False)
    pd.DataFrame(rows).to_csv(a.output/'retention.csv',index=False)
    manifest=json.loads((DATA/'sampling.json').read_text())
    summary={'sampling':manifest,'raw_events':raw[0],'unique_users':raw[1],'last_event_ms_shifted':endpoint,
      'source_time_reversals':rev[0],'same_timestamp_neighbors':rev[1],'attempts':quality,'sample_flow':flow,'retention':rows,
      'caveat':'Общая граница и одинаковость сдвига времени не подтверждены; цифры условные. Проект основан на публичном исследовании, не на эксперименте.'}
    (a.output/'results.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf8')
    print(pd.DataFrame(rows).query('session_gap_min == 30 and boundary_offset_days == 0').to_string(index=False),flush=True)
