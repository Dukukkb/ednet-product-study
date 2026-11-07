"""Создать воспроизводимую пользовательскую выборку исходных файлов EdNet KT3."""
import argparse, csv, random, hashlib, json
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

ROOT=Path(__file__).resolve().parents[1]
p=argparse.ArgumentParser()
p.add_argument('--kt3',required=True,type=Path,help='Папка исходных файлов u*.csv')
p.add_argument('--questions',required=True,type=Path,help='Исходный questions.csv')
p.add_argument('--sample-size',type=int,default=20000)
p.add_argument('--seed',type=int,default=20261103)
a=p.parse_args()
files=sorted(a.kt3.glob('u*.csv'))
if len(files)<a.sample_size: raise SystemExit(f'Найдено {len(files)} файлов, необходимо {a.sample_size}')
selected=sorted(random.Random(a.seed).sample(files,a.sample_size))
meta=ROOT/'data';meta.mkdir(exist_ok=True)
schema=pa.schema([('user_id',pa.string()),('source_row',pa.int32()),('timestamp',pa.int64()),('action_type',pa.string()),('item_id',pa.string()),('source',pa.string()),('user_answer',pa.string()),('platform',pa.string())])
cols={x:[] for x in schema.names};total=0; expected=['timestamp','action_type','item_id','source','user_answer','platform']
writer=pq.ParquetWriter(meta/'events.parquet',schema,compression='zstd')
def flush():
 if cols['user_id']:
  writer.write_table(pa.Table.from_pydict(cols,schema=schema)); [v.clear() for v in cols.values()]
try:
 for k,path in enumerate(selected,1):
  with path.open(newline='',encoding='utf-8-sig') as f:
   reader=csv.DictReader(f)
   if reader.fieldnames!=expected: raise ValueError(f'Неверные поля: {path.name} {reader.fieldnames}')
   for i,r in enumerate(reader,1):
    cols['user_id'].append(path.stem);cols['source_row'].append(i)
    cols['timestamp'].append(int(r['timestamp']))
    for name in expected[1:]:cols[name].append(r[name] or None)
    total+=1
    if len(cols['user_id'])>=150000:flush()
  if k%5000==0:print(f'Подготовлено пользователей: {k}',flush=True)
 flush()
finally:writer.close()
q=a.questions.read_bytes(); (meta/'questions.csv').write_bytes(q)
manifest={'seed':a.seed,'population_files':len(files),'sample_users':len(selected),'events':total,'sampling':'случайная выборка файлов пользователей без возвращения; упорядоченные имена, Python random.Random.sample','questions_sha256':hashlib.sha256(q).hexdigest()}
(meta/'sampling.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf8')
(meta/'selected_users.txt').write_text('\n'.join(p.stem for p in selected)+'\n',encoding='utf8')
print(json.dumps(manifest,ensure_ascii=False,indent=2),flush=True)
