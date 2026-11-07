"""Независимо перепроверить 40 выбранных последовательностей из оригинальных CSV."""
import argparse,csv,random
from pathlib import Path
import pandas as pd
R=Path(__file__).resolve().parents[1]
p=argparse.ArgumentParser();p.add_argument('--kt3',required=True,type=Path);args=p.parse_args()
d=pd.read_parquet(R/'work/user_metrics.parquet')
rng=random.Random(17); chosen=[]
for group in (True,False):
 subset=d[d['continued']==group]
 chosen+=rng.sample(subset.to_dict('records'),min(20,len(subset)))
questions={}; bundle_questions={}
with (R/'data/questions.csv').open(newline='',encoding='utf8') as f:
 for q in csv.DictReader(f):questions[q['question_id']]=q;bundle_questions.setdefault(q['bundle_id'],set()).add(q['question_id'])
DAY=86400000
for result in chosen:
 with (args.kt3/(result['user_id']+'.csv')).open(newline='',encoding='utf-8-sig') as f: events=list(csv.DictReader(f))
 for i,e in enumerate(events):e['_row']=i+1;e['timestamp']=int(e['timestamp'])
 events.sort(key=lambda e:(e['timestamp'],e['_row']))
 seq=1;last=None
 for e in events:
  if last is not None and e['timestamp']-last>30*60000:seq+=1
  e['_session']=seq;last=e['timestamp']
 bundles=[i for i,e in enumerate(events) if e['action_type'] in ('enter','submit') and e['item_id'].startswith('b')]
 attempts=[]
 for a,b in zip(bundles,bundles[1:]):
  enter,submit=events[a],events[b]
  if enter['action_type']!='enter' or submit['action_type']!='submit' or enter['item_id']!=submit['item_id'] or submit['_row']<=enter['_row']:continue
  responses={}
  for e in events[a+1:b]:
   if e['action_type']=='respond':responses[e['item_id']]=e['user_answer']
  expected=bundle_questions.get(enter['item_id'],set())
  if not expected or set(responses)!=expected:continue
  if any(not answer or answer!=questions[q]['correct_answer'] for q,answer in responses.items()):
   attempts.append((a,b,enter,submit))
 relevant=next((x for x in attempts if x[2]['_session']==1 and x[3]['_session']==1),None)
 assert relevant and relevant[2]['source']=='diagnosis',(result['user_id'],'anchor')
 anchor=relevant[3]['_row']
 continued=any(e['_session']==1 and e['_row']>anchor and e['action_type']=='enter' and e['item_id'].startswith('b') for e in events)
 end=max(e['timestamp'] for e in events if e['_session']==1)
 starts={}
 for e in events:starts.setdefault(e['_session'],e['timestamp'])
 d1=any(end+DAY<=t<end+2*DAY for sn,t in starts.items() if sn>1)
 d7=any(end+7*DAY<=t<end+8*DAY for sn,t in starts.items() if sn>1)
 for key,val in [('end_ms',end),('continued',continued),('d1',d1),('d7',d7)]:assert bool(result[key])==val if isinstance(val,bool) else result[key]==val,(result['user_id'],key)
print('Проверено пользователей:',len(chosen),'ошибок: 0')
