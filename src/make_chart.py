from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
R=Path(__file__).resolve().parents[1]
df=pd.read_csv(R/'results/retention.csv')
main=df[(df.session_gap_min==30)&(df.boundary_offset_days==0)]
fig,axs=plt.subplots(1,2,figsize=(12,5),sharey=False)
for ax,metric in zip(axs,['D1','D7']):
 d=main[main.metric==metric].sort_values('continued',ascending=False)
 y=np.arange(2);bars=ax.barh(y,d['percent'],color=['#157c85','#a8b8c0'],height=.52)
 ax.set_yticks(y,['Начали следующий блок','Не начали следующий блок']);ax.invert_yaxis()
 xmax=max(11,float(d['percent'].max())*1.45);ax.set_xlim(0,xmax)
 for i,row in enumerate(d.itertuples()):ax.text(row.percent+.15,i,f'{row.percent:.2f}%  ({row.returned:,} из {row.users:,})'.replace(',',' '),va='center',fontsize=10)
 ax.set_title(metric+' · доля возвратов',loc='left',fontsize=13,fontweight='bold');ax.set_xlabel('Пользователи, %');ax.spines[['top','right','left']].set_visible(False);ax.tick_params(axis='y',length=0);ax.grid(axis='x',alpha=.2);ax.set_axisbelow(True)
fig.suptitle('Продолжение занятий после ошибки',x=.02,ha='left',fontsize=17,fontweight='bold')
fig.text(.02,.02,'Оценка условна: начало нового сеанса после первого наблюдаемого сеанса; данные EdNet-KT3.',fontsize=9,color='#465965')
fig.text(.02,-.02,'D7: в группе не продолживших всего 2 возврата. Разница групп не доказывает причинность.',fontsize=9,color='#465965')
fig.tight_layout(rect=(0,.07,1,.91),w_pad=3)
fig.savefig(R/'results/retention_chart.png',dpi=180,bbox_inches='tight',facecolor='white')
print('results/retention_chart.png')
