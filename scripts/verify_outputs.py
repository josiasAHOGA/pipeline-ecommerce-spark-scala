"""Contrôle indépendant des sorties Scala. Dépendances : pandas et pyarrow."""
from pathlib import Path
import json
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
DATA=ROOT/'data'
OUT=ROOT/'output'
def read_report(name):
    return pd.concat([pd.read_csv(p) for p in (OUT/'csv'/name).glob('part-*.csv')],ignore_index=True)

tx=pd.read_csv(DATA/'transactions.csv',dtype={'timestamp':'string'},keep_default_na=False,na_values=[''])
u=pd.read_json(DATA/'users.json',lines=True)
p=pd.read_parquet(DATA/'products.parquet')
m=pd.read_csv(DATA/'merchants.csv')
parsed=pd.to_datetime(tx.timestamp,format='%Y%m%d%H%M%S',errors='coerce')
mask_tx=(tx.amount>0)&(tx.timestamp.str.len()==14)&parsed.notna()
masks={'transactions':mask_tx,'users':u.age.between(16,100)&u.annual_income.gt(0),
       'products':p.price.gt(0)&p.rating.between(1,5),'merchants':m.commission_rate.between(0,1)}
raw={'transactions':tx,'users':u,'products':p,'merchants':m}
quality=read_report('quality_report').set_index('dataset')
for name,df in raw.items():
    assert int(quality.loc[name,'nb_lignes_lues'])==len(df),name
    assert int(quality.loc[name,'nb_lignes_valides'])==int(masks[name].sum()),name
    assert int(quality.loc[name,'nb_lignes_rejetees'])==int((~masks[name]).sum()),name
    assert int(quality.loc[name,'nb_valeurs_nulles'])==int(df.isna().sum().sum()),name
    assert len(read_report('rejected_'+name))==int((~masks[name]).sum()),name
print('PASS : qualité et rejets recalculés indépendamment')

kept=tx[mask_tx].copy()
for name,key in [('users','user_id'),('products','product_id'),('merchants','merchant_id')]:
    valid_keys=set(raw[name].loc[masks[name],key])
    kept=kept[kept[key].isin(valid_keys)]
summary=read_report('summary').iloc[0]
assert len(kept)==int(summary.transactions)
assert len(kept)+len(read_report('join_rejections'))==int(mask_tx.sum())
assert round(float(kept.amount.sum()),2)==round(float(summary.revenue),2)
assert kept.user_id.nunique()==int(summary.customers)
assert kept.merchant_id.nunique()==int(summary.merchants)
for report in ['merchant_kpis','merchant_age_sales','category_region','payments_day_period']:
    assert abs(read_report(report).revenue.sum()-float(summary.revenue))<0.01,report
print('PASS : conservation des lignes et rapprochement du chiffre d’affaires')

kept['month']=pd.to_datetime(kept.timestamp,format='%Y%m%d%H%M%S').dt.to_period('M')
cohort=kept.groupby('user_id').month.min()
kept['cohort']=kept.user_id.map(cohort)
sizes=cohort.value_counts()
activity=kept.groupby(['cohort','month']).user_id.nunique()
co=read_report('cohort_retention')
for r in co.itertuples():
    cohort_month=pd.Period(r.cohort_month,freq='M')
    month=pd.Period(r.transaction_month,freq='M')
    assert r.cohort_size==sizes[cohort_month]
    assert r.active_users==activity.get((cohort_month,month),0)
    assert abs(r.retention_percent-round(r.active_users/r.cohort_size*100,2))<0.011
    assert month<=kept.month.max()
expected=sum(kept.month.max().ordinal-c.ordinal+1 for c in sizes.index)
assert len(co)==expected
print('PASS : toutes les cellules de rétention et les mois observables')

report_names=sorted(x.name for x in (OUT/'csv').iterdir() if x.is_dir())
for name in report_names:
    csv=read_report(name)
    parquet=pd.read_parquet(OUT/'parquet'/name)
    assert len(csv)==len(parquet),name
    assert list(csv.columns)==list(parquet.columns),name
rfm=read_report('rfm_customers')
assert len(rfm)==kept.user_id.nunique() and rfm.user_id.is_unique
for score in ['r_score','f_score','m_score']: assert rfm[score].between(1,5).all()
assert rfm.rfm_segment.notna().all()
enriched=pd.read_parquet(OUT/'parquet/enriched_transactions')
assert enriched.transaction_id.is_unique
assert int(enriched.is_suspicious.sum())==int(summary.suspicious_transactions)
assert int(enriched.catalog_merchant_mismatch.sum())==int(summary.catalog_merchant_mismatches)
assert enriched.age_group.notna().all()
print('PASS : cohérence CSV/Parquet, RFM, identifiants et synthèse')

result={
    'quality':quality.reset_index().to_dict(orient='records'),
    'summary':summary.to_dict(),
    'best_cohort':read_report('best_cohort_m3').to_dict(orient='records'),
    'top_merchants':read_report('merchant_kpis').head(5).to_dict(orient='records'),
    'retention':co.to_dict(orient='records'),
    'rfm':rfm.rfm_segment.value_counts().to_dict(),
    'join_rejections':len(read_report('join_rejections')),
    'report_count':len(report_names),
    'independent_checks':'passed'
}
(ROOT/'verification').mkdir(exist_ok=True)
(ROOT/'verification/metrics.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
print('VÉRIFICATION INDÉPENDANTE RÉUSSIE')
