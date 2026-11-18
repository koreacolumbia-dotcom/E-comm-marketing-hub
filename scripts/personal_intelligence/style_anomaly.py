"""Pure offline style-name anomaly ranking for complete daily SKU extracts."""
from collections import defaultdict
from datetime import date, timedelta
from statistics import median
from math import isfinite

def rank_styles(rows, end_date, limit=3):
    """Rows: date, item_id, style_name, pdp, qty; missing observations remain unknown.
    Caller must supply complete style/day coverage, not top-N exports.
    Prior-year alignment uses 364 days (same weekday).
    """
    end = date.fromisoformat(end_date)
    days = [end-timedelta(days=i) for i in range(13,-1,-1)]
    totals = defaultdict(lambda: defaultdict(dict))
    seen = set()
    for row in rows:
        day = date.fromisoformat(row['date'])
        name = ' '.join(row['style_name'].split()).casefold()
        if not name or not row.get('item_id'):
            raise ValueError('Style and SKU required')
        key = (day,row['item_id'])
        if key in seen:
            raise ValueError('Duplicate daily SKU')
        seen.add(key)
        for metric in ('pdp','qty'):
            value = row.get(metric)
            if value is not None and (not isfinite(value) or value < 0):
                raise ValueError('Invalid metric')
            bucket = totals[name][day]
            old = bucket.get(metric,0)
            bucket[metric] = None if value is None or old is None else old+value
    groups = defaultdict(list)
    for name, by_day in totals.items():
        series = {}
        for metric, floor in [('pdp',50),('qty',3)]:
            cy = [by_day.get(d,{}).get(metric) for d in days]
            ly = [by_day.get(d-timedelta(days=364),{}).get(metric) for d in days]
            series[metric] = {'current':cy,'prior':ly}
            base = cy[-8:-1]
            if cy[-1] is not None and all(v is not None for v in base):
                b = median(base)
                delta = cy[-1]-b
                if abs(delta)>=floor and (b==0 or abs(delta)/b>=.3):
                    groups[metric+('_up' if delta>0 else '_down')].append((abs(delta),name))
            if metric=='qty' and all(v is not None for v in cy[-7:]+ly[-7:]):
                a,b=sum(cy[-7:]),sum(ly[-7:])
                if b>=5 and b-a>=3 and a/b<=.7:
                    groups['qty_yoy_down'].append((b-a,name))
        by_day['series'] = series
    selected={}
    for label, candidates in groups.items():
        for _,name in sorted(candidates,key=lambda x:(-x[0],x[1]))[:limit]:
            selected.setdefault(name,[]).append(label)
    return {'dates':[d.isoformat() for d in days],
            'prior_dates':[(d-timedelta(days=364)).isoformat() for d in days],
            'cards':[{'style_key':name,'signals':signals,'series':totals[name]['series']}
                     for name,signals in selected.items()]}

if __name__=='__main__':
    import unittest
    class Tests(unittest.TestCase):
        def test_rank(self):
            end=date(2026,11,17)
            rows=[]
            for i in range(14):
                for sku in ('a','b'):
                    rows.append(dict(date=(end-timedelta(days=i)).isoformat(),item_id=sku,style_name='Jacket',pdp=100 if i==0 else 20,qty=0 if i==0 else 3))
                    rows.append(dict(date=(end-timedelta(days=i+364)).isoformat(),item_id=sku,style_name='Jacket',pdp=30,qty=10))
            result=rank_styles(rows,end.isoformat())
            self.assertEqual(len(result['cards']),1)
            self.assertEqual(set(result['cards'][0]['signals']),{'pdp_up','qty_down','qty_yoy_down'})
            self.assertEqual(result['cards'][0]['series']['pdp']['current'][-1],200)
            self.assertEqual(rank_styles(rows[:1],end.isoformat())['cards'],[])
            with self.assertRaises(ValueError): rank_styles(rows+rows[:1],end.isoformat())
    unittest.main()
