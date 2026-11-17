"""Evidence-only revenue attribution and reusable SVG trend chart."""
from math import isfinite
from itertools import permutations
from html import escape

def revenue_bridge(current, prior):
    """Exact Shapley decomposition of sessions × orders/session × revenue/order."""
    def factors(row):
        s,o,r=(float(row[k]) for k in ('sessions','orders','revenue'))
        if not all(isfinite(x) and x>=0 for x in (s,o,r)) or not s or not o:
            raise ValueError('Positive sessions and orders required')
        return [s,o/s,r/o]
    a,b=factors(prior),factors(current)
    effects=[0.,0.,0.]
    def prod(x): return x[0]*x[1]*x[2]
    for order in permutations(range(3)):
        v=a.copy()
        for i in order:
            before=prod(v);v[i]=b[i]
            effects[i]+=(prod(v)-before)/6
    return dict(zip(('traffic','conversion','aov'),effects))

def trend_svg(labels,actual,prior,forecast=None,lower=None,upper=None):
    n=len(labels)
    if n<2 or any(len(s)!=n for s in (actual,prior)) or any(s is not None and len(s)!=n for s in (forecast,lower,upper)):
        raise ValueError('Aligned dates and series required')
    series=[actual,prior,forecast or [],lower or [],upper or []]
    vals=[float(v) for s in series for v in s if v is not None]
    if not vals or not all(isfinite(v) and v>=0 for v in vals): raise ValueError('Finite nonnegative data required')
    ceiling=max(vals) or 1
    x=lambda i: 56+800*i/(n-1)
    y=lambda v: 210-180*float(v)/ceiling
    out=['<svg class="chart" viewBox="0 0 900 255" role="img" aria-label="Revenue trend and forecast">']
    for j in range(5):
        v=ceiling*j/4; yy=y(v)
        out.append(f'<line class="gridline" x1="56" x2="856" y1="{yy}" y2="{yy}"/><text x="50" y="{yy+4}" text-anchor="end" font-size="10">{v:,.0f}</text>')
    def path(s):
        active=False; cmds=[]
        for i,v in enumerate(s):
            if v is None: active=False;continue
            cmds.append(f'{"L" if active else "M"}{x(i):.2f} {y(v):.2f}');active=True
        return ' '.join(cmds)
    if lower is not None and upper is not None:
        idx=[i for i in range(n) if lower[i] is not None and upper[i] is not None]
        if idx and idx==list(range(idx[0],idx[-1]+1)) and all(lower[i]<=upper[i] for i in idx):
            coords=[(x(i),y(upper[i])) for i in idx]+[(x(i),y(lower[i])) for i in reversed(idx)]
            out.append('<polygon class="interval" points="'+' '.join(f'{a:.2f},{b:.2f}' for a,b in coords)+'"/>')
    for cls,s in [('prior',prior),('actual',actual),('forecast',forecast or [])]:
        out.append(f'<path class="{cls}" d="{path(s)}"/>')
    known=[i for i,v in enumerate(actual) if v is not None]
    if known:
        xx=x(known[-1]);out.append(f'<line class="focus" x1="{xx}" x2="{xx}" y1="20" y2="210"/>')
    for i in sorted(set([0,n//2,n-1]+known[-1:])):
        out.append(f'<text x="{x(i)}" y="235" font-size="11" text-anchor="middle">{escape(str(labels[i]))}</text>')
    return ''.join(out)+'</svg>'
