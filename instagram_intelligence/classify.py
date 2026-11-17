"""Conservative public-post classification; unknown remains unknown."""
import datetime as dt
import re
from zoneinfo import ZoneInfo

KST = ZoneInfo('Asia/Seoul')
AD = re.compile(r'(#광고|#협찬|유료광고|paid partnership|제품을 제공받|광고 포함)', re.I)
NEG = re.compile(r'(불량|환불|하자|불편|별로|실망|누락|불친절|품질 문제|고장)')
POS = re.compile(r'(추천|만족|좋아요|가성비|따뜻|예쁘|잘 샀)')
KOREAN = re.compile(r'[가-힣]')

def parse_date(value):
    if not value or not isinstance(value, str):
        return None
    try:
        value = value.replace('Z', '+00:00')
        parsed = dt.datetime.fromisoformat(value)
        return parsed.astimezone(KST) if parsed.tzinfo else None
    except ValueError:
        return None

def classify(candidate, creator_handles=(), now=None):
    now = now or dt.datetime.now(KST)
    evidence = candidate.get('evidence', [])
    text = ' '.join(str(e.get('title', ''))+' '+str(e.get('description', '')) for e in evidence)
    accounts = [str(e.get('account', '')).lower().lstrip('@') for e in evidence if e.get('account')]
    dates = [parse_date(e.get('published_at')) for e in evidence]
    dates = [d for d in dates if d]
    published = min(dates) if dates else None
    official = any(a in {'columbia_kr', 'columbia_creator_crew'} for a in accounts)
    creator = any(a in {h.lower().lstrip('@') for h in creator_handles} for a in accounts)
    category = 'official' if official else 'creator' if creator else 'sponsored' if AD.search(text) else 'ugc_unverified'
    sentiment = 'negative' if NEG.search(text) else 'positive' if POS.search(text) else 'neutral_or_unknown'
    return {'shortcode': candidate.get('shortcode'), 'published_at_kst': published.isoformat() if published else None,
            'within_48h': (dt.timedelta(0) <= now-published <= dt.timedelta(hours=48)) if published else None,
            'korean_text_evidence': bool(KOREAN.search(text)), 'category': category, 'sentiment': sentiment,
            'verified_tag_or_mention': False, 'verification': 'candidate_only'}
