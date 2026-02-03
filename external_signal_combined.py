#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin
from dataclasses import dataclass
from typing import List
import urllib3
import json

# =================================================================
# 1. 크롤링 엔진 (dc1.py 실전 로직 및 설정)
# =================================================================
KST = timezone(timedelta(hours=9))
GALLERY_ID = "climbing"
BASE_URL = "https://gall.dcinside.com"
MAX_PAGES = 50      # 실제 데이터 확보를 위해 페이지 수 상향
TARGET_DAYS = 7     # 최근 1일(어제~오늘) 데이터 대상

# 분석 대상 브랜드
BRAND_LIST = ["컬럼비아", "노스페이스", "파타고니아", "아크테릭스", "블랙야크", "K2", "캠프라인", "살로몬", "호카", "마무트"]

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

@dataclass
class Post:
    title: str
    url: str
    content: str
    comments: str
    created_at: datetime

def crawl_dc_engine(days: int):
    start_date = (datetime.now(KST) - timedelta(days=days)).date()
    posts = []
    stop_signal = False
    
    print(f"🚀 [M-OS SYSTEM] {GALLERY_ID} 갤러리 분석을 시작합니다...")
    
    for page in range(1, MAX_PAGES + 1):
        if stop_signal: break
        url = f"{BASE_URL}/board/lists/?id={GALLERY_ID}&page={page}"
        resp = SESSION.get(url, timeout=10)
        if resp.status_code != 200: break
        
        soup = BeautifulSoup(resp.text, "html.parser")
        rows = soup.select("tr.ub-content")
        
        for row in rows:
            num = row.select_one("td.gall_num").text
            if not num.isdigit(): continue # 공지사항 제외
            
            a_tag = row.select_one("td.gall_tit a")
            link = urljoin(BASE_URL, a_tag.get("href"))
            
            # 상세 내용 및 댓글 크롤링
            try:
                d_resp = SESSION.get(link, timeout=10)
                d_soup = BeautifulSoup(d_resp.text, "html.parser")
                
                date_el = d_soup.select_one(".gall_date")
                dt = datetime.strptime(date_el.get_text(strip=True), "%Y.%m.%d %H:%M:%S").replace(tzinfo=KST)
                
                # 날짜 제한 체크 (dc1.py 로직)
                if dt.date() < start_date:
                    stop_signal = True
                    break
                
                content = d_soup.select_one(".write_div").get_text("\n", strip=True) if d_soup.select_one(".write_div") else ""
                comments = "\n".join([c.get_text(strip=True) for c in d_soup.select(".comment_list .usertxt")])
                
                posts.append(Post(title=a_tag.text.strip(), url=link, content=content, comments=comments, created_at=dt))
            except:
                continue
        print(f"   - {page}페이지 분석 완료... (현재 수집량: {len(posts)})")
    return posts

# =================================================================
# 2. 데이터 고도화 분석 (브랜드별 문장 전체 추출)
# =================================================================
def process_data(posts: List[Post]):
    brand_map = {b: [] for b in BRAND_LIST}
    word_pool = []

    for p in posts:
        full_text = f"{p.title}\n{p.content}\n{p.comments}"
        # 키워드 집계용
        word_pool.extend(re.sub(r"[^가-힣a-zA-Z]", " ", full_text).split())
        
        # 문장 단위 분할 분석
        sentences = re.split(r'[.!?\n]', full_text)
        for b in BRAND_LIST:
            for s in sentences:
                s_clean = s.strip()
                if b in s_clean and len(s_clean) > 5:
                    brand_map[b].append({
                        "text": s_clean,
                        "url": p.url,
                        "title": p.title
                    })

    # 빈도수 상위 키워드
    top_kws = pd.Series([w for w in word_pool if len(w) > 1]).value_counts().head(15).to_dict()
    return brand_map, top_kws

# =================================================================
# 3. HTML 생성 (디자인 무변형 + 탭 반응형 스크립트 추가)
# =================================================================
def export_portal(brand_map, top_kws):
    # 상단 키워드 칩 생성
    kw_html = "".join([f'<span class="px-4 py-2 rounded-full bg-white/50 border border-white text-sm font-bold text-slate-600"># {k} <span class="text-blue-600">{v}</span></span>' for k, v in top_kws.items()])

    # 탭 메뉴 및 컨텐츠 생성
    tab_menu_html = ""
    content_area_html = ""
    
    # 데이터가 있는 브랜드만 필터링
    active_brands = [b for b in BRAND_LIST if len(brand_map[b]) > 0]
    
    for i, brand in enumerate(active_brands):
        is_first = "true" if i == 0 else "false"
        active_class = "bg-[#002d72] text-white shadow-lg" if i == 0 else "bg-white/50 text-slate-500 hover:bg-white"
        
        # 탭 버튼
        tab_menu_html += f"""
        <button onclick="switchTab('{brand}')" id="tab-{brand}" class="tab-btn px-6 py-3 rounded-2xl font-black transition-all text-sm {active_class}">
            {brand} <span class="ml-1 opacity-60 text-xs">{len(brand_map[brand])}</span>
        </button>"""
        
        # 탭 컨텐츠 (문장 리스트)
        display_style = "block" if i == 0 else "none"
        sentence_cards = ""
        for item in brand_map[brand]:
            sentence_cards += f"""
            <div class="glass-card p-6 border-white/80 hover:scale-[1.01] transition-transform">
                <p class="text-slate-700 font-medium leading-relaxed mb-5 italic">" {item['text']} "</p>
                <div class="flex items-center justify-between pt-4 border-t border-slate-100">
                    <span class="text-[10px] font-bold text-slate-400 uppercase tracking-widest">글제목: {item['title'][:25]}...</span>
                    <a href="{item['url']}" target="_blank" class="px-4 py-2 bg-[#002d72] text-white text-[10px] font-black rounded-xl hover:bg-blue-600 transition-colors flex items-center gap-2">
                        원문 링크 열기 <i class="fa-solid fa-arrow-up-right"></i>
                    </a>
                </div>
            </div>"""
        
        content_area_html += f"""
        <div id="content-{brand}" class="tab-content grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6" style="display: {display_style};">
            {sentence_cards}
        </div>"""

    # 최종 HTML (마케팅포털 HTML.txt 디자인 기반)
    full_html = f"""
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <title>Columbia M-OS Pro | Marketing Intelligent Hub</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
        :root {{ --brand: #002d72; --bg0: #f6f8fb; --bg1: #eef3f9; }}
        body {{ background: linear-gradient(180deg, var(--bg0), var(--bg1)); font-family: 'Plus Jakarta Sans', sans-serif; color: #0f172a; min-height: 100vh; }}
        .glass-card {{ background: rgba(255,255,255,0.55); backdrop-filter: blur(20px); border: 1px solid rgba(255,255,255,0.7); border-radius: 30px; box-shadow: 0 20px 50px rgba(0,45,114,0.05); }}
        .sidebar {{ background: rgba(255,255,255,0.7); backdrop-filter: blur(15px); border-right: 1px solid rgba(255,255,255,0.8); }}
    </style>
</head>
<body class="flex">
    <aside class="w-72 h-screen sticky top-0 sidebar hidden lg:flex flex-col p-8">
        <div class="flex items-center gap-4 mb-16 px-2">
            <div class="w-12 h-12 bg-[#002d72] rounded-2xl flex items-center justify-center text-white shadow-xl shadow-blue-900/20">
                <i class="fa-solid fa-mountain-sun text-xl"></i>
            </div>
            <div>
                <div class="text-xl font-black tracking-tighter italic">M-OS <span class="text-blue-600 font-extrabold">PRO</span></div>
                <div class="text-[9px] font-black uppercase tracking-[0.3em] text-slate-400">Marketing Portal</div>
            </div>
        </div>
        <nav class="space-y-4">
            <div class="p-4 rounded-2xl bg-white shadow-sm text-[#002d72] font-black flex items-center gap-4 cursor-pointer">
                <i class="fa-solid fa-tower-broadcast"></i> <span>Live VOC 분석</span>
            </div>
            <div class="p-4 rounded-2xl text-slate-400 font-bold flex items-center gap-4 hover:bg-white/50 transition-all cursor-not-allowed">
                <i class="fa-solid fa-chart-line"></i> <span>시장 지수</span>
            </div>
        </nav>
    </aside>

    <main class="flex-1 p-8 md:p-16">
        <header class="flex flex-col md:flex-row md:items-center justify-between mb-16 gap-6">
            <div>
                <h1 class="text-5xl font-black tracking-tight text-slate-900 mb-4">VOC Real-time Analysis</h1>
                <p class="text-slate-500 text-lg font-medium italic">디시인사이드 등산 갤러리 브랜드 언급 데이터</p>
            </div>
            <div class="glass-card px-6 py-4 flex items-center gap-4">
                <div class="flex h-3 w-3 relative"><span class="animate-ping absolute h-full w-full rounded-full bg-blue-400 opacity-75"></span><span class="relative inline-flex rounded-full h-3 w-3 bg-blue-600"></span></div>
                <span class="text-sm font-black text-slate-800 tracking-widest uppercase">{datetime.now().strftime('%Y-%m-%d %H:%M')}</span>
            </div>
        </header>

        <section class="glass-card p-10 mb-12">
            <h3 class="text-[10px] font-black uppercase tracking-[0.3em] text-blue-600 mb-8 flex items-center gap-2">
                <i class="fa-solid fa-hashtag"></i> Hot Keywords
            </h3>
            <div class="flex flex-wrap gap-3">{kw_html}</div>
        </section>

        <section>
            <div class="flex flex-wrap gap-2 mb-8">
                {tab_menu_html}
            </div>
            
            <div class="min-h-[500px]">
                {content_area_html}
            </div>
        </section>
    </main>

    <script>
        function switchTab(brand) {{
            // 모든 컨텐츠 숨기기
            document.querySelectorAll('.tab-content').forEach(el => el.style.display = 'none');
            // 선택된 컨텐츠 보이기
            document.getElementById('content-' + brand).style.display = 'grid';
            
            // 모든 버튼 스타일 초기화
            document.querySelectorAll('.tab-btn').forEach(btn => {{
                btn.classList.remove('bg-[#002d72]', 'text-white', 'shadow-lg');
                btn.classList.add('bg-white/50', 'text-slate-500');
            }});
            // 선택된 버튼 스타일 적용
            const activeBtn = document.getElementById('tab-' + brand);
            activeBtn.classList.add('bg-[#002d72]', 'text-white', 'shadow-lg');
            activeBtn.classList.remove('bg-white/50', 'text-slate-500');
        }}
    </script>
</body>
</html>
"""
    with open("reports/external_signal.html", "w", encoding="utf-8-sig") as f:
        f.write(full_html)
    print("✅ [성공] 인터랙티브 리포트 생성 완료: marketing_portal_final.html")

# =================================================================
# 메인 루틴
# =================================================================
if __name__ == "__main__":
    raw_data = crawl_dc_engine(days=TARGET_DAYS)
    if raw_data:
        brand_map, top_kws = process_data(raw_data)
        export_portal(brand_map, top_kws)
    else:
        print("❌ 데이터 수집 실패")
