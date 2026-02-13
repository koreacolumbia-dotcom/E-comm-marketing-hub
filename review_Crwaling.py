<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>VOC Dashboard | Official + Naver Reviews</title>

  <!-- Tailwind + FontAwesome -->
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">

  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    :root { --brand:#002d72; --bg0:#f6f8fb; --bg1:#eef3f9; }

    body{
      background: linear-gradient(180deg, var(--bg0), var(--bg1));
      font-family: 'Plus Jakarta Sans', sans-serif;
      color:#0f172a;
      min-height:100vh;
    }

    /* Glass system */
    .glass-card{
      background: rgba(255,255,255,0.55);
      backdrop-filter: blur(20px);
      border: 1px solid rgba(255,255,255,0.75);
      border-radius: 30px;
      box-shadow: 0 20px 50px rgba(0,45,114,0.05);
    }
    .sidebar{
      background: rgba(255,255,255,0.70);
      backdrop-filter: blur(15px);
      border-right: 1px solid rgba(255,255,255,0.80);
    }
    .summary-card{
      border-radius: 26px;
      background: rgba(255,255,255,0.55);
      border: 1px solid rgba(255,255,255,0.75);
      backdrop-filter: blur(18px);
      box-shadow: 0 20px 50px rgba(0,45,114,0.05);
      padding: 18px 20px;
    }
    .small-label{
      font-size: 10px;
      letter-spacing: 0.3em;
      text-transform: uppercase;
      font-weight: 900;
    }
    .input-glass{
      background: rgba(255,255,255,0.65);
      border: 1px solid rgba(255,255,255,0.80);
      border-radius: 18px;
      padding: 12px 14px;
      outline: none;
      font-weight: 800;
      color:#0f172a;
    }
    .input-glass:focus{
      box-shadow: 0 0 0 4px rgba(0,45,114,0.10);
      border-color: rgba(0,45,114,0.25);
    }
    .chip{
      border-radius: 9999px;
      padding: 10px 14px;
      font-weight: 900;
      font-size: 12px;
      border: 1px solid rgba(255,255,255,0.85);
      background: rgba(255,255,255,0.60);
      color:#334155;
      cursor:pointer;
      user-select:none;
    }
    .chip.active{
      background: rgba(0,45,114,0.95);
      color:#fff;
      border-color: rgba(0,45,114,1);
      box-shadow: 0 10px 30px rgba(0,45,114,0.15);
    }

    /* Tab buttons */
    .tab-btn{
      padding: 10px 14px;
      border-radius: 18px;
      font-weight: 900;
      font-size: 12px;
      border: 1px solid rgba(255,255,255,0.85);
      background: rgba(255,255,255,0.60);
      color:#475569;
      transition: all .15s ease;
    }
    .tab-btn:hover{ background: rgba(255,255,255,0.90); }
    .tab-btn.active{
      background: rgba(0,45,114,0.95);
      color:#fff;
      border-color: rgba(0,45,114,1);
      box-shadow: 0 10px 30px rgba(0,45,114,0.15);
    }

    /* overlay */
    .overlay{
      position: fixed;
      inset:0;
      background: rgba(255,255,255,0.65);
      backdrop-filter: blur(10px);
      display:none;
      align-items:center;
      justify-content:center;
      z-index:9999;
    }
    .overlay.show{ display:flex; }
    .spinner{
      width:56px;height:56px;border-radius:9999px;
      border:6px solid rgba(0,0,0,0.08);
      border-top-color: rgba(0,45,114,0.95);
      animation: spin .9s linear infinite;
    }
    @keyframes spin { to { transform: rotate(360deg);} }

    /* Tables */
    .tbl{
      width:100%;
      border-collapse: separate;
      border-spacing: 0;
      overflow:hidden;
      border-radius: 22px;
      border: 1px solid rgba(255,255,255,0.85);
      background: rgba(255,255,255,0.55);
    }
    .tbl th{
      font-size: 11px;
      letter-spacing: .22em;
      text-transform: uppercase;
      font-weight: 900;
      color:#475569;
      background: rgba(255,255,255,0.75);
      padding: 14px 14px;
      position: sticky;
      top: 0;
      z-index: 1;
    }
    .tbl td{
      padding: 14px 14px;
      border-top: 1px solid rgba(255,255,255,0.75);
      font-weight: 800;
      color:#0f172a;
      font-size: 13px;
      vertical-align: top;
    }
    .tbl .muted{ color:#64748b; font-weight:800; font-size:12px; }

    /* Review cards */
    .review-card{
      border-radius: 26px;
      background: rgba(255,255,255,0.55);
      border: 1px solid rgba(255,255,255,0.80);
      backdrop-filter: blur(18px);
      box-shadow: 0 16px 42px rgba(0,45,114,0.04);
      padding: 18px 18px;
    }
    .badge{
      display:inline-flex;
      align-items:center;
      gap:6px;
      padding: 6px 10px;
      border-radius: 9999px;
      font-size: 11px;
      font-weight: 900;
      border: 1px solid rgba(255,255,255,0.85);
      background: rgba(255,255,255,0.65);
      color:#334155;
    }
    .badge.neg{ background: rgba(239,68,68,0.10); color:#b91c1c; border-color: rgba(239,68,68,0.18); }
    .badge.pos{ background: rgba(16,185,129,0.10); color:#047857; border-color: rgba(16,185,129,0.18); }
    .badge.size{ background: rgba(59,130,246,0.10); color:#1d4ed8; border-color: rgba(59,130,246,0.18); }

    /* image */
    .img-box{ width:72px; height:72px; border-radius:18px; overflow:hidden; background: rgba(255,255,255,0.70); border:1px solid rgba(255,255,255,0.85); }
    .img-box img{ width:100%; height:100%; object-fit:cover; display:block; }

    /* Two-pane layout for review list */
    .review-list{
      display:grid;
      grid-template-columns: 1fr;
      gap: 14px;
    }
    @media (min-width: 1024px){
      .review-list{ grid-template-columns: 1fr 1fr; }
    }

    /* Embedded mode (optional) */
    body.embedded aside, body.embedded header { display:none !important; }
    body.embedded main{ padding: 24px !important; }

    /* clamp helpers */
    .line-clamp-1{
      overflow:hidden; display:-webkit-box; -webkit-line-clamp:1; -webkit-box-orient:vertical;
    }
    .line-clamp-2{
      overflow:hidden; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical;
    }
  </style>
</head>

<body class="flex">

  <!-- overlay -->
  <div id="overlay" class="overlay">
    <div class="glass-card px-8 py-7 flex items-center gap-4">
      <div class="spinner"></div>
      <div>
        <div class="text-sm font-black text-slate-900">Processing...</div>
        <div id="overlayMsg" class="text-xs font-bold text-slate-500 mt-1">잠시만요</div>
      </div>
    </div>
  </div>

  <!-- Sidebar -->
  <aside class="w-72 h-screen sticky top-0 sidebar hidden lg:flex flex-col p-8">
    <div class="flex items-center gap-4 mb-14 px-2">
      <div class="w-12 h-12 bg-[color:var(--brand)] rounded-2xl flex items-center justify-center text-white shadow-xl shadow-blue-900/20">
        <i class="fa-solid fa-comments text-xl"></i>
      </div>
      <div>
        <div class="text-xl font-black tracking-tighter italic">VOC <span class="text-blue-600 font-extrabold">HUB</span></div>
        <div class="text-[9px] font-black uppercase tracking-[0.3em] text-slate-400">Official + Naver Reviews</div>
      </div>
    </div>

    <div class="glass-card p-5">
      <div class="small-label text-blue-600 mb-2">Schedule</div>
      <div class="text-sm font-black text-slate-900">주 1회</div>
      <div class="text-xs font-bold text-slate-500 mt-2">월요일 오전 9시 (KST)</div>
      <div class="mt-4 text-xs font-bold text-slate-500">
        * 이 레이아웃은 정적 HTML (데모 데이터 탑재). <br/>
        실제 데이터 연결은 후속 단계에서 삽입예정입니닷
      </div>
    </div>

    <div class="mt-auto pt-8 text-xs font-bold text-slate-500">
      <div class="small-label text-blue-600 mb-2">Snapshot</div>
      <div>수집일: <span id="runDateSide" class="font-black text-slate-700">-</span></div>
      <div class="mt-2">기간: <span id="dateRangeSide" class="font-black text-slate-700">-</span></div>
    </div>
  </aside>

  <!-- Main -->
  <main class="flex-1 p-8 md:p-14">

    <header class="flex flex-col md:flex-row md:items-center justify-between mb-10 gap-6">
      <div>
        <h1 class="text-4xl md:text-5xl font-black tracking-tight text-slate-900 mb-3">
          Official몰 & Naver 리뷰 VOC 대시보드
        </h1>
        <div id="headerMeta" class="text-sm text-slate-500 font-bold">
          - · - · 주 1회 자동 업데이트(월 09:00)
        </div>
      </div>

      <div class="glass-card px-6 py-4 flex items-center gap-4">
        <div class="flex h-3 w-3 relative">
          <span class="animate-ping absolute h-full w-full rounded-full bg-blue-400 opacity-75"></span>
          <span class="relative inline-flex rounded-full h-3 w-3 bg-blue-600"></span>
        </div>
        <span class="text-sm font-black text-slate-800 tracking-widest uppercase">VOC Snapshot</span>
      </div>
    </header>

    <!-- 0) Tabs -->
    <section class="mb-8">
      <div class="flex flex-wrap gap-2 items-center">
        <button class="tab-btn active" data-tab="combined" onclick="switchSourceTab('combined')">
          Combined <span class="ml-2 opacity-70">공식몰+네이버(1탭)</span>
        </button>
        <button class="tab-btn" data-tab="official" onclick="switchSourceTab('official')">
          Official Mall
        </button>
        <button class="tab-btn" data-tab="naver" onclick="switchSourceTab('naver')">
          Naver
        </button>

        <div class="ml-auto flex items-center gap-3">
          <div class="small-label text-blue-600">View</div>
          <button class="chip active" id="chip-daily" onclick="toggleChip('daily')">당일 업로드 순</button>
          <button class="chip" id="chip-pos" onclick="toggleChip('pos')">긍정 키워드 포함</button>
          <button class="chip" id="chip-size" onclick="toggleChip('size')">사이즈 이슈만</button>
          <button class="chip" id="chip-low" onclick="toggleChip('low')">저평점만</button>
        </div>
      </div>
    </section>

    <!-- 1) Summary -->
    <section class="mb-10">
      <div class="glass-card p-8">
        <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
          <div>
            <div class="small-label text-blue-600 mb-2">1. Summary</div>
            <div class="text-2xl font-black text-slate-900">핵심 이슈 한 장 요약</div>
          </div>
          <div class="text-xs font-black text-slate-500">
            * 현재는 데모 데이터(샘플)로 표시
          </div>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <div class="summary-card">
            <div class="small-label text-blue-600 mb-2">1-1) Size mention</div>
            <div class="text-3xl font-black"><span id="sizeMentionRate">-</span>%</div>
            <div class="text-xs font-bold text-slate-500 mt-2">전체 리뷰 중 사이즈 관련 언급 비중</div>
          </div>

          <div class="summary-card">
            <div class="small-label text-red-600 mb-2">1-2) Complaint Top 5</div>
            <div id="topComplaints" class="mt-2 flex flex-wrap gap-2"></div>
            <div class="text-xs font-bold text-slate-500 mt-3">반복적으로 등장하는 불만 키워드</div>
          </div>

          <div class="summary-card">
            <div class="small-label text-blue-600 mb-2">1-3) Priority Top 3</div>
            <ol id="priorityTop3" class="mt-2 space-y-2"></ol>
            <div class="text-xs font-bold text-slate-500 mt-3">개선 필요 제품 Top 3(사이즈 이슈율)</div>
          </div>
        </div>

        <div class="mt-6 grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div class="summary-card">
            <div class="small-label text-blue-600 mb-2"><i class="fa-solid fa-list mr-2"></i>요구사항</div>
            <ul class="text-sm font-extrabold text-slate-700 space-y-2">
              <li>• 당일 올라온 리뷰는 <span class="text-slate-900">업로드 순서대로</span> 보기</li>
              <li>• <span class="text-slate-900">긍정 키워드 포함</span> 리뷰는 별도 리스트 제공</li>
              <li>• 공식몰/네이버는 <span class="text-slate-900">한 탭(Combined)</span>에서 합쳐서도 보기</li>
              <li>• 상품코드마다 <span class="text-slate-900">이미지</span> 추가</li>
              <li>• 100자 이상 텍스트 리뷰는 <span class="text-slate-900">이미지(캡처/첨부)</span> 끌어오기</li>
            </ul>
          </div>

          <div class="summary-card">
            <div class="small-label text-blue-600 mb-2"><i class="fa-solid fa-filter mr-2"></i>빠른 필터(UI + 데모)</div>
            <div class="flex flex-wrap gap-2">
              <span class="badge pos"><i class="fa-solid fa-face-smile"></i> #긍정키워드</span>
              <span class="badge size"><i class="fa-solid fa-ruler"></i> #사이즈이슈</span>
              <span class="badge neg"><i class="fa-solid fa-triangle-exclamation"></i> #저평점</span>
              <span class="badge"><i class="fa-solid fa-image"></i> #100자+이미지</span>
            </div>
            <div class="text-xs font-bold text-slate-500 mt-3">칩/탭은 실제로 리스트를 필터링(샘플)</div>
          </div>
        </div>
      </div>
    </section>

    <!-- 2) Priority ranking -->
    <section class="mb-10">
      <div class="glass-card p-8">
        <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
          <div>
            <div class="small-label text-blue-600 mb-2">2. 개선 우선순위 제품 랭킹</div>
          </div>
          <div class="flex gap-2">
            <button class="chip active" id="rank-size" onclick="switchRankMode('size')">2-1) 사이즈 이슈율</button>
            <button class="chip" id="rank-low" onclick="switchRankMode('low')">2-2) 저평점 비중</button>
            <button class="chip" id="rank-both" onclick="switchRankMode('both')">2-3) 교집합</button>
          </div>
        </div>

        <div class="overflow-auto">
          <table class="tbl min-w-[980px]">
            <thead>
              <tr>
                <th class="text-left">제품명</th>
                <th class="text-left">리뷰 수</th>
                <th class="text-left">사이즈 이슈율</th>
                <th class="text-left">저평점 비중</th>
                <th class="text-left">주요 문제 키워드</th>
              </tr>
            </thead>
            <tbody id="rankingBody"></tbody>
          </table>
        </div>
      </div>
    </section>

    <!-- 3) Size issue structure -->
    <section class="mb-10">
      <div class="glass-card p-8">
        <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
          <div>
            <div class="small-label text-blue-600 mb-2">3. 사이즈 이슈 구조 분석</div>
          </div>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <div class="summary-card">
            <div class="small-label text-blue-600 mb-2">3-1) small vs big</div>
            <div class="flex items-center justify-between mt-3">
              <span class="badge size">too_small <span id="tooSmall">-</span>%</span>
              <span class="badge size">too_big <span id="tooBig">-</span>%</span>
            </div>
            <div class="text-xs font-bold text-slate-500 mt-3">비율만 노출(그래프 제외)</div>
          </div>

          <div class="summary-card lg:col-span-2">
            <div class="small-label text-blue-600 mb-2">3-2) 옵션(사이즈)별 이슈율</div>
            <div class="overflow-auto mt-3">
              <table class="tbl min-w-[820px]">
                <thead>
                  <tr>
                    <th class="text-left">옵션 사이즈</th>
                    <th class="text-left">리뷰 수</th>
                    <th class="text-left">too_small</th>
                    <th class="text-left">too_big</th>
                    <th class="text-left">정사이즈/기타</th>
                  </tr>
                </thead>
                <tbody id="sizeOptBody"></tbody>
              </table>
            </div>
          </div>
        </div>

        <div class="summary-card mt-4">
          <div class="small-label text-blue-600 mb-2">3-3) 핏 관련 반복 표현</div>
          <div id="fitWords" class="flex flex-wrap gap-2 mt-2"></div>
        </div>
      </div>
    </section>

    <!-- 4) Complaint keywords -->
    <section class="mb-10">
      <div class="glass-card p-8">
        <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
          <div>
            <div class="small-label text-blue-600 mb-2">4. 반복 불만 키워드 분석</div>
          </div>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <div class="summary-card">
            <div class="small-label text-red-600 mb-2">4-1) 저평점에서 상대적으로 많이</div>
            <div id="liftWords" class="mt-3 space-y-2"></div>
          </div>

          <div class="summary-card">
            <div class="small-label text-blue-600 mb-2">4-2) 제품 공통 문제 키워드</div>
            <div id="commonIssues" class="mt-3 flex flex-wrap gap-2"></div>
            <div class="text-xs font-bold text-slate-500 mt-3">사전 기반 토픽 분류도 후속 가능</div>
          </div>

          <div class="summary-card">
            <div class="small-label text-blue-600 mb-2">4-3) 사이즈 이슈에서 반복 표현</div>
            <div id="sizePhrases" class="mt-3 flex flex-wrap gap-2"></div>
          </div>
        </div>
      </div>
    </section>

    <!-- 5) Evidence reviews -->
    <section class="mb-10">
      <div class="glass-card p-8">
        <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
          <div>
            <div class="small-label text-blue-600 mb-2">5. 대표 근거 리뷰</div>
          </div>

          <div class="flex gap-2">
            <button class="chip active" id="ev-size" onclick="switchEvidence('size')">5-1) 사이즈 이슈</button>
            <button class="chip" id="ev-low" onclick="switchEvidence('low')">5-2) 저평점</button>
            <button class="chip" id="ev-req" onclick="switchEvidence('req')">5-3) 개선 요청</button>
          </div>
        </div>

        <div id="evidenceList" class="review-list"></div>
      </div>
    </section>

    <!-- 6) Daily review feed -->
    <section class="mb-10">
      <div class="glass-card p-8">
        <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
          <div>
            <div class="small-label text-blue-600 mb-2">Daily Feed</div>
            <div class="text-2xl font-black text-slate-900">그날 올라온 리뷰 (업로드 순)</div>
            <div class="text-sm font-bold text-slate-500 mt-2">기본은 “전체 노출”, 필터는 최소화</div>
          </div>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-4 gap-3 mb-6">
          <select id="productSelect" class="input-glass" onchange="renderAll()">
            <option value="">제품 선택 (전체)</option>
          </select>

          <select id="sizeSelect" class="input-glass" onchange="renderAll()">
            <option value="">옵션 사이즈 (전체)</option>
            <option value="S">S</option><option value="M">M</option><option value="L">L</option><option value="XL">XL</option>
          </select>

          <select id="sortSelect" class="input-glass" onchange="renderAll()">
            <option value="upload">정렬: 업로드 순 (기본)</option>
            <option value="latest">최신순</option>
            <option value="long">리뷰 길이 긴 순</option>
            <option value="low">저평점순</option>
          </select>

          <input id="qInput" class="input-glass" placeholder="텍스트 검색(옵션)" oninput="renderAll()" />
        </div>

        <div id="dailyFeed" class="space-y-4"></div>

        <div class="hidden review-card text-center" id="noResults">
          <div class="text-lg font-black text-slate-800">검색 결과가 없습니다.</div>
        </div>
      </div>
    </section>

    <footer class="text-xs font-bold text-slate-500 pb-8">
      * 현재 페이지는 <b>데모 데이터</b>로 채워진 정적 HTML입니다.<br/>
      * 실제 데이터 연결(크롤링/집계/치환)은 후속 단계에서 자동화 가능합니다.
    </footer>

  </main>

  <script>
    // ----------------------------
    // 0) Overlay helpers
    // ----------------------------
    const overlay = document.getElementById('overlay');
    const overlayMsg = document.getElementById('overlayMsg');

    const uiState = {
      sourceTab: 'combined',
      chips: { daily:true, pos:false, size:false, low:false },
      rankMode: 'size',
      evidenceMode: 'size'
    };

    function showOverlay(msg){
      overlayMsg.textContent = msg || '잠시만요';
      overlay.classList.add('show');
    }
    function hideOverlay(){
      overlay.classList.remove('show');
    }
    function runWithOverlay(msg, fn){
      showOverlay(msg);
      setTimeout(() => { try { fn(); } finally { requestAnimationFrame(hideOverlay); } }, 0);
    }

    function switchSourceTab(tab){
      runWithOverlay('Switching source...', () => {
        uiState.sourceTab = tab;
        document.querySelectorAll('.tab-btn').forEach(b => {
          b.classList.toggle('active', b.getAttribute('data-tab') === tab);
        });
        renderAll();
      });
    }

    function toggleChip(key){
      runWithOverlay('Applying filter...', () => {
        uiState.chips[key] = !uiState.chips[key];
        const el = document.getElementById('chip-' + key);
        if (el) el.classList.toggle('active', uiState.chips[key]);
        renderAll();
      });
    }

    function switchRankMode(mode){
      runWithOverlay('Switching ranking...', () => {
        uiState.rankMode = mode;
        document.getElementById('rank-size').classList.toggle('active', mode==='size');
        document.getElementById('rank-low').classList.toggle('active', mode==='low');
        document.getElementById('rank-both').classList.toggle('active', mode==='both');
        renderAll();
      });
    }

    function switchEvidence(mode){
      runWithOverlay('Switching evidence...', () => {
        uiState.evidenceMode = mode;
        document.getElementById('ev-size').classList.toggle('active', mode==='size');
        document.getElementById('ev-low').classList.toggle('active', mode==='low');
        document.getElementById('ev-req').classList.toggle('active', mode==='req');
        renderAll();
      });
    }

    // Embedded mode
    (function () {
      try { if (window.self !== window.top) document.body.classList.add("embedded"); }
      catch (e) { document.body.classList.add("embedded"); }
    })();

    // ----------------------------
    // 1) Demo data (임의 채움)
    // ----------------------------
    const demo = (() => {
      const runDate = new Date();
      const pad2 = (n) => String(n).padStart(2,'0');
      const fmt = (d) => `${d.getFullYear()}.${pad2(d.getMonth()+1)}.${pad2(d.getDate())}`;
      const rangeStart = new Date(runDate.getTime() - 6*24*3600*1000);
      const dateRange = `${fmt(rangeStart)} ~ ${fmt(runDate)}`;

      const products = [
        { code:"C6C7-001", name:"Titan Ridge™ 다운 재킷", cat:"jacket" },
        { code:"C6C7-014", name:"Basin Trail™ 플리스 집업", cat:"fleece" },
        { code:"C6C7-022", name:"Storm Crest™ 바람막이", cat:"windbreaker" },
        { code:"C6C7-035", name:"Summit Peak™ 등산화", cat:"shoes" },
        { code:"C6C7-048", name:"Riverbend™ 소프트쉘", cat:"softshell" },
        { code:"C6C7-056", name:"Alpine Light™ 패딩 베스트", cat:"vest" }
      ];

      // 리뷰 샘플
      const sampleTexts = {
        pos: [
          "가볍고 따뜻해요. 출근길에도 부담 없고, 핏이 생각보다 예쁘게 떨어집니다. 재구매 의사 있어요.",
          "색감이 화면이랑 거의 같고, 마감도 탄탄합니다. 겨울에 데일리로 딱.",
          "배송도 빠르고 사이즈 안내대로 갔더니 딱 맞네요. 만족!"
        ],
        low: [
          "생각보다 얇아서 한파엔 부족해요. 실밥이 조금 튀어나온 부분도 있었고요.",
          "발볼이 좁게 나왔는지 오래 걸으니 앞쪽이 아파요. 반품 고민중입니다.",
          "색상 차이가 있어요. 사진보다 톤이 어둡고 주머니 지퍼가 뻑뻑합니다."
        ],
        size: [
          "정사이즈라길래 샀는데 어깨가 타이트해요. 한 치수 크게 갈 걸 그랬습니다.",
          "기장이 길게 느껴져요. 체형 따라 다를 듯. 소매도 약간 길어요.",
          "허리는 넉넉한데 가슴이 타이트… 상체 있는 분은 업사이징 추천합니다."
        ],
        req: [
          "사이즈 가이드에 '어깨/가슴 실측' 표기가 더 명확하면 좋겠어요.",
          "지퍼가 손에 잘 안 잡혀요. 풀러 개선되면 더 좋을 것 같습니다.",
          "색상 사진을 자연광/실내 두 버전으로 보여주면 오해가 줄 듯!"
        ]
      };

      const sources = ["Official", "Naver"];
      const sizes = ["S","M","L","XL"];

      const rand = (arr) => arr[Math.floor(Math.random()*arr.length)];
      const randInt = (a,b) => a + Math.floor(Math.random()*(b-a+1));

      const reviews = [];
      // 42개 정도 생성
      for (let i=0;i<42;i++){
        const p = rand(products);
        const src = rand(sources);
        const rating = Math.random() < 0.25 ? randInt(1,2) : (Math.random()<0.25 ? randInt(3,4) : 5);
        const opt = rand(sizes);

        const tags = [];
        let text = "";

        // 텍스트 유형 결정
        const r = Math.random();
        if (rating <= 2){
          tags.push("low");
          text = rand(sampleTexts.low);
        } else if (r < 0.30){
          tags.push("size");
          text = rand(sampleTexts.size);
        } else if (r < 0.55){
          tags.push("pos");
          text = rand(sampleTexts.pos);
        } else {
          tags.push("req");
          text = rand(sampleTexts.req);
        }

        // size 태그 확률로 추가
        if (Math.random() < 0.28 && !tags.includes("size")){
          tags.push("size");
          text = rand(sampleTexts.size) + " " + text;
        }

        // pos 태그 확률로 추가
        if (Math.random() < 0.22 && !tags.includes("pos") && rating >= 4){
          tags.push("pos");
        }

        // 100자+ 리뷰 일부 만들기
        if (Math.random() < 0.35){
          text = text + " " + "착용감, 보온성, 소재감까지 종합적으로 괜찮았고 다음번에는 다른 색상도 고려 중입니다. 사이즈는 평소 착용 기준으로 선택하세요.";
        }

        const createdAt = new Date(runDate.getTime() - randInt(0, 6)*24*3600*1000 - randInt(0, 23)*3600*1000 - randInt(0, 59)*60*1000);

        reviews.push({
          id: `rv_${i+1}`,
          product_code: p.code,
          product_name: p.name,
          option_size: opt,
          source: src,
          rating,
          created_at: createdAt.toISOString(),
          text,
          tags,
          has_text_image: text.length >= 100 && Math.random() < 0.60
        });
      }

      // 요약/키워드 데모 (대충 현실적으로)
      const complaintTop5 = ["지퍼", "색상차이", "실밥/마감", "배송지연", "냄새"];
      const commonIssues = ["배송", "품질", "마감", "냄새", "색상 차이", "원단", "내구성", "지퍼"];
      const sizePhrases = ["정사이즈", "한치수 크게", "한치수 작게", "어깨", "소매", "기장", "발볼", "허리"];
      const fitWords = ["작다","크다","타이트","넉넉","길다","짧다","핏이 예쁨","부해보임","슬림","오버핏"];

      const liftWords = [
        { word:"지퍼", lift:1.9 },
        { word:"마감", lift:1.7 },
        { word:"색상", lift:1.6 },
        { word:"냄새", lift:1.5 },
        { word:"실밥", lift:1.4 }
      ];

      return {
        runDateStr: fmt(runDate),
        dateRangeStr: dateRange,
        products,
        reviews,
        complaintTop5,
        commonIssues,
        sizePhrases,
        fitWords,
        liftWords
      };
    })();

    // ----------------------------
    // 2) Utilities
    // ----------------------------
    const esc = (s) => String(s)
      .replaceAll("&","&amp;")
      .replaceAll("<","&lt;")
      .replaceAll(">","&gt;")
      .replaceAll('"',"&quot;")
      .replaceAll("'","&#039;");

    const fmtDT = (iso) => {
      const d = new Date(iso);
      const pad2 = (n) => String(n).padStart(2,'0');
      return `${d.getFullYear()}.${pad2(d.getMonth()+1)}.${pad2(d.getDate())} ${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
    };

    const makeSeed = (s) => {
      s = (s || "").toString();
      let h = 0;
      for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
      return h || Math.floor(Math.random() * 1e9);
    };

    function applyAutoImages(){
      document.querySelectorAll("img.auto-img").forEach((img) => {
        const code = img.dataset.product || "seed";
        const seed = makeSeed(code);
        const unsplashUrl = `https://source.unsplash.com/300x300/?outdoor,jacket,clothing&sig=${seed}`;
        const picsumUrl = `https://picsum.photos/seed/${seed}/300/300`;

        img.referrerPolicy = "no-referrer";
        img.loading = "lazy";

        img.onerror = () => {
          if (!img.dataset.fallbackApplied) {
            img.dataset.fallbackApplied = "1";
            img.src = picsumUrl;
          }
        };
        img.src = unsplashUrl;
      });
    }

    // ----------------------------
    // 3) Data filtering by UI state
    // ----------------------------
    function getFilteredReviews(){
      let rows = demo.reviews.slice();

      // source tab
      if (uiState.sourceTab === "official") rows = rows.filter(r => r.source === "Official");
      if (uiState.sourceTab === "naver") rows = rows.filter(r => r.source === "Naver");

      // product dropdown
      const pSel = document.getElementById("productSelect");
      const productCode = pSel ? pSel.value : "";
      if (productCode) rows = rows.filter(r => r.product_code === productCode);

      // size dropdown
      const sSel = document.getElementById("sizeSelect");
      const sizeOpt = sSel ? sSel.value : "";
      if (sizeOpt) rows = rows.filter(r => r.option_size === sizeOpt);

      // query
      const q = (document.getElementById("qInput")?.value || "").trim().toLowerCase();
      if (q) {
        rows = rows.filter(r =>
          r.product_name.toLowerCase().includes(q) ||
          r.product_code.toLowerCase().includes(q) ||
          r.text.toLowerCase().includes(q) ||
          r.option_size.toLowerCase().includes(q)
        );
      }

      // chips
      if (uiState.chips.pos) rows = rows.filter(r => r.tags.includes("pos"));
      if (uiState.chips.size) rows = rows.filter(r => r.tags.includes("size"));
      if (uiState.chips.low) rows = rows.filter(r => r.rating <= 2 || r.tags.includes("low"));

      // sort
      const sort = document.getElementById("sortSelect")?.value || "upload";
      if (sort === "latest") rows.sort((a,b) => new Date(b.created_at) - new Date(a.created_at));
      else if (sort === "long") rows.sort((a,b) => (b.text.length - a.text.length));
      else if (sort === "low") rows.sort((a,b) => (a.rating - b.rating) || (new Date(b.created_at)-new Date(a.created_at)));
      else {
        // upload order: 오래된→최근 (실무에서 "그날 올라온 순서" 느낌)
        rows.sort((a,b) => new Date(a.created_at) - new Date(b.created_at));
      }

      return rows;
    }

    // ----------------------------
    // 4) Metrics for summary/ranking (demo but consistent)
    // ----------------------------
    function calcMetrics(reviews){
      const total = reviews.length || 1;
      const sizeMention = reviews.filter(r => r.tags.includes("size")).length;
      const low = reviews.filter(r => r.rating <= 2).length;

      // size structure
      const sizeIssue = reviews.filter(r => r.tags.includes("size"));
      const tooSmall = Math.round(55 + Math.random()*10); // demo ratio
      const tooBig = 100 - tooSmall;

      // product-level stats
      const byProd = new Map();
      for (const r of reviews){
        if (!byProd.has(r.product_code)){
          byProd.set(r.product_code, {
            product_code: r.product_code,
            product_name: r.product_name,
            reviews: 0,
            sizeIssue: 0,
            low: 0,
            issueKwds: new Map()
          });
        }
        const g = byProd.get(r.product_code);
        g.reviews += 1;
        if (r.tags.includes("size")) g.sizeIssue += 1;
        if (r.rating <= 2) g.low += 1;

        // issue keywords rough extraction by tags
        const kwPool = [];
        if (r.rating <= 2) kwPool.push("지퍼","마감","냄새","색상","배송");
        if (r.tags.includes("size")) kwPool.push("정사이즈","기장","어깨","발볼");
        if (r.text.includes("지퍼")) kwPool.push("지퍼");
        if (r.text.includes("색")) kwPool.push("색상");
        if (r.text.includes("실밥")) kwPool.push("실밥");
        if (kwPool.length){
          for (const kw of kwPool){
            g.issueKwds.set(kw, (g.issueKwds.get(kw)||0) + 1);
          }
        }
      }

      // ranking rows
      const rows = Array.from(byProd.values()).map(g => {
        const sizeRate = Math.round((g.sizeIssue / Math.max(1,g.reviews))*100);
        const lowRate = Math.round((g.low / Math.max(1,g.reviews))*100);
        const kwds = Array.from(g.issueKwds.entries())
          .sort((a,b)=>b[1]-a[1])
          .slice(0,4)
          .map(x=>x[0])
          .join(", ");
        return { ...g, sizeRate, lowRate, kwds };
      });

      // rank modes
      const rankSize = rows.slice().sort((a,b)=> b.sizeRate - a.sizeRate || b.reviews - a.reviews);
      const rankLow  = rows.slice().sort((a,b)=> b.lowRate  - a.lowRate  || b.reviews - a.reviews);
      const rankBoth = rows.slice().sort((a,b)=> ((b.sizeRate+b.lowRate) - (a.sizeRate+a.lowRate)) || b.reviews - a.reviews);

      // size option table
      const sizeOpts = ["S","M","L","XL"].map(sz => {
        const rs = reviews.filter(r => r.option_size === sz);
        const cnt = rs.length;
        const si = rs.filter(r => r.tags.includes("size")).length;
        // split too_small / too_big demo
        const small = cnt ? Math.round((si/cnt)*100 * 0.55) : 0;
        const big = cnt ? Math.round((si/cnt)*100 * 0.45) : 0;
        const ok = Math.max(0, 100 - small - big);
        return { sz, cnt, small, big, ok };
      });

      return {
        total,
        sizeMentionRate: Math.round((sizeMention/total)*100),
        lowRate: Math.round((low/total)*100),
        tooSmall,
        tooBig,
        rankSize,
        rankLow,
        rankBoth,
        sizeOpts
      };
    }

    // ----------------------------
    // 5) Renderers
    // ----------------------------
    function renderHeader(){
      document.getElementById("runDateSide").textContent = demo.runDateStr;
      document.getElementById("dateRangeSide").textContent = demo.dateRangeStr;
      document.getElementById("headerMeta").textContent = `${demo.runDateStr} · ${demo.dateRangeStr} · 주 1회 자동 업데이트(월 09:00)`;
    }

    function renderSummary(metrics){
      document.getElementById("sizeMentionRate").textContent = metrics.sizeMentionRate;

      // top complaints (demo list)
      const el = document.getElementById("topComplaints");
      el.innerHTML = demo.complaintTop5.map(k => `<span class="badge neg">${esc(k)}</span>`).join("");

      // priority top3 from rankSize
      const top3 = metrics.rankSize.slice(0,3);
      const ol = document.getElementById("priorityTop3");
      ol.innerHTML = top3.map((r,idx) => `
        <li class="flex items-center justify-between gap-3">
          <span class="font-black text-slate-900">${esc(r.product_name)}</span>
          <span class="badge size">Size ${r.sizeRate}%</span>
        </li>
      `).join("");
    }

    function renderRanking(metrics){
      let rows = [];
      if (uiState.rankMode === "size") rows = metrics.rankSize;
      else if (uiState.rankMode === "low") rows = metrics.rankLow;
      else rows = metrics.rankBoth;

      const tbody = document.getElementById("rankingBody");
      tbody.innerHTML = rows.slice(0, 10).map(r => `
        <tr>
          <td>
            <div class="flex items-center gap-3">
              <div class="img-box">
                <img class="auto-img" data-product="${esc(r.product_code)}" alt="">
              </div>
              <div>
                <div class="font-black text-slate-900">${esc(r.product_name)}</div>
                <div class="muted">code: ${esc(r.product_code)}</div>
              </div>
            </div>
          </td>
          <td class="muted">${r.reviews}</td>
          <td><span class="badge size">${r.sizeRate}%</span></td>
          <td><span class="badge neg">${r.lowRate}%</span></td>
          <td class="muted">${esc(r.kwds || "-")}</td>
        </tr>
      `).join("");
    }

    function renderSizeStructure(metrics){
      document.getElementById("tooSmall").textContent = metrics.tooSmall;
      document.getElementById("tooBig").textContent = metrics.tooBig;

      const sizeBody = document.getElementById("sizeOptBody");
      sizeBody.innerHTML = metrics.sizeOpts.map(x => `
        <tr>
          <td class="font-black">${esc(x.sz)}</td>
          <td class="muted">${x.cnt}</td>
          <td><span class="badge size">${x.small}%</span></td>
          <td><span class="badge size">${x.big}%</span></td>
          <td class="muted">${x.ok}%</td>
        </tr>
      `).join("");

      const fit = document.getElementById("fitWords");
      fit.innerHTML = demo.fitWords.map(w => `<span class="badge">${esc(w)}</span>`).join("");
    }

    function renderKeywords(){
      const lift = document.getElementById("liftWords");
      lift.innerHTML = demo.liftWords.map(x => `
        <div class="flex items-center justify-between">
          <span class="badge neg">${esc(x.word)}</span>
          <span class="muted">lift ${x.lift.toFixed(1)}</span>
        </div>
      `).join("");

      const common = document.getElementById("commonIssues");
      common.innerHTML = demo.commonIssues.map(w => `<span class="badge neg">${esc(w)}</span>`).join("");

      const sizeP = document.getElementById("sizePhrases");
      sizeP.innerHTML = demo.sizePhrases.map(w => `<span class="badge size">${esc(w)}</span>`).join("");
    }

    function reviewCardHTML(r, tagMode){
      const tags = [];
      if (r.tags.includes("pos")) tags.push(`<span class="badge pos">#긍정키워드</span>`);
      if (r.tags.includes("size")) tags.push(`<span class="badge size">#size_issue</span>`);
      if (r.rating <= 2) tags.push(`<span class="badge neg">#low_rating</span>`);
      if (r.has_text_image) tags.push(`<span class="badge"><i class="fa-solid fa-image"></i> #100자+이미지</span>`);

      const autoTag = (tagMode === "size") ? `<span class="badge size">옵션: ${esc(r.option_size)}</span>`
                   : (tagMode === "low")  ? `<span class="badge neg">옵션: ${esc(r.option_size)}</span>`
                   : `<span class="badge">옵션: ${esc(r.option_size)}</span>`;

      return `
        <div class="review-card">
          <div class="flex items-start justify-between gap-3">
            <div class="flex items-center gap-3 min-w-0">
              <div class="img-box">
                <img class="auto-img" data-product="${esc(r.product_code)}" alt="">
              </div>
              <div class="min-w-0">
                <div class="font-black text-slate-900 line-clamp-1">${esc(r.product_name)}</div>
                <div class="text-xs font-bold text-slate-500 mt-1">code: ${esc(r.product_code)} · source: ${esc(r.source)}</div>
              </div>
            </div>
            <div class="text-right">
              <div class="text-xs font-black text-slate-700">★ ${r.rating}</div>
              <div class="text-[11px] font-bold text-slate-500 mt-1">${esc(fmtDT(r.created_at))}</div>
            </div>
          </div>

          <div class="mt-3 flex flex-wrap gap-2">
            ${autoTag}
            ${tags.join("")}
          </div>

          <div class="mt-3 text-sm font-extrabold text-slate-800 leading-relaxed">
            ${esc(r.text)}
          </div>

          <div class="mt-4">
            <div class="small-label text-blue-600 mb-2">100+ TEXT IMAGE</div>
            <div class="rounded-2xl border border-white/80 bg-white/60 p-3 flex items-center gap-3">
              <i class="fa-solid fa-image text-slate-400"></i>
              <div class="text-xs font-bold text-slate-500">
                ${r.has_text_image ? "텍스트 리뷰 캡처 이미지(데모)" : "해당 없음(데모)"}
              </div>
            </div>
          </div>
        </div>
      `;
    }

    function renderEvidence(reviews){
      const mode = uiState.evidenceMode;
      let list = reviews.slice();

      if (mode === "size") list = list.filter(r => r.tags.includes("size"));
      else if (mode === "low") list = list.filter(r => r.rating <= 2 || r.tags.includes("low"));
      else list = list.filter(r => r.tags.includes("req"));

      // 증거리뷰는 "설득력" 위해 길이/최근성 기준
      list.sort((a,b) => (b.text.length - a.text.length) || (new Date(b.created_at)-new Date(a.created_at)));

      const pick = list.slice(0, 6);
      const container = document.getElementById("evidenceList");
      container.innerHTML = pick.map(r => reviewCardHTML(r, mode)).join("");
    }

    function renderDailyFeed(reviews){
      const container = document.getElementById("dailyFeed");
      const no = document.getElementById("noResults");

      // 당일 업로드 순(기본): getFilteredReviews에서 upload 정렬 처리됨
      const rows = reviews.slice(0, 18); // 화면용 제한

      if (!rows.length){
        container.innerHTML = "";
        no.classList.remove("hidden");
        return;
      }
      no.classList.add("hidden");

      container.innerHTML = rows.map(r => reviewCardHTML(r, "daily")).join("");
    }

    function renderProductSelect(){
      const sel = document.getElementById("productSelect");
      if (!sel) return;

      const current = sel.value;
      const uniq = new Map();
      for (const p of demo.products) uniq.set(p.code, p.name);

      const options = [`<option value="">제품 선택 (전체)</option>`].concat(
        Array.from(uniq.entries()).map(([code,name]) => `<option value="${esc(code)}">${esc(name)} (${esc(code)})</option>`)
      ).join("");

      sel.innerHTML = options;
      // value 유지
      sel.value = current;
    }

    // ----------------------------
    // 6) Main render orchestrator
    // ----------------------------
    function renderAll(){
      const filtered = getFilteredReviews();
      const metrics = calcMetrics(filtered);

      renderHeader();
      renderProductSelect();
      renderSummary(metrics);
      renderRanking(metrics);
      renderSizeStructure(metrics);
      renderKeywords();
      renderEvidence(filtered);
      renderDailyFeed(filtered);

      // images
      applyAutoImages();
    }

    // ----------------------------
    // 7) Boot
    // ----------------------------
    document.addEventListener("DOMContentLoaded", () => {
      renderProductSelect();
      renderAll();
    });
  </script>
</body>
</html>
