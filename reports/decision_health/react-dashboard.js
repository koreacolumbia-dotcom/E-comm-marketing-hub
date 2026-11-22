/* Columbia source-backed React executive dashboard (React 18 + Tailwind).
   The existing analytic modules remain intact as an expandable fallback. */
(function () {
  "use strict";
  var mount = document.getElementById("csk-react-root");
  if (!mount || !window.React || !window.ReactDOM || !window.ReactDOM.createRoot) return;

  var R = window.React, h = R.createElement;
  var useState = R.useState, useEffect = R.useEffect, useMemo = R.useMemo;
  var num = function (x) { return typeof x === "number" && Number.isFinite(x); };
  var fmt = function (x) { return num(x) ? Math.round(x).toLocaleString("ko-KR") : "—"; };
  var krw = function (x) {
    if (!num(x)) return "—";
    var a = Math.abs(x), sign = x < 0 ? "−" : "";
    if (a >= 100000000) return sign + (a / 100000000).toFixed(2) + "억 원";
    if (a >= 10000) return sign + Math.round(a / 10000).toLocaleString("ko-KR") + "만 원";
    return sign + fmt(a) + "원";
  };
  var percent = function (now, before) { return num(now) && num(before) && before > 0 ? (now / before - 1) * 100 : null; };
  var delta = function (n, unit) { return num(n) ? (n > 0 ? "+" : "") + n.toFixed(unit === "pp" ? 2 : 1) + (unit === "pp" ? "%p" : "%") : "—"; };
  var dateRange = function (x) { return String(x || "").split(" ~ "); };
  var cls = function () { return Array.prototype.slice.call(arguments).filter(Boolean).join(" "); };
  var safeUrl = function (url) { return typeof url === "string" && /^https:\/\/[^<>"\s]+$/.test(url) ? url : null; };
  var attr = function (x) { return x == null ? "—" : String(x); };
  var finiteTotal = function (v) { return v && ["revenue","orders","sessions"].every(function (k) { return num(v[k]) && v[k] > 0; }) && v.orders <= v.sessions; };
  var permutations = [[0,1,2],[0,2,1],[1,0,2],[1,2,0],[2,0,1],[2,1,0]];
  function driverBridge(ops) {
    var w = ops && ops.weekly, p = w && w.previous && w.previous.total, c = w && w.current && w.current.total;
    if (!w || w.status !== "ready" || !finiteTotal(p) || !finiteTotal(c) ||
      w.current.status !== "complete" || w.previous.status !== "complete" ||
      dateRange(w.period)[1] !== ops.report_date) return [];
    var start = [p.sessions, p.orders / p.sessions, p.revenue / p.orders];
    var end = [c.sessions, c.orders / c.sessions, c.revenue / c.orders];
    var totals = [0,0,0];
    var multiply = function (arr) { return arr[0] * arr[1] * arr[2]; };
    permutations.forEach(function (perm) {
      var values = start.slice();
      perm.forEach(function (i) { var before = multiply(values); values[i] = end[i]; totals[i] += (multiply(values) - before) / 6; });
    });
    if (Math.abs(totals.reduce(function (a,b) { return a+b; },0) - (c.revenue-p.revenue)) > 1) return [];
    return [
      {id:"traffic",name:"유입 규모",impact:totals[0],before:p.sessions,after:c.sessions,
       metric:"세션",unit:"건",action:"소스/매체별 방문 증감과 구매 기여 흐름 확인",explain:"세션 증감에 따른 매출 산술 기여",target:"acquisition"},
      {id:"conversion",name:"구매전환 효율",impact:totals[1],before:p.orders/p.sessions*100,after:c.orders/c.sessions*100,
       metric:"CVR",unit:"%",action:"세션 증가 대비 구매전환이 약한 매체를 검토",explain:"세션당 주문 전환율 변동에 따른 산술 기여",target:"acquisition"},
      {id:"aov",name:"객단가·구매 구성",impact:totals[2],before:p.revenue/p.orders,after:c.revenue/c.orders,
       metric:"객단가",unit:"원",action:"주문당 상품수·상품군·실현매출 가설을 추가 대조",explain:"주문당 매출 변동에 따른 산술 기여",target:"products"}
    ].sort(function (a,b) { return (a.impact >= 0) - (b.impact >= 0) || Math.abs(b.impact)-Math.abs(a.impact); });
  }
  function sourceGroup(name) {
    var txt = String(name || "").toLowerCase();
    if (/lms|edm|crm|kakao_ch|alimtalk|sms|email|push|friendstalk/.test(txt)) return "owned";
    if (/cpc|ppc|cpm|display|banner|retarget|pmax|paid|meta_bd|gfa/.test(txt)) return "paid";
    if (/organic|referral|direct|none|social/.test(txt)) return "organic";
    return "other";
  }
  function dailySeries(ops,di,key) {
    var w=ops&&ops.weekly||{},parts=dateRange(w.period),record=(di&&di.sections||[]).find(function(x){return x.number===4;})||{};
    var allDates=record.dates||[],numbers=record[key]||[],target=w.current&&w.current.total&&w.current.total[key];
    if(parts.length!==2 || !num(target) || target<=0 || allDates.length!==numbers.length)return null;
    var values=[];
    for(var i=0;i<allDates.length;i++)if(allDates[i]>=parts[0]&&allDates[i]<=parts[1]){
      if(!num(numbers[i])||numbers[i]<0)return null;values.push(numbers[i]);
    }
    if(values.length!==7)return null;
    var total=values.reduce(function(a,b){return a+b;},0);
    return Math.abs(total-target)/target<=0.005?values:null;
  }
  function metricRows(ops,di) {
    var w=ops && ops.weekly, a=w && w.current && w.current.total, b=w && w.previous && w.previous.total;
    if (!w || w.status !== "ready" || !finiteTotal(a) || !finiteTotal(b)) return [];
    return [
      {key:"revenue",label:"매출",value:a.revenue,prior:b.revenue,display:krw(a.revenue),before:krw(b.revenue),icon:"₩",series:dailySeries(ops,di,"revenue"),trend:"bar"},
      {key:"orders",label:"주문",value:a.orders,prior:b.orders,display:fmt(a.orders)+"건",before:fmt(b.orders)+"건",icon:"□"},
      {key:"sessions",label:"세션",value:a.sessions,prior:b.sessions,display:fmt(a.sessions),before:fmt(b.sessions),icon:"↗",series:dailySeries(ops,di,"sessions"),trend:"line"},
      {key:"aov",label:"객단가",value:a.revenue/a.orders,prior:b.revenue/b.orders,display:fmt(a.revenue/a.orders)+"원",before:fmt(b.revenue/b.orders)+"원",icon:"◇"}
    ];
  }
  function alignedSource(source,ops) {
    var w=ops && ops.weekly || {},p=source && source.period || {},a=p.current || {},b=p.previous || {};
    var wc=dateRange(w.period),wp=dateRange(w.comparison_period);
    return source && source.status === "ga4_measured" && source.report_date === ops.report_date &&
      wc.length === 2 && wp.length === 2 && a.start === wc[0] && a.end === wc[1] &&
      b.start === wp[0] && b.end === wp[1];
  }
  function officialEvents(dx,ops) {
    var p=dateRange(ops && ops.weekly && ops.weekly.period);
    var start=p[0],end=p[1],rows=dx && dx.market && dx.market.official_event_overlap || [];
    return Array.isArray(rows) && start && end ? rows.filter(function (x) {
      return x && x.start && x.end && x.start <= end && x.end >= start;
    }).sort(function (a,b) { return String(b.start).localeCompare(String(a.start)); }) : [];
  }
  function productRows(di,officialImages) {
    var c=di && di.product_focus && di.product_focus.cards || [];
    var normalizeName=function(v){return String(v||"").replace(/[‘’`]/g,"'").replace(/\s+/g," ").trim().toLowerCase();};
    var byName=new Map((officialImages&&officialImages.products||[]).filter(function(v){
      return v&&v.name&&/^https:\/\/assetscdn\d+\.styleship\.com\/assets\/columbia\/data\/productimages\/a\/[12]\/[a-z0-9_-]+\.jpg$/i.test(v.image_url||"") &&
        /^https:\/\/www\.columbiakorea\.co\.kr\//i.test(v.product_url||"");
    }).map(function(v){return [normalizeName(v.name),v];}));
    return Array.isArray(c) ? c.filter(function (x) {
      return x && x.name && num(x.pdp_7d) && num(x.pdp_prior_7d) &&
        num(x.sales_7d) && num(x.sales_previous_7d) &&
        x.sales_source === "ga4_purchase_items" && x.sales_match_method === "normalized_exact_item_name";
    }).map(function(x){var p=byName.get(normalizeName(x.name));return {
      ...x,image_url:p?p.image_url:null,product_url:p?p.product_url:null,
      representative_color:p?p.color:null
    };}).sort(function (a,b) {
      var aDelta=Math.abs(a.sales_7d-a.sales_previous_7d),bDelta=Math.abs(b.sales_7d-b.sales_previous_7d);
      return bDelta-aDelta || Math.abs(b.pdp_change_pct||0)-Math.abs(a.pdp_change_pct||0);
    }) : [];
  }
  var tabs = [
    {id:"overview",label:"전체 현황"},
    {id:"acquisition",label:"유입·전환"},
    {id:"products",label:"상품"},
    {id:"market",label:"경쟁사"}
  ];

  function DailyMiniChart(props) {
    var values=props.values;
    if(!Array.isArray(values)||values.length!==7)return null;
    var max=Math.max.apply(null,values.concat([1]));
    var min=Math.min.apply(null,values);
    var span=Math.max(1,max-min);
    var points=values.map(function(v,i){return (10+i*36)+","+(50-40*(v-min)/span).toFixed(2);}).join(" ");
    return h("div",{className:"csk-mini-trend",title:"동일 기간 실제 일별 "+props.label+" 집계"},
      h("svg",{viewBox:"0 0 236 58",preserveAspectRatio:"none",role:"img","aria-label":"최근 7일 "+props.label+" 일별 추이"},
        props.type==="bar"?values.map(function(v,i){
          var height=Math.max(4,44*v/max);
          return h("rect",{key:i,x:12+i*34,y:52-height,width:19,height:height,rx:4,fill:i===6?"#5e54f3":"#abb1fa"});
        }):h("polyline",{points:points,fill:"none",stroke:"#0072ce",strokeWidth:"3",strokeLinecap:"round",strokeLinejoin:"round"})
      ),
      h("span",null,"최근 7일 일별 "+props.label+" 추이")
    );
  }
  function ComparisonTrack(props) {
    var max=Math.max(props.now||0,props.prior||0,1);
    return h("div",{className:"csk-comparison","aria-label":"직전 7일과 최근 7일 비교"},
      h("div",{className:"compare-row"},h("span",null,"직전 7일"),h("div",{className:"compare-rail"},h("div",{className:"compare-fill prior",style:{width:(Math.max(0,props.prior)/max*100)+"%"}}))),
      h("div",{className:"compare-row"},h("span",null,"최근 7일"),h("div",{className:"compare-rail"},h("div",{className:"compare-fill now",style:{width:(Math.max(0,props.now)/max*100)+"%"}})))
    );
  }
  function KpiCard(props) {
    var m=props.item,p=percent(m.value,m.prior);
    return h("article",{className:"csk-kpi-card rounded-2xl shadow-sm min-w-0"},
      h("div",{className:"kpi-label-row"},h("span",{className:"kpi-label"},m.label),h("span",{className:"kpi-icon","aria-hidden":"true"},m.icon)),
      h("strong",{className:"kpi-big-number",title:fmt(m.value)+(m.key==="revenue" || m.key==="aov"?"원":"")},m.display),
      h("div",{className:"kpi-change"},h("span",{className:cls("delta-pill",p>0?"up":p<0?"down":"flat")},delta(p)),h("span",null,"전주 대비")),
      m.series?h(DailyMiniChart,{values:m.series,type:m.trend,label:m.label}):h(ComparisonTrack,{prior:m.prior,now:m.value}),
      h("p",{className:"kpi-previous"},"전주 ",m.before)
    );
  }
  function StatCard(props) {
    return h("div",{className:"csk-detail-stat"},
      h("span",null,props.title),h("strong",null,props.value),
      props.sub ? h("small",null,props.sub):null
    );
  }
  function SourcePills(props) {
    return h("div",{className:"csk-filter-pills"},
      [["all","전체"],["paid","유료"],["organic","오가닉·직접"],["owned","CRM"],["other","기타"]].map(function (x) {
        return h("button",{key:x[0],type:"button",className:cls("filter-pill",props.value===x[0]&&"active"),
          onClick:function () { props.onChange(x[0]); },"aria-pressed":props.value===x[0]},x[1]);
      })
    );
  }
  function PeriodWarning(props) {
    return h("div",{className:"csk-period-warning",role:"status"},
      h("strong",null,"기간 불일치"),
      h("span",null,"이 소스/매체 분석은 ",props.current," 기준으로, 전체 매출 분석(",props.operation,")과 다릅니다. 두 결과를 동일 기간의 매출 기여로 연결하지 않습니다.")
    );
  }
  function ActionPanel(props) {
    return h("div",{className:"csk-action-panel"},
      h("span",null,props.label || "다음 실행"),
      h("p",null,props.text)
    );
  }
  function EmptyState(props) {
    return h("div",{className:"csk-empty"},
      h("strong",null,props.title || "표시할 근거가 없습니다."),
      h("p",null,props.note || "데이터가 수집되면 실제 관측값만 표시합니다.")
    );
  }
  function CauseWorkspace(props) {
    var causes=props.causes,selected=causes.find(function (x) { return x.id===props.selected; }) || causes[0];
    return h("div",{className:"csk-workspace-grid"},
      h("aside",{className:"csk-workspace-list"},
        causes.length ? causes.map(function (d,i) {
          return h("button",{key:d.id,type:"button",className:cls("workspace-row",selected && selected.id===d.id && "selected"),onClick:function(){props.onSelect(d.id);}},
            h("span",{className:"workspace-row-number"},"0"+(i+1)),
            h("span",{className:"workspace-row-copy"},
              h("strong",null,d.name),
              h("small",null,d.explain)
            ),
            h("span",{className:cls("row-number",d.impact<0?"minus":"plus")},(d.impact>0?"+":"")+krw(d.impact))
          );
        }) : h(EmptyState,{title:"매출 원인 산술분해 미확보",note:"동일한 두 7일 기간의 매출·주문·세션 데이터가 필요합니다."})
      ),
      h("section",{className:"csk-workspace-detail"},
        selected ? [
          h("div",{className:"detail-topline",key:"top"},h("span",null,"원인 기여 분석"),h("span",{className:"detail-badge"},"산술분해 · 인과 확정 아님")),
          h("h3",{key:"title"},selected.name),
          h("strong",{key:"impact",className:"detail-large"},(selected.impact>0?"+":"")+krw(selected.impact)),
          h("p",{key:"desc",className:"detail-sub"},"전체 매출 변화에 대한 ",selected.metric," 요인의 산술 기여 금액입니다."),
          h("div",{key:"stats",className:"csk-detail-stats"},
            h(StatCard,{title:"직전 7일 "+selected.metric,value:selected.metric==="객단가"?fmt(selected.before)+"원":selected.metric==="CVR"?selected.before.toFixed(2)+"%":fmt(selected.before)}),
            h(StatCard,{title:"최근 7일 "+selected.metric,value:selected.metric==="객단가"?fmt(selected.after)+"원":selected.metric==="CVR"?selected.after.toFixed(2)+"%":fmt(selected.after)}),
            h(StatCard,{title:"변동",value:selected.metric==="CVR"?delta(selected.after-selected.before,"pp"):delta(percent(selected.after,selected.before))})
          ),
          h(ActionPanel,{key:"action",text:selected.action}),
          h("button",{type:"button",key:"go",className:"csk-detail-go",onClick:function(){props.onTab(selected.target);}},"관련 상세 데이터 보기 →")
        ] : h(EmptyState,{title:"분석 기준일 확인 필요"})
      )
    );
  }
  function SourceWorkspace(props) {
    var raw=props.source,rows=Array.isArray(raw && raw.rows)?raw.rows:[];
    var list=rows.filter(function (x) {
      return (props.group==="all" || sourceGroup(x.source_medium)===props.group) &&
        String(x.source_medium || "").toLowerCase().indexOf(props.search.toLowerCase())!==-1;
    }).sort(function (a,b) {
      if(props.sort==="sessions")return (b.current && b.current.sessions||0)-(a.current && a.current.sessions||0);
      if(props.sort==="revenue")return (b.current && b.current.revenue||0)-(a.current && a.current.revenue||0);
      if(props.sort==="conversion")return (b.current && b.current.cvr_pct||0)-(a.current && a.current.cvr_pct||0);
      return a.revenue_delta-b.revenue_delta;
    });
    var selected=list.find(function (x) { return x.source_medium===props.selected; }) || list[0];
    var current=raw && raw.period && raw.period.current || {},prior=raw && raw.period && raw.period.previous || {};
    return h("div",null,
      h("div",{className:"csk-workspace-controls"},
        h(SourcePills,{value:props.group,onChange:props.onGroup}),
        h("div",{className:"csk-filter-right"},
          h("select",{className:"csk-sort",value:props.sort,onChange:function(e){props.onSort(e.target.value);},"aria-label":"소스/매체 정렬"},
            [["loss","매출 하락순"],["sessions","세션순"],["revenue","매출순"],["conversion","CVR순"]].map(function(opt){return h("option",{key:opt[0],value:opt[0]},opt[1]);})
          ),
          h("input",{type:"search",className:"csk-search",value:props.search,onChange:function(e){props.onSearch(e.target.value);},placeholder:"소스/매체 검색","aria-label":"소스/매체 검색"})
        )
      ),
      !alignedSource(raw,props.ops) ? h(PeriodWarning,{current:attr(current.start)+" ~ "+attr(current.end),operation:props.ops && props.ops.weekly && props.ops.weekly.period || "미확보"}) : null,
      h("div",{className:"csk-workspace-grid"},
        h("aside",{className:"csk-workspace-list"},
          h("p",{className:"list-meta"},props.sort==="loss"?"매출 감소순":props.sort==="sessions"?"세션순":props.sort==="revenue"?"매출순":"CVR순"," · ",fmt(list.length),"개 소스/매체"),
          list.length ? list.slice(0,props.showAll?list.length:12).map(function(x,i) {
            var chosen=selected && selected.source_medium===x.source_medium;
            return h("button",{key:x.source_medium,type:"button",className:cls("workspace-row",chosen&&"selected"),onClick:function(){props.onSelect(x.source_medium);}},
              h("span",{className:"workspace-row-number"},""+(i+1)),
              h("span",{className:"workspace-row-copy"},h("strong",null,x.source_medium),h("small",null,"세션 "+fmt(x.current && x.current.sessions)+" · CVR "+(num(x.current && x.current.cvr_pct)?x.current.cvr_pct.toFixed(2)+"%":"—"))),
              h("span",{className:cls("row-number",x.revenue_delta<0?"minus":"plus")},(x.revenue_delta>0?"+":"")+krw(x.revenue_delta))
            );
          }) : h(EmptyState,{title:"조건에 맞는 소스/매체가 없습니다."}),
          list.length>12?h("button",{type:"button",className:"csk-show-all",onClick:props.onToggleAll},props.showAll?"접기":"전체 "+fmt(list.length)+"개 보기"):null
        ),
        h("section",{className:"csk-workspace-detail"},
          selected?[
            h("div",{className:"detail-topline",key:"top"},h("span",null,"소스/매체 상세"),h("span",{className:"detail-badge"},"GA4 세션 귀속")),
            h("h3",{key:"title"},selected.source_medium),
            h("strong",{key:"value",className:"detail-large"},krw(selected.current && selected.current.revenue)),
            h("p",{key:"period",className:"detail-sub"},"관측 기간 "+attr(current.start)+" ~ "+attr(current.end)+" · 전주 "+attr(prior.start)+" ~ "+attr(prior.end)),
            h("div",{key:"stats",className:"csk-detail-stats"},
              h(StatCard,{title:"세션",value:fmt(selected.current.sessions),sub:delta(percent(selected.current.sessions,selected.previous.sessions))+" 전주 대비"}),
              h(StatCard,{title:"구매",value:fmt(selected.current.transactions)+"건",sub:(selected.transactions_delta>0?"+":"")+fmt(selected.transactions_delta)+"건"}),
              h(StatCard,{title:"CVR",value:num(selected.current.cvr_pct)?selected.current.cvr_pct.toFixed(2)+"%":"—",sub:delta(selected.cvr_change_pp,"pp")})
            ),
            h(ActionPanel,{key:"action",label:"다음 확인",text:"세션 변화와 구매전환율 변화를 함께 점검하세요. 실제 광고비/증분 ROAS를 의미하지 않습니다."})
          ]:h(EmptyState,{title:"소스/매체 데이터 미확보"})
        )
      )
    );
  }
  function ProductWorkspace(props) {
    var allProducts=props.products;
    var families=Array.from(new Set(allProducts.map(function(x){return x.family;}).filter(Boolean))).sort();
    var products=allProducts.filter(function(x){
      return (props.family==="all" || x.family===props.family) &&
        String(x.name||"").toLowerCase().indexOf(props.search.toLowerCase())!==-1;
    });
    var selected=products.find(function (x) { return x.name===props.selected; })||products[0];
    var funnel=props.funnel;
    var aligned=funnel && funnel.status==="ga4_measured" && funnel.report_date===props.ops.report_date;
    var f=aligned && selected && Array.isArray(funnel.products)?funnel.products.find(function (x) {return x.name===selected.name && x.coverage_reliable===true;}):null;
    return h("div",null,
      props.focusDate!==props.ops.report_date?h("div",{className:"csk-period-warning",role:"status"},
        h("strong",null,"상품 분석 기준일 차이"),
        h("span",null,"PDP·판매 기준 ",attr(props.focusDate)," · 운영 KPI 기준 ",attr(props.ops.report_date),
          " · 두 기간의 합계와 기여액을 직접 연결하지 않습니다.")):null,
      h("div",{className:"csk-workspace-controls"},
        h("div",{className:"csk-filter-pills"},
          [["all","전체"]].concat(families.map(function(x){return [x,x];})).map(function(opt){
            return h("button",{type:"button",key:opt[0],
              className:cls("filter-pill",props.family===opt[0]&&"active"),
              "aria-pressed":props.family===opt[0],
              onClick:function(){props.onFamily(opt[0]);}},opt[1]);
          })
        ),
        h("input",{type:"search",className:"csk-search",value:props.search,
          onChange:function(e){props.onSearch(e.target.value);},
          placeholder:"상품명 검색","aria-label":"상품명 검색"})
      ),
      h("div",{className:"csk-workspace-grid"},
      h("aside",{className:"csk-workspace-list"},
        h("p",{className:"list-meta"},"PDP 조회·GA4 판매 비교 가능 ",fmt(products.length),"종"),
        products.length?products.slice(0,props.showAll?products.length:12).map(function(x,i){
          var saleDelta=x.sales_7d-x.sales_previous_7d;
          return h("button",{key:x.name,type:"button",className:cls("workspace-row",selected&&selected.name===x.name&&"selected"),onClick:function(){props.onSelect(x.name);}},
            x.image_url ? h("img",{className:"csk-product-thumb",src:x.image_url,alt:x.name+" 대표 색상 "+attr(x.representative_color),loading:"lazy",referrerPolicy:"no-referrer"}):
              h("span",{className:"workspace-row-number"},""+(i+1)),
            h("span",{className:"workspace-row-copy"},h("strong",null,x.name),h("small",null,"PDP "+delta(x.pdp_change_pct)+" · 구매 "+fmt(x.sales_previous_7d)+" → "+fmt(x.sales_7d)+"개")),
            h("span",{className:cls("row-number",saleDelta<0?"minus":"plus")},(saleDelta>0?"+":"")+fmt(saleDelta)+"개")
          );
        }):h(EmptyState,{title:"PDP·판매 매칭 자료가 없습니다."}),
        products.length>12?h("button",{type:"button",className:"csk-show-all",onClick:props.onToggleAll},props.showAll?"접기":"전체 "+fmt(products.length)+"개 보기"):null
      ),
      h("section",{className:"csk-workspace-detail"},
        selected?[
          h("div",{className:"detail-topline",key:"top"},h("span",null,"상품명 기준 분석"),h("span",{className:"detail-badge"},"GA4 매칭 · 인과 미확정")),
          h("div",{key:"product-head",className:"csk-product-head"},
            selected.image_url?h("img",{src:selected.image_url,alt:selected.name+" "+attr(selected.representative_color)+" 대표 이미지",className:"csk-product-hero-image",loading:"lazy",referrerPolicy:"no-referrer"}):null,
            h("div",{className:"csk-product-head-info"},
              h("h3",null,selected.name),
              h("strong",{className:"detail-large"},delta(selected.pdp_change_pct)),
              selected.product_url?h("a",{className:"csk-product-official",href:selected.product_url,target:"_blank",rel:"noopener noreferrer"},"공식몰 · 대표 색상 "+attr(selected.representative_color)+" ↗"):null
            )
          ),
          h("p",{key:"meta",className:"detail-sub"},"PDP 조회 변화 · 판매는 GA4 상품 구매수량으로 별도 집계"),
          h("div",{key:"stats",className:"csk-detail-stats"},
            h(StatCard,{title:"PDP 조회",value:fmt(selected.pdp_7d),sub:"전주 "+fmt(selected.pdp_prior_7d)}),
            h(StatCard,{title:"GA4 판매수량",value:fmt(selected.sales_7d)+"개",sub:"전주 "+fmt(selected.sales_previous_7d)+"개"}),
            h(StatCard,{title:"조회→장바구니",value:f&&num(f.current.view_to_cart_pct)?f.current.view_to_cart_pct.toFixed(2)+"%":"미확보",sub:f?"동일 GA4 세션 퍼널":"퍼널 미검증"})
          ),
          h(ActionPanel,{key:"action",text:f?"유입 소스 구성·조회→장바구니·장바구니→구매 완료 흐름을 비교하세요.":"일치하는 세션 퍼널 확보 후 이탈 단계 분석을 진행하세요."}),
          h("p",{key:"disclaimer",className:"detail-footer"},"GA4 상품명 매칭 표본 · PDP 조회 대비 판매 단순비는 CVR이 아닙니다.")
        ]:h(EmptyState,{title:"검증 가능한 상품 상세 데이터가 없습니다."})
      )
      )
    );
  }
  function EventWorkspace(props) {
    var events=props.events;
    var eventKey=function(x){return x.brand+"|"+x.start+"|"+x.title;};
    var selected=events.find(function(x){return eventKey(x)===props.selected;})||events[0];
    var w=props.ops && props.ops.weekly || {};
    return h("div",{className:"csk-workspace-grid"},
      h("aside",{className:"csk-workspace-list"},
        h("p",{className:"list-meta"},"공식 기간 확인된 행사 · "+attr(w.period)),
        events.length?events.slice(0,12).map(function(x,i){
          return h("button",{key:x.brand+"-"+x.start+"-"+i,type:"button",className:cls("workspace-row",selected===x&&"selected"),onClick:function(){props.onSelect(eventKey(x));}},
            h("span",{className:"workspace-row-number"},""+(i+1)),
            h("span",{className:"workspace-row-copy"},h("strong",null,x.brand),h("small",null,x.title)),
            h("span",{className:"row-number"},"행사")
          );
        }):h(EmptyState,{title:"동일 기간에 확인된 공식 행사가 없습니다."})
      ),
      h("section",{className:"csk-workspace-detail"},
        selected?[
          h("div",{className:"detail-topline",key:"top"},h("span",null,"경쟁사 공식 행사"),h("span",{className:"detail-badge"},"일정 확인 · 인과 미확정")),
          h("h3",{key:"title"},selected.brand),
          h("strong",{key:"value",className:"detail-large event-title"},selected.title),
          h("div",{key:"stats",className:"csk-detail-stats"},
            h(StatCard,{title:"시작",value:selected.start}),
            h(StatCard,{title:"종료",value:selected.end}),
            h(StatCard,{title:"근거",value:"공식 기간",sub:"관측 시기 일치만 확인"})
          ),
          h(ActionPanel,{key:"action",text:"동일 기간의 경쟁사 행사 정보입니다. 컬럼비아의 구매 전환이나 매출에 미친 효과는 확인되지 않았습니다."}),
          safeUrl(selected.url)?h("a",{key:"link",className:"csk-detail-go",href:selected.url,target:"_blank",rel:"noopener noreferrer"},"공식 행사 근거 열기 ↗"):null
        ]:h(EmptyState,{title:"공식 기간 확인 행사 미확보"})
      )
    );
  }

  function ContextCard(props) {
    return h("article",{className:"csk-context-card rounded-2xl"},
      h("span",{className:"context-eyebrow"},props.eyebrow),
      h("strong",null,props.title),
      h("p",null,props.note),
      props.link?h("a",{href:props.link,target:props.external?"_blank":undefined,rel:props.external?"noopener noreferrer":undefined},props.linkLabel||"상세 보기 →"):null
    );
  }
  function ContextCards(props) {
    var di=props.di||{},ops=props.ops||{},dx=props.dx||{},events=props.events||[],products=props.products||[];
    var weather=(di.sections||[]).find(function(s){return s.number===4;})||{};
    var f=(weather.forecast||[]).find(function(x){return x.date>ops.report_date && num(x.min_temp)&&num(x.max_temp);});
    var forecast=f?f.date+" · 서울 최저 "+Math.round(f.min_temp)+"° / 최고 "+Math.round(f.max_temp)+"°":"서울 기온 관측·예보 미확보";
    var calendar=ops.calendar && Array.isArray(ops.calendar.current)?ops.calendar.current:[];
    var cal=calendar.filter(function(x){return x && x.title;}).slice(0,1);
    var sale=products[0],saleText=sale?sale.name+" · PDP "+delta(sale.pdp_change_pct):"상품명 기준 비교 대상 미확보";
    return h("section",{className:"csk-context-section"},
      h("div",{className:"csk-section-head"},h("h2",null,"관련 신호"),h("span",null,"관측 근거 · 인과관계 별도 검증")),
      h("div",{className:"csk-context-grid grid"},
        h(ContextCard,{eyebrow:"WEATHER",title:"서울 기온",note:forecast,link:"../daily_intelligence/index.html",linkLabel:"기온 상세"}),
        h(ContextCard,{eyebrow:"PDP · SALES",title:"주요 상품 동향",note:saleText,link:"../daily_intelligence/index.html",linkLabel:"상품 상세"}),
        h(ContextCard,{eyebrow:"COLUMBIA",title:"브랜드 일정",note:cal.length?cal[0].title:"공식 일정 데이터 미연동",link:"../marketing_operations/index.html",linkLabel:"운영 정보"}),
        h(ContextCard,{eyebrow:"MARKET",title:"경쟁사 행사 "+fmt(events.length)+"건",note:events.length?events[0].brand+" · "+events[0].title:"동일 기간 공식 기간 확인 자료 없음",link:"../competitor_sale_calendar/index.html",linkLabel:"행사 캘린더"})
      )
    );
  }
  function SourcePreview(props) {
    var data=props.source||{},rows=Array.isArray(data.rows)?data.rows:[];
    var selected=rows.filter(function(x){return x&&x.previous&&x.current&&num(x.revenue_delta);})
      .sort(function(a,b){return a.revenue_delta-b.revenue_delta;}).slice(0,4);
    return h("section",{className:"csk-bottom-records"},
      h("div",{className:"csk-section-head"},h("h2",null,"소스/매체별 매출 감소 상위"),h("button",{type:"button",onClick:function(){props.onTab("acquisition");}},"전체 소스·매체 보기 →")),
      selected.length?h("div",{className:"csk-records-list"},
        selected.map(function(x){
          return h("div",{key:x.source_medium,className:"csk-record-row"},
            h("strong",null,x.source_medium),
            h("span",null,"세션 "+fmt(x.current.sessions)),
            h("span",null,"CVR "+(num(x.current.cvr_pct)?x.current.cvr_pct.toFixed(2)+"%":"—")),
            h("b",{className:cls(x.revenue_delta<0?"negative":"positive")},(x.revenue_delta>0?"+":"")+krw(x.revenue_delta))
          );
        })
      ):h(EmptyState,{title:"소스/매체 집계 미확보"}),
      !alignedSource(data,props.ops)?h("small",{className:"csk-quiet-note"},"주의: 소스/매체 기간과 운영 매출 기간이 다릅니다. 값은 참고용입니다."):null
    );
  }

  function Dashboard(props) {
    var data=props.data,ops=data.ops,di=data.di||{},dx=data.dx||{},source=data.source||{},funnel=data.funnel||{};
    var [tab,setTab]=useState("overview");
    var [causeId,setCause]=useState(null);
    var [group,setGroup]=useState("all");
    var [sourceSort,setSourceSort]=useState("loss");
    var [productFamily,setProductFamily]=useState("all");
    var [productSearch,setProductSearch]=useState("");
    var [showAllProducts,setShowAllProducts]=useState(false);
    var [search,setSearch]=useState("");
    var [selectedSource,setSource]=useState(null);
    var [selectedProduct,setProduct]=useState(null);
    var [selectedEvent,setEvent]=useState(null);
    var [showAll,setShowAll]=useState(false);
    var [showLegacy,setLegacy]=useState(false);
    var drivers=useMemo(function(){return driverBridge(ops);},[ops]);
    var metrics=useMemo(function(){return metricRows(ops,di);},[ops,di]);
    var products=useMemo(function(){return productRows(di,data.pictures);},[di,data.pictures]);
    var events=useMemo(function(){return officialEvents(dx,ops);},[dx,ops]);
    useEffect(function(){
      document.body.classList.add("csk-react-ready");
      return function(){document.body.classList.remove("csk-react-ready");};
    },[]);
    useEffect(function(){
      document.body.classList.toggle("csk-show-legacy",showLegacy);
      return function(){document.body.classList.remove("csk-show-legacy");};
    },[showLegacy]);
    var title=tab==="overview"?"매출 변화의 핵심 원인":tab==="acquisition"?"유입부터 구매전환까지":tab==="products"?"상품별 PDP·구매 흐름":"경쟁사 공식 행사";
    var subtitle=tab==="overview"?"매출 영향액 기준 · 전주 대비 산술분해":tab==="acquisition"?"실제 GA4 소스/매체 집계 · 기간 분리":tab==="products"?"상품명 기준 조회·판매 변화": "공식 기간이 확인된 프로모션만 표시";
    var count=tab==="overview"?drivers.length:tab==="acquisition"?(source.rows||[]).length:tab==="products"?products.length:events.length;
    return h("div",{className:"csk-react-dashboard w-full mx-auto"},
      h("div",{className:"csk-appbar items-center gap-4"},
        h("div",{className:"csk-appbrand"},
          h("span",{className:"csk-brand-mark","aria-hidden":"true"},"◆"),
          h("div",null,h("strong",null,"COLUMBIA"),h("small",null,"E-COMM INTELLIGENCE"))
        ),
        h("nav",{className:"csk-top-tabs flex items-center","aria-label":"분석 분류"},
          tabs.map(function(t){return h("button",{key:t.id,type:"button",className:cls("csk-top-tab",tab===t.id&&"active"),
            "aria-current":tab===t.id?"page":undefined,onClick:function(){setTab(t.id);}},t.label);})
        ),
        h("div",{className:"csk-report-day"},h("span",null,"분석 기준"),h("strong",null,attr(ops.report_date)))
      ),
      h("div",{className:"csk-hero-heading"},
        h("div",null,h("span",{className:"hero-eyebrow"},"CSK / PERFORMANCE INTELLIGENCE"),h("h1",null,"이커머스 성과 인텔리전스"),
          h("p",null,attr(ops.weekly && ops.weekly.period)," · 직전 7일 ",attr(ops.weekly && ops.weekly.comparison_period))),
        h("a",{className:"csk-header-link",href:"../daily_intelligence/index.html"},"전체 리포트 보기 ↗")
      ),
      h("section",{className:"csk-kpi-grid grid","aria-label":"실제 운영 주간 KPI"},
        metrics.length?metrics.map(function(m){return h(KpiCard,{key:m.key,item:m});}):
          h(EmptyState,{title:"운영 KPI 근거 없음",note:"완전한 주간 세션·주문·매출 합계를 확인할 수 없습니다."})
      ),
      h("div",{className:"csk-workspace-heading"},
        h("div",null,h("span",null,"EXECUTIVE WORKSPACE"),h("h2",null,title),h("p",null,subtitle)),
        h("span",{className:"csk-workspace-count"},fmt(count)+"개 항목")
      ),
      h("section",{className:"csk-workspace rounded-2xl","aria-label":"선택한 의사결정 분석"},
        h("div",{className:"csk-workspace-toolbar"},h("strong",null,title),h("span",null,attr(ops.report_date)," 기준")),
        tab==="overview"?h(CauseWorkspace,{causes:drivers,selected:causeId,onSelect:setCause,onTab:setTab}):
        tab==="acquisition"?h(SourceWorkspace,{source:source,ops:ops,group:group,onGroup:setGroup,sort:sourceSort,onSort:setSourceSort,search:search,onSearch:setSearch,selected:selectedSource,onSelect:setSource,showAll:showAll,onToggleAll:function(){setShowAll(!showAll);}}):
        tab==="products"?h(ProductWorkspace,{products:products,funnel:funnel,ops:ops,focusDate:di.product_focus&&di.product_focus.date,selected:selectedProduct,onSelect:setProduct,family:productFamily,onFamily:setProductFamily,search:productSearch,onSearch:setProductSearch,showAll:showAllProducts,onToggleAll:function(){setShowAllProducts(!showAllProducts);}}):
        h(EventWorkspace,{events:events,ops:ops,selected:selectedEvent,onSelect:setEvent})
      ),
      h(ContextCards,{di:di,ops:ops,dx:dx,events:events,products:products}),
      tab==="overview"?h(SourcePreview,{source:source,ops:ops,onTab:setTab}):null,
      h("div",{className:"csk-footer-actions"},
        h("button",{type:"button",className:"csk-legacy-toggle",onClick:function(){setLegacy(!showLegacy);}},showLegacy?"기존 상세 분석 접기 −":"기존 상세 분석·검증 근거 펼치기 +"),
        h("span",null,"수치와 가설 구분 · 기존 데이터 자동 갱신 · 임의 데이터 없음")
      )
    );
  }

  function boot() {
    var sources={
      ops:"../marketing_operations/latest.json",
      di:"../daily_intelligence/latest.json",
      dx:"./diagnoses.json",
      source:"./source_medium_weekly.json",
      funnel:"./product_funnel_evidence.json",
      pictures:"./official_product_images.json"
    };
    var keys=Object.keys(sources),token=String(Date.now());
    Promise.all(keys.map(function(k){
      return fetch(sources[k]+"?v="+token,{cache:"no-store"}).then(function(res){
        if(!res.ok) throw new Error(k+" HTTP "+res.status);
        return res.json();
      }).then(function(value){return [k,value];}).catch(function(){return [k,null];});
    })).then(function(results){
      var data={};results.forEach(function(row){data[row[0]]=row[1];});
      if(!data.ops || !data.ops.weekly || !data.ops.weekly.current || !data.ops.weekly.previous) return;
      var root=window.ReactDOM.createRoot(mount);
      root.render(h(Dashboard,{data:data}));
    }).catch(function(){/* keep legacy UI as the accessible fallback */});
  }
  boot();
})();