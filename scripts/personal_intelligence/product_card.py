"""Evidence-first product cards. Missing inputs remain missing."""
from price_impact import price_impacts

def enrich_cards(cards, weather, events, evidence=None):
    evidence=evidence or {}
    for card in cards:
        sku=card["sku"]
        e=evidence.get(sku,{})
        daily=e.get("daily_metrics",{})
        prices=e.get("prices",[])
        card["price_history"]=prices
        card["price_impacts"]=price_impacts(prices,daily)
        card["aligned_daily_metrics"]=daily
        card["weather"]=weather
        card["historical_weather"]=e.get("historical_weather")
        card["pdp_series"]=e.get("pdp_series")
        card["factors"]={}
        for key in ("paid","organic","crm","competition","inventory","social","conversion"):
            value=e.get(key)
            card["factors"][key]={"status":"evidence_available" if value is not None else "source_required","evidence":value}
        card["factors"]["price"]={"status":"observed_change" if card["price_impacts"] else "no_verified_change","evidence":card["price_impacts"]}
        card["factors"]["weather"]={"status":"forecast_context_only" if not card["historical_weather"] else "association_unverified","evidence":weather}
        card["calendar_context"]=events or []
        card["diagnosis"]={"causal_status":"unverified","observations":[{"metric":"sales_yoy_pct","value":card.get("yoy_pct")}],"actions":["Check missing product-level evidence before attribution","Compare price-change windows with weather, traffic and stock"]}
    return cards
