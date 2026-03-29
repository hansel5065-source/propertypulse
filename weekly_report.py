"""
weekly_report.py — Generate a summary report after each weekly scrape.
Called by weekly_run.bat after merge_new_data.py runs.
"""
import json, os, sys, argparse
from datetime import datetime

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_JSON = os.path.join(APP_DIR, "data.json")

def run(log_file=None, output_file=None):
    # Load current data
    with open(DATA_JSON, encoding="utf-8", errors="replace") as f:
        data = json.load(f)

    total = len(data)
    by_county = {}
    by_cat = {}
    by_distress = {}
    hot = 0
    new_this_week = 0
    today_str = datetime.now().strftime("%Y-%m-%d")

    for r in data:
        c = r.get("county", "Unknown")
        by_county[c] = by_county.get(c, 0) + 1

        cat = r.get("category", "unknown")
        by_cat[cat] = by_cat.get(cat, 0) + 1

        dl = r.get("distressLevel", "other")
        by_distress[dl] = by_distress.get(dl, 0) + 1

        if r.get("score", 0) >= 7:
            hot += 1

        scraped = r.get("scrapedAt", "")
        if scraped.startswith(today_str):
            new_this_week += 1

    lines = [
        "=" * 60,
        f"PROPERTYPULSE WEEKLY REPORT — {datetime.now().strftime('%A, %B %d, %Y')}",
        "=" * 60,
        "",
        f"TOTAL LEADS IN DATABASE:  {total:,}",
        f"NEW LEADS THIS RUN:       {new_this_week:,}",
        f"HOT LEADS (score 7+):     {hot:,}",
        "",
        "── BY COUNTY ──────────────────────────────────────────",
    ]
    for k, v in sorted(by_county.items(), key=lambda x: -x[1]):
        lines.append(f"  {k:<20} {v:>5}")

    lines += ["", "── BY CATEGORY ─────────────────────────────────────────"]
    for k, v in sorted(by_cat.items(), key=lambda x: -x[1]):
        lines.append(f"  {k:<25} {v:>5}")

    lines += ["", "── BY URGENCY ──────────────────────────────────────────"]
    urgency_labels = {
        "active_sale": "🔴 Active Sale (going to auction/foreclosure)",
        "lien_legal":  "🟠 Lien / Legal (IRS, HOA, mechanic, lis pendens)",
        "delinquent":  "🟡 Delinquent (behind on taxes — early stage)",
        "other":       "⚪ Other",
    }
    for k, label in urgency_labels.items():
        v = by_distress.get(k, 0)
        lines.append(f"  {label:<50} {v:>5}")

    # Errors from log
    errors = []
    if log_file and os.path.exists(log_file):
        with open(log_file, encoding="utf-8", errors="replace") as f:
            for line in f:
                if "ERROR" in line or "No data returned" in line or "failed" in line.lower():
                    errors.append("  " + line.strip())

    if errors:
        lines += ["", "── ERRORS / ISSUES ─────────────────────────────────────"]
        lines += errors[:20]
        if len(errors) > 20:
            lines.append(f"  ... and {len(errors)-20} more. See full log.")

    lines += [
        "",
        "── ACTION ITEMS ────────────────────────────────────────",
        "  • Open PropertyPulse → Urgency: 🔴 Active Sale → sort by Score",
        "  • Properties with sale dates this week need immediate attention",
        "  • Tax Delinquent = early stage — skip trace owners, send mail",
        "",
        "=" * 60,
    ]

    report = "\n".join(lines)
    print(report)

    if output_file:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"\nReport saved: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", default=None)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    run(log_file=args.log, output_file=args.output)
