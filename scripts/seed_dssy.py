"""
NAG V3 — DSSY Synthetic Data Seed Script
=========================================
Generates ~300,000 realistic beneficiary records + 6 years of payment history
so every analytics query ("last 3 years payments", "category breakdown", etc.)
returns real numbers instead of hallucinating.

Record breakdown:
  beneficiaries              : 300,000  (actual DSSY ~1.4 lakh scale)
  scheme_enrollments         :  90,000  (30% have a prior category)
  beneficiary_status_history : 318,000  (initial + exit events)
  payment_batches            :      72  (one per month, FY 2020-21→2025-26)
  payments (last 6 months)   :  ~35,000 (sample — full history via payment_summary)
  payment_summary            :   1,680  (pre-aggregated 2020–2026)
  life_certificates          : ~130,000 (active beneficiaries, years 2022–2025)
  TOTAL                      : ~875,000 rows

WHY THIS MATTERS FOR THE AI:
  - "Last 3 years payment" → queries payment_summary_monthly (real aggregates)
  - "Total beneficiaries" → 300,000 rows in beneficiaries table
  - "Category breakdown" → correct counts per category (not hallucinated 4k/7k)
  - "Life cert compliance" → real is_late_submission / payment_suspended data
  - All numbers are deterministic (random.seed(42)) — consistent across runs

Usage:
  pip install asyncpg faker
  export NEON_DATABASE_URL="postgresql://user:pass@host/dbname?sslmode=require"
  python scripts/seed_dssy.py [--dry-run] [--batch 500]
"""

import asyncio
import asyncpg
import argparse
import os
import sys
import random
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

# Fix Windows console encoding for Unicode characters
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# Auto-load .env
_env = Path(__file__).resolve().parent.parent / ".env"
if _env.exists():
    for _line in _env.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("seed")
random.seed(42)

try:
    from faker import Faker
    fake = Faker("en_IN")
except ImportError:
    print("[ERROR] faker not installed. Run: pip install faker")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# REFERENCE DATA  (matches actual seed data in create_neon_schema.py)
# ─────────────────────────────────────────────────────────────────────────────

# district_id: 1=North Goa, 2=South Goa
# taluka_id matches the INSERT order in create_neon_schema.py SEED_SQL
TALUKAS = [
    # (taluka_id, district_id, taluka_name)
    (1,  1, "Tiswadi"),
    (2,  1, "Bardez"),
    (3,  1, "Pernem"),
    (4,  1, "Bicholim"),
    (5,  1, "Satari"),
    (6,  1, "Ponda"),
    (7,  2, "Salcete"),
    (8,  2, "Mormugao"),
    (9,  2, "Quepem"),
    (10, 2, "Sanguem"),
    (11, 2, "Canacona"),
    (12, 2, "Dharbandora"),
]
TALUKA_WEIGHTS = [0.12, 0.14, 0.07, 0.05, 0.04, 0.08, 0.16, 0.10, 0.07, 0.06, 0.05, 0.06]

# category_id matches INSERT order in create_neon_schema.py
# (id, code, total_count, age_min, age_max, gender_lock, monthly_amount)
CATEGORIES = [
    (1, "SC",    174000, 60, 95,  None,     2000),   # Senior Citizen  ~58%
    (2, "WD",     75000, 30, 82,  "Female", 2500),   # Widow           ~25%
    (3, "SW",     18000, 50, 80,  "Female", 2000),   # Single Woman    ~6%
    (4, "DAC",    12600, 18, 65,  None,     2000),   # Disabled Adult  ~4.2%
    (5, "DCB90",   6900,  5, 17,  None,     2500),   # Disabled <90%   ~2.3%
    (6, "DC90",    4500,  5, 60,  None,     3500),   # Disabled 90%+   ~1.5%
    (7, "HIV",     9000, 20, 60,  None,     2000),   # HIV/AIDS        ~3%
]
# Total: 300,000

GOA_MALE   = ["Ramesh","Suresh","Mohan","Ganesh","Prakash","Devidas","Antonio",
               "Francisco","Jose","Mario","Peter","John","Shaikh","Mohammad",
               "Abdul","Pandurang","Tukaram","Vitthal","Maruti","Damodar"]
GOA_FEMALE = ["Savitri","Radha","Lakshmi","Parvati","Geeta","Sunita","Meena",
               "Maria","Teresa","Rosa","Fatima","Lucia","Angela","Safia","Amina",
               "Sonali","Priya","Deepa","Smita","Lata","Indira","Rukmini"]
GOA_SURNAMES = ["Naik","Gawas","Sawant","Dessai","Parsekar","Gaonkar","Kamat",
                "D'Souza","Fernandes","Pereira","Gomes","Rodrigues","Mendes",
                "Shaikh","Khan","Teli","Bhandari","Kulkarni","Parab","Harmalkar"]
GOA_PINCODES = ["403001","403002","403101","403102","403201","403202","403401",
                "403402","403501","403502","403601","403602","403702","403703"]

MONTH_NAMES = {
    1:"January",2:"February",3:"March",4:"April",5:"May",6:"June",
    7:"July",8:"August",9:"September",10:"October",11:"November",12:"December",
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _name(gender: str) -> str:
    first = random.choice(GOA_MALE if gender == "Male" else GOA_FEMALE)
    return f"{first} {random.choice(GOA_SURNAMES)}"


def _dob(age_min: int, age_max: int) -> date:
    today = date.today()
    age   = random.randint(age_min, age_max)
    yr    = today.year - age
    try:
        return date(yr, random.randint(1, 12), random.randint(1, 28))
    except ValueError:
        return date(yr, 1, 1)


def _reg_date() -> date:
    # Weighted toward post-2020 digitisation spike
    year_pool = (
        list(range(2002, 2010)) * 15 +
        list(range(2010, 2017)) * 20 +
        list(range(2017, 2020)) * 15 +
        list(range(2020, 2024)) * 35 +
        list(range(2024, 2026)) * 15
    )
    y = random.choice(year_pool)
    try:
        return date(y, random.randint(1, 12), random.randint(1, 28))
    except ValueError:
        return date(y, 1, 1)


def _aadhaar(used: set) -> str:
    for _ in range(50):
        a = str(random.randint(2, 9)) + str(random.randint(10_000_000_000, 99_999_999_999))[:11]
        if a not in used:
            used.add(a)
            return a
    # Fallback: guaranteed unique
    a = str(random.randint(2, 9)) + str(len(used)).zfill(11)
    used.add(a)
    return a


def _fiscal(d: date) -> int:
    return d.year if d.month >= 4 else d.year - 1


def _fiscal_label(fy: int) -> str:
    return f"{fy}-{str(fy+1)[2:]}"


def _quarter(d: date) -> int:
    m = d.month
    if m in (4, 5, 6):    return 1
    if m in (7, 8, 9):    return 2
    if m in (10, 11, 12): return 3
    return 4


# ─────────────────────────────────────────────────────────────────────────────
# DATA GENERATORS
# ─────────────────────────────────────────────────────────────────────────────

def gen_beneficiaries() -> list[dict]:
    log.info("Generating 300,000 beneficiaries …")
    rows: list[dict] = []
    aadhaar_used: set[str] = set()
    idx = 1

    for (cat_id, cat_code, cat_count, age_min, age_max, gender_lock, monthly_amt) in CATEGORIES:
        # Status distribution: 94% Active, 3% Inactive, 2% Deceased, 1% other
        statuses = (
            ["Active"] * int(cat_count * 0.94) +
            ["Inactive"] * int(cat_count * 0.03) +
            ["Deceased"] * int(cat_count * 0.02) +
            ["Inactive"] * (cat_count - int(cat_count * 0.94)
                            - int(cat_count * 0.03)
                            - int(cat_count * 0.02))
        )
        random.shuffle(statuses)

        for i in range(cat_count):
            gender = gender_lock or random.choice(["Male", "Female", "Male", "Female", "Female"])
            if cat_code == "SC":
                gender = random.choice(["Female", "Female", "Male"])  # women live longer

            dob      = _dob(age_min, age_max)
            reg_date = _reg_date()
            if reg_date < date(2002, 1, 1):
                reg_date = date(2002, random.randint(1, 12), random.randint(1, 28))

            taluka = random.choices(TALUKAS, weights=TALUKA_WEIGHTS, k=1)[0]
            status = statuses[i]
            status_changed = None
            if status != "Active":
                days_ago = random.randint(30, 1800)
                status_changed = datetime.now() - timedelta(days=days_ago)

            rows.append({
                "beneficiary_id":       idx,
                "beneficiary_code":     f"DSSY/GOA/{str(idx).zfill(7)}",
                "first_name":           _name(gender).split()[0],
                "last_name":            _name(gender).split()[-1],
                "gender":               gender,
                "date_of_birth":        dob,
                "age":                  date.today().year - dob.year,
                "district_id":          taluka[1],
                "taluka_id":            taluka[0],
                "pincode":              random.choice(GOA_PINCODES),
                "category_id":          cat_id,
                "current_monthly_amount": float(monthly_amt),
                "bank_id":              random.randint(1, 10),
                "account_number":       str(random.randint(10_000_000_000, 99_999_999_999)),
                "aadhaar_number":       _aadhaar(aadhaar_used),
                "phone_number":         f"98{random.randint(10_000_000, 99_999_999)}",
                "registration_date":    reg_date,
                "status":               status,
                "status_changed_at":    status_changed,
                "created_at":           datetime.combine(reg_date, datetime.min.time()),
                "updated_at":           datetime.now(),
            })
            idx += 1

    random.shuffle(rows)
    log.info("  ✓ %d beneficiaries", len(rows))
    return rows


def gen_enrollments(bens: list[dict]) -> list[dict]:
    log.info("Generating scheme_enrollments …")
    rows: list[dict] = []
    eid = 1
    for b in bens:
        reg  = b["registration_date"]
        cid  = b["category_id"]

        # 30% had a prior category
        if random.random() < 0.30 and cid in (1, 3):
            prior_cid  = 2 if cid == 1 else 4
            prior_end  = reg - timedelta(days=random.randint(30, 365))
            prior_start = prior_end - timedelta(days=random.randint(180, 3650))
            if prior_start >= date(2002, 1, 1):
                rows.append({
                    "enrollment_id":   eid,
                    "beneficiary_id":  b["beneficiary_id"],
                    "category_id":     prior_cid,
                    "enrollment_date": prior_start,
                    "enrollment_year": prior_start.year,
                    "end_date":        prior_end,
                    "end_reason":      "Category_Transfer",
                    "is_current":      False,
                    "created_at":      datetime.combine(prior_start, datetime.min.time()),
                })
                eid += 1

        # Current enrollment
        exit_date = exit_reason = None
        if b["status"] != "Active":
            exit_date = (
                b["status_changed_at"].date()
                if b["status_changed_at"] else
                reg + timedelta(days=random.randint(90, 1800))
            )
            exit_reason = {
                "Inactive": "Income_Exceeded",
                "Deceased": "Death",
            }.get(b["status"], "Other")

        rows.append({
            "enrollment_id":   eid,
            "beneficiary_id":  b["beneficiary_id"],
            "category_id":     cid,
            "enrollment_date": reg,
            "enrollment_year": reg.year,
            "end_date":        exit_date,
            "end_reason":      exit_reason,
            "is_current":      b["status"] == "Active",
            "created_at":      datetime.combine(reg, datetime.min.time()),
        })
        eid += 1

    log.info("  ✓ %d enrollments", len(rows))
    return rows


def gen_status_history(bens: list[dict]) -> list[dict]:
    log.info("Generating beneficiary_status_history …")
    rows: list[dict] = []
    hid = 1
    for b in bens:
        rows.append({
            "id":             hid,
            "beneficiary_id": b["beneficiary_id"],
            "old_status":     None,
            "new_status":     "Active",
            "changed_at":     datetime.combine(b["registration_date"], datetime.min.time()),
            "reason":         "New_Enrollment",
        })
        hid += 1
        if b["status"] != "Active":
            exit_at = (
                b["status_changed_at"]
                if b["status_changed_at"] else
                datetime.combine(
                    b["registration_date"] + timedelta(days=random.randint(90, 1800)),
                    datetime.min.time(),
                )
            )
            rows.append({
                "id":             hid,
                "beneficiary_id": b["beneficiary_id"],
                "old_status":     "Active",
                "new_status":     b["status"],
                "changed_at":     exit_at,
                "reason":         {"Inactive": "Income_Exceeded", "Deceased": "Death"}.get(b["status"], "Other"),
            })
            hid += 1

    log.info("  ✓ %d status history rows", len(rows))
    return rows


def gen_payment_batches() -> list[dict]:
    """72 monthly ECS batches: April 2020 → March 2026."""
    log.info("Generating payment_batches …")
    rows: list[dict] = []
    bid = 1
    cur = date(2020, 4, 1)
    end = date(2026, 3, 31)
    today = date.today()
    while cur <= min(end, today):
        fy = _fiscal(cur)
        rows.append({
            "batch_id":          bid,
            "batch_reference":   f"BATCH/{cur.year}/{str(cur.month).zfill(2)}",
            "payment_month":     cur.month,
            "payment_year":      cur.year,
            "fiscal_year":       fy,
            "fiscal_year_label": _fiscal_label(fy),
            "batch_status":      "Completed",
            "total_beneficiaries": random.randint(138_000, 142_000),
            "total_amount":      float(random.randint(650_000_000, 750_000_000)),
            "paid_count":        random.randint(133_000, 139_000),
            "failed_count":      random.randint(1_000, 5_000),
            "pending_count":     random.randint(200, 1_500),
            "initiated_at":      datetime(cur.year, cur.month, 1),
            "completed_at":      datetime(cur.year, cur.month, 1) + timedelta(days=3),
            "created_at":        datetime(cur.year, cur.month, 1),
        })
        bid += 1
        # advance to next month
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)

    log.info("  ✓ %d payment batches", len(rows))
    return rows


def gen_payment_summary(bens: list[dict]) -> list[dict]:
    """
    Pre-aggregated payment_summary rows — exactly what the AI queries for
    "last 3 years payments", "YoY comparison", "district-wise payout by year".
    One row per (year, month, district, taluka, category).
    These numbers are grounded: the AI will read real aggregates, not hallucinate.
    """
    log.info("Generating payment_summary (pre-aggregated) …")

    # Build active-beneficiary count map: (cat_id, taluka_id, district_id) → {count, male, female}
    counts: dict[tuple, dict] = {}
    for b in bens:
        if b["status"] != "Active":
            continue
        key = (b["category_id"], b["taluka_id"], b["district_id"])
        if key not in counts:
            counts[key] = {"count": 0, "male": 0, "female": 0}
        counts[key]["count"] += 1
        if b["gender"] == "Male":
            counts[key]["male"] += 1
        else:
            counts[key]["female"] += 1

    # Monthly amounts per category at different periods
    CAT_AMOUNT: dict[int, dict] = {
        # cat_id: {period → amount}
        1: {2020: 1500, 2021: 2000, 2022: 2000, 2023: 2000, 2024: 2000, 2025: 2000},
        2: {2020: 1500, 2021: 2500, 2022: 2500, 2023: 2500, 2024: 2500, 2025: 2500},
        3: {2020: 1500, 2021: 2000, 2022: 2000, 2023: 2000, 2024: 2000, 2025: 2000},
        4: {2020: 1500, 2021: 2000, 2022: 2000, 2023: 2000, 2024: 2000, 2025: 2000},
        5: {2020: 2000, 2021: 2500, 2022: 2500, 2023: 2500, 2024: 2500, 2025: 2500},
        6: {2020: 3000, 2021: 3500, 2022: 3500, 2023: 3500, 2024: 3500, 2025: 3500},
        7: {2020: 1500, 2021: 2000, 2022: 2000, 2023: 2000, 2024: 2000, 2025: 2000},
    }

    rows: list[dict] = []
    sid = 1
    cur = date(2020, 4, 1)
    end = date(2026, 3, 31)
    today = date.today()

    while cur <= min(end, today):
        yr = cur.year
        mo = cur.month
        fy = _fiscal(cur)
        fy_label = _fiscal_label(fy)
        q = _quarter(cur)

        for (cat_id, taluka_id, district_id), cnt in counts.items():
            n = cnt["count"]
            if n == 0:
                cur = date(cur.year + (1 if cur.month == 12 else 0),
                           1 if cur.month == 12 else cur.month + 1, 1)
                continue

            # Slight beneficiary growth over years
            growth = 1.0 + (yr - 2020) * 0.025
            adj_n  = max(1, int(n * growth * random.uniform(0.98, 1.02)))

            # Amount historically correct
            amount = CAT_AMOUNT.get(cat_id, {}).get(yr, 2000)

            fail_rate    = random.uniform(0.01, 0.04)
            pending_rate = random.uniform(0.003, 0.015)
            failed  = max(0, int(adj_n * fail_rate))
            pending = max(0, int(adj_n * pending_rate))
            paid    = adj_n - failed - pending

            rows.append({
                "summary_id":          sid,
                "payment_year":        yr,
                "payment_month":       mo,
                "month_name":          MONTH_NAMES[mo],
                "fiscal_year":         fy,
                "fiscal_year_label":   fy_label,
                "quarter":             q,
                "district_id":         district_id,
                "taluka_id":           taluka_id,
                "category_id":         cat_id,
                "total_beneficiaries": adj_n,
                "paid_count":          max(0, paid),
                "pending_count":       pending,
                "failed_count":        failed,
                "on_hold_count":       0,
                "total_base_amount":   float(adj_n * amount),
                "total_net_amount":    float(max(0, paid * amount)),
                "male_count":          cnt["male"],
                "female_count":        cnt["female"],
                "last_updated_at":     datetime.now(),
            })
            sid += 1

        # Next month
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)

    log.info("  ✓ %d payment_summary rows", len(rows))
    return rows


def gen_payments_sample(bens: list[dict], months: int = 6) -> list[dict]:
    """Individual payment rows — last N months, 25% of active beneficiaries."""
    log.info("Generating individual payments (last %d months) …", months)
    active  = [b for b in bens if b["status"] == "Active"]
    sampled = random.sample(active, min(35_000, len(active)))
    today   = date.today()
    rows: list[dict] = []
    pid = 1
    for b in sampled:
        for m in range(months):
            # Payment date = first few days of that month
            pay_month = today.month - m
            pay_year  = today.year
            while pay_month <= 0:
                pay_month += 12
                pay_year  -= 1
            pay_date = date(pay_year, pay_month, random.randint(1, 5))

            roll = random.random()
            if roll < 0.03:
                status = "Failed"
            elif roll < 0.05:
                status = "Pending"
            else:
                status = "Paid"

            rows.append({
                "payment_id":      pid,
                "beneficiary_id":  b["beneficiary_id"],
                "payment_date":    pay_date,
                "payment_month":   pay_date.month,
                "payment_year":    pay_date.year,
                "amount":          b["current_monthly_amount"],
                "status":          status,
                "payment_method":  "ECS",
                "transaction_id":  f"ECS{random.randint(1_000_000_000, 9_999_999_999)}",
                "created_at":      datetime.combine(pay_date, datetime.min.time()),
            })
            pid += 1

    log.info("  ✓ %d individual payment rows", len(rows))
    return rows


def gen_life_certs(bens: list[dict]) -> list[dict]:
    """Annual life certs for active beneficiaries, years 2024–2025."""
    log.info("Generating life_certificates …")
    active = [b for b in bens if b["status"] == "Active"]
    rows: list[dict] = []
    cid = 1
    for b in active:
        for year in (2024, 2025):
            if random.random() < 0.08:   # 8% miss submission
                continue
            sub_month = random.randint(4, 7)   # April–July
            try:
                sub_date = date(year, sub_month, random.randint(1, 28))
            except ValueError:
                sub_date = date(year, sub_month, 1)
            is_late  = sub_month > 5
            days_late = max(0, (sub_date - date(year, 5, 31)).days) if is_late else 0
            rows.append({
                "cert_id":          cid,
                "beneficiary_id":   b["beneficiary_id"],
                "submission_date":  sub_date,
                "due_month":        4,
                "due_year":         year,
                "fiscal_year":      _fiscal(date(year, 4, 1)),
                "is_late_submission": is_late,
                "days_late":        days_late,
                "issued_by_type":   random.choice(
                    ["Bank_Manager", "Gazetted_Officer", "Bank_Manager", "Aadhaar_eKYC"]
                ),
                "payment_suspended": False,
                "created_at":       datetime.combine(sub_date, datetime.min.time()),
            })
            cid += 1

    log.info("  ✓ %d life cert rows", len(rows))
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE OPERATIONS
# ─────────────────────────────────────────────────────────────────────────────

async def truncate(conn: asyncpg.Connection):
    log.info("Truncating existing data …")
    order = [
        "life_certificates",
        "payments",
        "payment_batches",
        "scheme_enrollments",
        "beneficiary_status_history",
        "payment_summary",
        "beneficiaries",
    ]
    for t in order:
        await conn.execute(f"TRUNCATE TABLE {t} RESTART IDENTITY CASCADE")
    log.info("  ✓ tables cleared")


async def bulk_insert(
    conn: asyncpg.Connection,
    table: str,
    rows: list[dict],
    batch: int = 500,
):
    if not rows:
        return
    cols  = list(rows[0].keys())
    total = 0
    for i in range(0, len(rows), batch):
        chunk  = rows[i : i + batch]
        values = []
        for r in chunk:
            values.extend(r[c] for c in cols)
        placeholders = ", ".join(
            f"({', '.join(f'${j * len(cols) + k + 1}' for k in range(len(cols)))})"
            for j in range(len(chunk))
        )
        sql = (
            f"INSERT INTO {table} ({', '.join(cols)}) VALUES {placeholders} "
            f"ON CONFLICT DO NOTHING"
        )
        await conn.execute(sql, *values)
        total += len(chunk)
        if total % 50_000 == 0 or total == len(rows):
            log.info("    %s: %d / %d rows", table, total, len(rows))


async def seed(dsn: str, batch_size: int = 500, dry_run: bool = False):
    log.info("=" * 60)
    log.info("NAG V3 — DSSY Seed Script  (300k beneficiaries)")
    log.info("=" * 60)

    bens        = gen_beneficiaries()
    enrollments = gen_enrollments(bens)
    history     = gen_status_history(bens)
    batches     = gen_payment_batches()
    payments    = gen_payments_sample(bens, months=6)
    summary     = gen_payment_summary(bens)
    certs       = gen_life_certs(bens)

    total = (len(bens) + len(enrollments) + len(history) +
             len(batches) + len(payments) + len(summary) + len(certs))

    log.info("")
    log.info("RECORD COUNT SUMMARY")
    log.info("  beneficiaries              : %7d", len(bens))
    log.info("  scheme_enrollments         : %7d", len(enrollments))
    log.info("  beneficiary_status_history : %7d", len(history))
    log.info("  payment_batches            : %7d", len(batches))
    log.info("  payments (sample)          : %7d", len(payments))
    log.info("  payment_summary            : %7d", len(summary))
    log.info("  life_certificates          : %7d", len(certs))
    log.info("  TOTAL                      : %7d", total)
    log.info("")
    log.info("KEY COUNTS (what the AI will see — no hallucination):")
    active = sum(1 for b in bens if b["status"] == "Active")
    log.info("  Active beneficiaries       : %7d", active)
    log.info("  Inactive beneficiaries     : %7d", sum(1 for b in bens if b["status"] == "Inactive"))
    log.info("  Deceased beneficiaries     : %7d", sum(1 for b in bens if b["status"] == "Deceased"))
    from collections import Counter
    cat_counts = Counter(b["category_id"] for b in bens if b["status"] == "Active")
    for cat_id, _, cat_code, *_ in [(c[0], c[1], c[2]) for c in CATEGORIES]:
        log.info("  %-28s: %7d", cat_code, cat_counts.get(cat_id, 0))
    log.info("")

    if dry_run:
        log.info("DRY RUN — no database writes.")
        return

    log.info("Connecting to Neon …")
    conn = await asyncpg.connect(dsn)
    log.info("  ✓ Connected")

    # Helper: each batch auto-commits (no wrapping transaction) to avoid
    # Neon free-tier connection timeouts on long-running single transactions.
    async def _insert_table(label, table, rows, col_filter=None):
        log.info("Inserting %s …", label)
        data = rows
        if col_filter:
            data = [{k: v for k, v in r.items() if k in col_filter} for r in rows]
        await bulk_insert(conn, table, data, batch_size)

    try:
        async with conn.transaction():
            await truncate(conn)

        # ── Disable triggers during bulk insert ──────────────────
        # The audit_beneficiaries trigger stores to_jsonb(NEW) for every INSERT
        # which doubles storage. The status_history trigger is redundant since
        # we insert history rows explicitly. Disable both to fit 300k in 512 MB.
        log.info("Disabling user triggers for bulk insert …")
        await conn.execute("ALTER TABLE beneficiaries DISABLE TRIGGER USER")
        await conn.execute("ALTER TABLE payments DISABLE TRIGGER USER")

        await _insert_table("beneficiaries", "beneficiaries", bens)

        await _insert_table("scheme_enrollments", "scheme_enrollments", enrollments,
            ("enrollment_id","beneficiary_id","category_id",
             "enrollment_date","end_date","is_current","created_at"))

        await _insert_table("beneficiary_status_history", "beneficiary_status_history", history,
            ("id","beneficiary_id","old_status","new_status","changed_at","reason"))

        await _insert_table("payment_batches", "payment_batches", batches)

        await _insert_table("payments", "payments", payments,
            ("payment_id","beneficiary_id","payment_date",
             "amount","status","payment_method","transaction_id","created_at"))

        await _insert_table("payment_summary", "payment_summary", summary,
            ("summary_id","payment_year","payment_month","fiscal_year",
             "district_id","taluka_id","category_id",
             "total_beneficiaries","paid_count","pending_count",
             "failed_count","total_base_amount","total_net_amount",
             "male_count","female_count","last_updated_at"))

        await _insert_table("life_certificates", "life_certificates", certs)

        # ── Re-enable triggers ───────────────────────────────────
        log.info("Re-enabling user triggers …")
        await conn.execute("ALTER TABLE beneficiaries ENABLE TRIGGER USER")
        await conn.execute("ALTER TABLE payments ENABLE TRIGGER USER")
        log.info("  ✓ Triggers re-enabled")

        log.info("")
        log.info("Refreshing materialized views …")
        for mv in (
            "payment_summary_monthly",
            "beneficiary_summary_current",
            "beneficiary_registration_summary_monthly",
        ):
            try:
                await conn.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv}")
                log.info("  ✓ %s refreshed", mv)
            except Exception as e:
                # CONCURRENTLY needs a unique index; fall back to non-concurrent
                try:
                    await conn.execute(f"REFRESH MATERIALIZED VIEW {mv}")
                    log.info("  ✓ %s refreshed (non-concurrent)", mv)
                except Exception as e2:
                    log.warning("  ⚠ %s skipped: %s", mv, e2)

    finally:
        # Safety: always re-enable triggers even on failure
        try:
            await conn.execute("ALTER TABLE beneficiaries ENABLE TRIGGER ALL")
            await conn.execute("ALTER TABLE payments ENABLE TRIGGER ALL")
        except Exception:
            pass
        await conn.close()

    log.info("")
    log.info("=" * 60)
    log.info("SEED COMPLETE — %d total rows", total)
    log.info("The AI now has real data. No more hallucinated counts.")
    log.info("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NAG V3 DSSY Seed — 300k records")
    parser.add_argument("--dsn",      default=os.getenv("NEON_DATABASE_URL", ""),
                        help="Neon PostgreSQL DSN")
    parser.add_argument("--batch",    type=int, default=500,
                        help="Insert batch size (default 500)")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Print counts only — no DB writes")
    args = parser.parse_args()

    if not args.dry_run and not args.dsn:
        print("ERROR: Provide --dsn or set NEON_DATABASE_URL in .env")
        sys.exit(1)

    asyncio.run(seed(dsn=args.dsn, batch_size=args.batch, dry_run=args.dry_run))
