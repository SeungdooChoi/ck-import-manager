import streamlit as st
import pandas as pd
from sqlalchemy import text
from datetime import datetime, timedelta
import time
import pytz
import re
import io
import json
import uuid
import html as _html

# ==========================================
# 0. 기본 설정 및 스타일
# ==========================================
st.set_page_config(page_title="수입진행관리 (CK Global)", layout="wide", page_icon="🚢")
KST = pytz.timezone('Asia/Seoul')

def get_kst_today():
    return datetime.now(KST).date()

def get_kst_now():
    return datetime.now(KST)

st.markdown("""
<style>
    .block-container { max-width: 98% !important; padding-top: 1rem; }
    
    /* 상태 배지 스타일 */
    .status-badge { padding: 4px 8px; border-radius: 6px; font-weight: bold; font-size: 0.85em; display: inline-block; }
    .status-pending { background-color: #fff3bf; color: #d05d00; border: 1px solid #ffec99; }
    .status-arrived { background-color: #d3f9d8; color: #2b8a3e; border: 1px solid #b2f2bb; }
    .status-canceled { background-color: #ffe3e3; color: #c92a2a; border: 1px solid #ffc9c9; }

    /* 폼 헤더 스타일 */
    .form-header {
        font-weight: 700;
        font-size: 1.0em;
        margin-top: 15px;
        margin-bottom: 8px;
        color: #343a40;
        border-left: 4px solid #339af0;
        padding-left: 8px;
    }
    
    /* 삼각무역 그룹 헤더 */
    .tri-header {
        background-color: #f1f3f5;
        padding: 8px;
        border-radius: 5px;
        font-weight: bold;
        text-align: center;
        margin-bottom: 10px;
        border: 1px solid #dee2e6;
    }
    .tri-header-lc { background-color: #fff9db; color: #f08c00; border-color: #ffec99; } /* L/C - Yellowish */
    .tri-header-pay { background-color: #ffe3e3; color: #e03131; border-color: #ffc9c9; } /* Payment - Reddish */
    
    /* 데이터프레임 스타일 */
    .stDataFrame { font-size: 13px; }
    
    /* 상단 메뉴(nav_radio)만 탭처럼 스타일링 — 다른 라디오(상태 선택 등)는 기본 모양 유지 */
    .st-key-nav_radio div[role="radiogroup"] > label > div:first-of-type {
        display: none;
    }
    .st-key-nav_radio div[role="radiogroup"] {
        flex-direction: row;
        gap: 10px;
        border-bottom: 1px solid #dee2e6;
        padding-bottom: 5px;
        margin-bottom: 15px;
    }

    /* 진행현황 표: 좁은 화면에서는 표 안에서만 가로 스크롤 */
    .progress-wrap { overflow-x: auto; -webkit-overflow-scrolling: touch; }

    /* 모바일 카드 (진행현황 / 장부 공용) */
    .m-card { border: 1px solid #dee2e6; border-radius: 10px; margin-bottom: 10px; overflow: hidden; background: #fff; }
    .m-card-head { display: flex; justify-content: space-between; align-items: baseline; gap: 8px;
                   padding: 8px 12px; background: #f1f3f5; font-size: 14px; }
    .m-card-head .sub { color: #868e96; font-size: 12px; white-space: nowrap; }
    .m-ship { display: grid; grid-template-columns: 74px 1fr auto; align-items: center; gap: 8px;
              padding: 7px 12px; border-top: 1px solid #f1f3f5; font-size: 13px; }
    .m-ship .d { font-weight: 700; }
    .m-ship .s { color: #495057; font-size: 12px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .m-ship .q { font-weight: 700; text-align: right; }
    .m-ship.cell-pending { background: #fff9db; }
    .m-ship.cell-arrived { background: #e3fafc; }
    .m-dest { font-weight: 700; font-size: 14px; margin: 14px 0 6px; color: #343a40; }
    .m-kv { display: grid; grid-template-columns: 1fr 1fr; gap: 2px 12px; padding: 8px 12px; font-size: 13px; }
    .m-kv .k { color: #868e96; font-size: 11px; }

    @media (max-width: 640px) {
        .block-container { max-width: 100% !important; padding: 0.6rem 0.7rem 3rem !important; }
        h1 { font-size: 1.35rem !important; }
        h3 { font-size: 1.1rem !important; }
        .stDataFrame { font-size: 12px; }

        /* 메뉴: 한 줄 가로 스와이프 + 알약 버튼 */
        .st-key-nav_radio div[role="radiogroup"] {
            flex-wrap: nowrap; overflow-x: auto; gap: 6px; padding-bottom: 8px;
            -webkit-overflow-scrolling: touch; scrollbar-width: none;
        }
        .st-key-nav_radio div[role="radiogroup"]::-webkit-scrollbar { display: none; }
        .st-key-nav_radio div[role="radiogroup"] > label {
            flex: 0 0 auto; white-space: nowrap; margin: 0;
            padding: 8px 14px; border: 1px solid #dee2e6; border-radius: 999px; background: #f8f9fa;
        }
        .st-key-nav_radio div[role="radiogroup"] > label:has(input:checked) {
            background: #339af0; border-color: #339af0;
        }
        .st-key-nav_radio div[role="radiogroup"] > label:has(input:checked) p { color: #fff !important; font-weight: 700; }

        /* 터치 영역 확대 */
        .stButton button, .stFormSubmitButton button { min-height: 46px; }
        input, textarea, [data-baseweb="select"] { font-size: 16px !important; } /* iOS 입력 시 자동 확대 방지 */

        /* 입력 폼: 3열이 1열로 길어지지 않도록 2열씩 유지 */
        [data-testid="stForm"] [data-testid="stHorizontalBlock"] { flex-wrap: wrap !important; gap: 0.5rem !important; }
        [data-testid="stForm"] [data-testid="stColumn"] {
            min-width: calc(50% - 0.5rem) !important; flex: 1 1 calc(50% - 0.5rem) !important;
        }
        [data-testid="stTabs"] [role="tablist"] { overflow-x: auto; }
    }
</style>
""", unsafe_allow_html=True)

def is_mobile_device():
    """User-Agent로 모바일 여부 판단 (재고장 앱과 동일한 방식)"""
    try:
        ua = st.context.headers.get("User-Agent", "").lower()
        return any(m in ua for m in ['mobile', 'android', 'iphone', 'ipod'])
    except Exception:
        return False

IS_MOBILE = is_mobile_device()

# DB 연결 및 스키마 업데이트
try:
    conn = st.connection("supabase", type="sql")

    @st.cache_resource
    def run_db_migrations():
        # 앱 프로세스 시작 후 최초 1회만 실행 (st.cache_resource) — 매 rerun(탭 전환/버튼 클릭)마다
        # ALTER TABLE 100여 개를 매번 재실행하던 것이 초기 로딩/탭 전환이 느렸던 주된 원인이었음
        with conn.session as s:
            # 공통 컬럼 정의 (수입/수출)
            common_cols = [
                ("ck_code", "TEXT"), ("size", "TEXT"), ("unit_price", "NUMERIC"), ("supplier", "TEXT"),
                ("global_code", "TEXT"), ("doojin_code", "TEXT"), ("agency", "TEXT"), ("agency_contract", "TEXT"),
                ("origin", "TEXT"), ("packing", "TEXT"), ("open_qty", "NUMERIC"), ("doc_qty", "NUMERIC"),
                ("box_qty", "NUMERIC"), ("unit2", "TEXT"), ("open_amount", "NUMERIC"), ("doc_amount", "NUMERIC"),
                ("tt_check", "TEXT"), ("bank", "TEXT"), ("usance", "TEXT"), ("at_sight", "TEXT"),
                ("open_date", "DATE"), ("lc_no", "TEXT"), ("invoice_no", "TEXT"), ("bl_no", "TEXT"),
                ("lg_no", "TEXT"), ("insurance", "TEXT"), ("customs_broker_date", "DATE"), ("etd", "DATE"),
                ("arrival_date", "DATE"), ("warehouse", "TEXT"), ("actual_in_qty", "NUMERIC"), ("destination", "TEXT"),
                ("doc_acceptance", "DATE"), ("acceptance_rate", "NUMERIC"), ("maturity_date", "DATE"),
                ("ext_maturity_date", "DATE"), ("acceptance_fee", "NUMERIC"), ("discount_fee", "NUMERIC"),
                ("payment_date", "DATE"), ("payment_amount", "NUMERIC"), ("exchange_rate", "NUMERIC"),
                ("balance", "NUMERIC"), ("avg_exchange_rate", "NUMERIC"),
                ("arrival_exchange_rate", "NUMERIC"), # 도착일 환율 (이미지 반영)
                ("clearance_info", "JSONB"), ("declaration_info", "JSONB"),
                ("remaining_qty", "NUMERIC"), ("transport_status", "TEXT"),
                ("status", "TEXT"), ("product_id", "INTEGER"), ("note", "TEXT"), ("quantity", "NUMERIC"), ("expected_date", "DATE")
            ]

            # 1. Import Schedules 테이블 업데이트
            for col_name, col_type in common_cols:
                s.execute(text(f"ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS {col_name} {col_type};"))

            # 2. Export Schedules 테이블 생성 (수입과 동일 구조)
            s.execute(text("""
                CREATE TABLE IF NOT EXISTS export_schedules (
                    id SERIAL PRIMARY KEY,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """))
            for col_name, col_type in common_cols:
                s.execute(text(f"ALTER TABLE export_schedules ADD COLUMN IF NOT EXISTS {col_name} {col_type};"))

            # 3. Triangular Trades 테이블 생성 (부가 정보 태그용)
            # ck_code, origin, product_name 등은 import_id로 찾을 수도 있지만, 스냅샷 성격으로 저장
            s.execute(text("""
                CREATE TABLE IF NOT EXISTS triangular_trades (
                    id SERIAL PRIMARY KEY,
                    import_id INTEGER,
                    ck_code TEXT,
                    importer TEXT,
                    origin TEXT,
                    product_name TEXT,
                    size TEXT,
                    packing TEXT,
                    open_qty NUMERIC,
                    unit TEXT,
                    open_amount NUMERIC,
                    invoice_no TEXT,
                    eta DATE,
                    payment_date DATE,
                    payment_amount NUMERIC,
                    exchange_rate NUMERIC,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """))

            # 4. 로그인/접근권한 (재고장 앱과 users 테이블을 공유) — 재고장의 session_token과
            # 충돌하지 않도록 이 앱 전용 세션 컬럼(import_session_token 등)을 별도로 둔다.
            try:
                s.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS import_access BOOLEAN DEFAULT FALSE;"))
                s.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS import_session_token TEXT;"))
                s.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS import_session_expiry TIMESTAMP;"))
            except Exception:
                pass

            # 5. 품목코드 체계: 품목은 '품명' 단위가 아니라 '품명+원산지+사이즈' 조합 단위로 코드가 부여됨
            # (예: 열빙어-캐나다-46/50과 열빙어-노르웨이-46/50은 서로 다른 코드). products에 원산지 컬럼 추가.
            try:
                s.execute(text("ALTER TABLE products ADD COLUMN IF NOT EXISTS origin TEXT;"))
            except Exception:
                pass

            s.commit()
        return True

    run_db_migrations()
except Exception as e:
    st.error(f"🚨 DB 연결 오류: .streamlit/secrets.toml을 확인하세요.\n{e}")
    st.stop()

# ==========================================
# 1. 데이터 조회 및 액션 함수
# ==========================================

@st.cache_data(ttl=600)
def get_products_df():
    """DB에 등록된 품목 리스트 조회 (재고관리 앱과 공유하는 products 테이블)"""
    try:
        with conn.session as s:
            df = pd.DataFrame(s.execute(text("SELECT product_id, product_name, product_code, category, unit, origin, size FROM products WHERE is_active = TRUE ORDER BY category, product_name")).fetchall())
            if not df.empty:
                df.columns = ['ID', '품목명', '품목코드', '카테고리', '단위', '원산지', '사이즈']
            return df
    except Exception:
        return pd.DataFrame()

def rollback_session():
    """DB 오류 발생 시 오염된 트랜잭션을 정리 (실패해도 무시)"""
    try:
        with conn.session as s: s.rollback()
    except Exception: pass

# --- 로그인 (재고장 앱과 users 테이블 공유, import_access 컬럼으로 접근 허용 여부만 구분) ---
def check_import_login(input_id, input_name):
    """사번+이름이 일치하고 import_access가 TRUE인 계정만 로그인 허용"""
    try:
        with conn.session as s:
            row = s.execute(text(
                "SELECT user_id, username, name, import_access FROM users WHERE username = :uid AND name = :uname"
            ), {"uid": str(input_id).strip(), "uname": str(input_name).strip()}).fetchone()
        if not row:
            return None, "사번 또는 이름이 일치하지 않습니다."
        if not row[3]:
            return None, "이 계정은 수입장부 접근 권한이 없습니다. 관리자에게 문의하세요."
        return {"user_id": row[0], "username": row[1], "name": row[2]}, None
    except Exception as e:
        return None, f"로그인 처리 중 오류: {e}"

def start_import_session(user_id):
    """로그인 성공 시 이 앱 전용 세션 토큰 발급 (재고장의 session_token과는 별개 컬럼)"""
    token = str(uuid.uuid4())
    expiry = get_kst_now() + timedelta(hours=8)
    try:
        with conn.session as s:
            s.execute(text("UPDATE users SET import_session_token = :t, import_session_expiry = :e WHERE user_id = :uid"),
                      {"t": token, "e": expiry, "uid": user_id})
            s.commit()
    except Exception:
        rollback_session()
    return token

def clear_import_session(user_id):
    try:
        with conn.session as s:
            s.execute(text("UPDATE users SET import_session_token = NULL, import_session_expiry = NULL WHERE user_id = :uid"), {"uid": user_id})
            s.commit()
    except Exception:
        rollback_session()

def try_restore_import_session(token):
    try:
        with conn.session as s:
            row = s.execute(text(
                "SELECT user_id, username, name, import_session_expiry FROM users WHERE import_session_token = :t AND import_access = TRUE"
            ), {"t": token}).fetchone()
        if not row: return None
        exp = row[3]
        if exp:
            if exp.tzinfo is None: exp = KST.localize(exp)
            if get_kst_now() > exp: return None
        return {"user_id": row[0], "username": row[1], "name": row[2]}
    except Exception:
        return None

def register_new_product(code, name, cat, unit):
    """신규 품목 DB 등록"""
    try:
        with conn.session as s:
            chk = s.execute(text("SELECT 1 FROM products WHERE product_code = :code"), {"code": code}).fetchone()
            if chk: return False, "이미 존재하는 품목코드입니다."
            s.execute(text("""
                INSERT INTO products (product_code, product_name, category, unit, is_active)
                VALUES (:code, :name, :cat, :unit, TRUE)
            """), {"code": code, "name": name, "cat": cat, "unit": unit})
            s.commit()
        get_products_df.clear()
        return True, "품목 등록 완료"
    except Exception as e:
        rollback_session()
        return False, str(e)

# --- 품목코드 매칭 (품명+원산지+사이즈 조합 단위로 코드가 부여됨) ---
def _norm_key(v):
    return str(v or '').replace(' ', '').strip().upper()

def resolve_product_match(p_df, name, origin, size):
    """품명+원산지+사이즈로 기존 품목(코드)을 정확히 매칭. 매칭되면 해당 row(Series), 없으면 None"""
    if p_df is None or p_df.empty or not str(name or '').strip():
        return None
    cand = p_df[p_df['품목명'].astype(str).str.strip() == str(name).strip()]
    if cand.empty:
        return None
    n_origin, n_size = _norm_key(origin), _norm_key(size)
    exact = cand[cand['원산지'].apply(_norm_key).eq(n_origin) & cand['사이즈'].apply(_norm_key).eq(n_size)]
    if not exact.empty:
        return exact.iloc[0]
    return None

def get_product_variants(p_df, name):
    """같은 품명의 기존 원산지/사이즈 조합(변형) 목록 - 신규건 여부 판단 및 참고용"""
    if p_df is None or p_df.empty or not str(name or '').strip():
        return p_df.iloc[0:0] if p_df is not None else pd.DataFrame()
    return p_df[p_df['품목명'].astype(str).str.strip() == str(name).strip()]

def generate_new_temp_code():
    """완전 신규 품명+원산지+사이즈 조합에 부여할 임시 코드 (NEW-0001 형식). ERP 쪽 실제 코드 체계와
    섞이지 않도록 'NEW-' 접두사로 명확히 구분해두고, 나중에 실제 코드로 정정하도록 유도한다."""
    try:
        with conn.session as s:
            rows = s.execute(text("SELECT product_code FROM products WHERE product_code LIKE 'NEW-%'")).fetchall()
        nums = []
        for r in rows:
            m = re.match(r'^NEW-(\d+)$', str(r[0] or ''))
            if m: nums.append(int(m.group(1)))
        nxt = max(nums) + 1 if nums else 1
        return f"NEW-{nxt:04d}"
    except Exception:
        return f"NEW-{int(time.time()) % 10000:04d}"

def register_product_variant(name, origin, size, category=None, unit=None):
    """완전 신규 품명+원산지+사이즈 조합을 위한 품목 자동 생성 (임시 코드 부여)"""
    code = generate_new_temp_code()
    try:
        with conn.session as s:
            s.execute(text("""
                INSERT INTO products (product_code, product_name, origin, size, category, unit, is_active)
                VALUES (:code, :name, :origin, :size, :cat, :unit, TRUE)
            """), {"code": code, "name": name, "origin": origin or None, "size": size or None,
                   "cat": category or '미분류', "unit": unit or 'EA'})
            s.commit()
        get_products_df.clear()
        return True, code, None
    except Exception as e:
        rollback_session()
        return False, None, str(e)

def upsert_product_code_master(rows):
    """품목코드자료(품목코드/제품명/원산지/사이즈) 엑셀을 products에 일괄 반영.
    코드가 이미 있으면 품명/원산지/사이즈를 갱신, 없으면 신규 등록."""
    ok_cnt, fail = 0, []
    try:
        with conn.session as s:
            for r in rows:
                code = str(r.get('code') or '').strip()
                name = str(r.get('name') or '').strip()
                if not code or not name:
                    continue
                try:
                    exist = s.execute(text("SELECT product_id FROM products WHERE product_code = :code"), {"code": code}).fetchone()
                    if exist:
                        s.execute(text("""
                            UPDATE products SET product_name=:name, origin=:origin, size=:size WHERE product_code=:code
                        """), {"name": name, "origin": r.get('origin'), "size": r.get('size'), "code": code})
                    else:
                        s.execute(text("""
                            INSERT INTO products (product_code, product_name, origin, size, category, unit, is_active)
                            VALUES (:code, :name, :origin, :size, :cat, :unit, TRUE)
                        """), {"code": code, "name": name, "origin": r.get('origin'), "size": r.get('size'),
                               "cat": r.get('category') or '미분류', "unit": r.get('unit') or 'EA'})
                    ok_cnt += 1
                except Exception as e:
                    fail.append(f"{code} {name}: {e}")
            s.commit()
        get_products_df.clear()
        return ok_cnt, fail
    except Exception as e:
        rollback_session()
        return ok_cnt, fail + [str(e)]

@st.cache_data(ttl=30)
def get_schedule_data(table_name='import_schedules', status_filter='ALL'):
    """데이터 조회 (수입/수출 공용). 저장/삭제 시 get_schedule_data.clear()로 무효화됨"""
    with conn.session as s:
        # 수입인 경우 삼각무역 태그 존재 여부 확인
        extra_col = ""
        if table_name == 'import_schedules':
            extra_col = ", (SELECT COUNT(*) FROM triangular_trades WHERE import_id = s.id) as tri_cnt"

        base_sql = f"""
            SELECT s.*, p.product_name, p.product_code as db_prod_code, p.unit as p_unit{extra_col}
            FROM {table_name} s
            LEFT JOIN products p ON s.product_id = p.product_id
        """
        params = {}
        if status_filter != 'ALL':
            base_sql += " WHERE s.status = :status_filter"
            params['status_filter'] = status_filter

        base_sql += " ORDER BY s.expected_date ASC, s.id DESC"

        df = pd.DataFrame(s.execute(text(base_sql), params).fetchall())
        return df

def sync_import_to_inventory(sid):
    """수입 일정 -> 재고 동기화 (수입 전용)"""
    try:
        with conn.session as s:
            sch = s.execute(text("SELECT * FROM import_schedules WHERE id = :sid"), {"sid": sid}).mappings().fetchone()
            if not sch: return False, "일정 정보를 찾을 수 없습니다."
            
            def to_date(d):
                if isinstance(d, str):
                    try: return datetime.strptime(d, '%Y-%m-%d').date()
                    except: return None
                return d
            
            def to_float(v):
                try: return float(v) if v else 0.0
                except: return 0.0

            if sch['status'] == 'ARRIVED':
                missing_fields = []
                qty = 0.0
                if to_float(sch.get('actual_in_qty')) > 0: qty = to_float(sch.get('actual_in_qty'))
                elif to_float(sch.get('open_qty')) > 0: qty = to_float(sch.get('open_qty'))
                elif to_float(sch.get('quantity')) > 0: qty = to_float(sch.get('quantity'))
                
                if qty <= 0: missing_fields.append("수량(실입고, 오픈, 또는 기본수량)")
                entry_date = to_date(sch.get('arrival_date')) or to_date(sch.get('expected_date'))
                if not entry_date: missing_fields.append("입고일(실입고일 또는 ETA)")

                if missing_fields: return False, f"필수 정보 누락: {', '.join(missing_fields)}"
                
                prod = s.execute(text("SELECT category, unit FROM products WHERE product_id = :pid"), {"pid": sch['product_id']}).fetchone()
                cat = prod[0] if prod else '기타'
                unit = prod[1] if prod else 'Box'
                lot_no = entry_date.strftime("%Y-%m-%d")
                wh = sch.get('warehouse') if sch.get('warehouse') else '미정'
                ck_code_val = sch.get('ck_code') or '-'
                note_text = f"수입도착({ck_code_val}) {sch.get('note', '')}"
                price = to_float(sch.get('unit_price'))

                check = s.execute(text("""
                    SELECT stock_id FROM stock_by_lot 
                    WHERE product_id = :pid AND lot_number = :lot AND quantity = :qty AND is_cleared = FALSE
                """), {"pid": sch['product_id'], "lot": lot_no, "qty": qty}).fetchone()
                
                if not check:
                    s.execute(text("""
                        INSERT INTO stock_by_lot 
                        (product_id, lot_number, quantity, entry_date, warehouse_loc, manufacturer, unit_price, size, note, category, unit, is_cleared)
                        VALUES (:pid, :lot, :qty, :ed, :wh, :man, :price, :size, :note, :cat, :unit, FALSE)
                    """), {
                        "pid": sch['product_id'], "lot": lot_no, "qty": qty, "ed": entry_date, "wh": wh,
                        "man": sch.get('supplier', ''), "price": price, "size": sch.get('size', ''),
                        "note": note_text, "cat": cat, "unit": unit
                    })
                    
                    s.execute(text("""
                        INSERT INTO transactions 
                        (trans_type, product_id, lot_number, quantity, manager_id, remarks, status, trans_date) 
                        VALUES ('IN', :pid, :lot, :qty, (SELECT user_id FROM users LIMIT 1), '수입도착(미통관)', 'VALID', NOW())
                    """), {"pid": sch['product_id'], "lot": lot_no, "qty": qty})
                    s.commit()
                    return True, "재고(미통관) 등록 완료"
                else: return True, "이미 등록된 재고입니다."

            else:
                e_date = to_date(sch.get('arrival_date')) or to_date(sch.get('expected_date'))
                if not e_date: return True, "삭제할 대상 날짜 없음"
                l_no = e_date.strftime("%Y-%m-%d")
                ck_code_val = sch.get('ck_code') or '-'
                note_pattern = f"수입도착({ck_code_val})%"
                s.execute(text("DELETE FROM stock_by_lot WHERE product_id = :pid AND lot_number = :lot AND note LIKE :note AND is_cleared = FALSE"), {"pid": sch['product_id'], "lot": l_no, "note": note_pattern})
                s.commit()
                return True, "관련 재고 삭제 완료 (롤백)"
    except Exception as e:
        rollback_session()
        return False, f"동기화 오류: {str(e)}"

def save_schedule(data, sid=None, table_name='import_schedules'):
    """상세 정보 저장 (수입/수출 공용)"""
    try:
        with conn.session as s:
            cols = [
                'product_id', 'expected_date', 'quantity', 'note', 'status', 'size', 'supplier', 'unit_price', 'ck_code',
                'global_code', 'doojin_code', 'agency', 'agency_contract', 'origin', 'packing', 
                'open_qty', 'doc_qty', 'box_qty', 'unit2', 'open_amount', 'doc_amount',
                'tt_check', 'bank', 'usance', 'at_sight', 'open_date', 'lc_no', 'invoice_no', 'bl_no', 'lg_no', 'insurance',
                'customs_broker_date', 'etd', 'arrival_date', 'warehouse', 'actual_in_qty', 'destination',
                'doc_acceptance', 'acceptance_rate', 'maturity_date', 'ext_maturity_date', 'acceptance_fee', 'discount_fee',
                'payment_date', 'payment_amount', 'exchange_rate', 'balance', 'avg_exchange_rate', 'arrival_exchange_rate',
                'clearance_info', 'declaration_info', 'remaining_qty', 'transport_status'
            ]
            numeric_cols = ['quantity', 'unit_price', 'open_qty', 'doc_qty', 'box_qty', 'open_amount', 'doc_amount',
                            'actual_in_qty', 'acceptance_rate', 'acceptance_fee', 'discount_fee', 'payment_amount',
                            'exchange_rate', 'balance', 'avg_exchange_rate', 'arrival_exchange_rate', 'remaining_qty']
            json_cols = ['clearance_info', 'declaration_info']

            # [중요] data에 실제로 들어있는 컬럼만 갱신한다. 전체 cols를 기준으로 SET하면
            # (예: 수출 탭에서 셀 하나만 바꿔 보낸 diff) 안 보낸 나머지 컬럼이 전부 0/NULL로 덮어써진다.
            sid_int = None
            try: sid_int = int(sid) if sid is not None and str(sid).strip() != '' else None
            except (TypeError, ValueError): sid_int = None

            use_cols = [c for c in cols if c in data]
            if sid_int is None and 'status' not in use_cols:
                data = {**data, 'status': 'PENDING'}
                use_cols.append('status')
            if not use_cols:
                return False, "저장할 변경 내용이 없습니다."

            params = {}
            for k in use_cols:
                val = data.get(k)
                if k in numeric_cols:
                    if val is None or str(val).strip() == '': params[k] = 0
                    else:
                        try: params[k] = float(str(val).replace(',', '').strip())
                        except: params[k] = 0
                elif k in json_cols:
                    if isinstance(val, (list, dict)): params[k] = json.dumps(val, ensure_ascii=False)
                    elif isinstance(val, str) and (val.startswith('[') or val.startswith('{')): params[k] = val
                    else: params[k] = '[]'
                else:
                    if val is None or str(val).strip() == '' or str(val).lower() == 'nan': params[k] = None
                    else: params[k] = val

            target_id = None
            if sid_int is not None:
                set_clause = ", ".join([f"{c} = CAST(:{c} AS JSONB)" if c in json_cols else f"{c} = :{c}" for c in use_cols])
                s.execute(text(f"UPDATE {table_name} SET {set_clause} WHERE id = :id"), {**params, "id": sid_int})
                target_id = sid_int
            else:
                col_str = ", ".join(use_cols)
                val_str = ", ".join([f"CAST(:{c} AS JSONB)" if c in json_cols else f":{c}" for c in use_cols])
                res = s.execute(text(f"INSERT INTO {table_name} ({col_str}) VALUES ({val_str}) RETURNING id"), params)
                target_id = res.fetchone()[0]
            s.commit()

        get_schedule_data.clear()

        if table_name == 'import_schedules' and params.get('status') == 'ARRIVED' and target_id:
            ok, msg = sync_import_to_inventory(target_id)
            if not ok:
                with conn.session as s:
                    s.execute(text(f"UPDATE {table_name} SET status = 'PENDING' WHERE id = :id"), {"id": target_id})
                    s.commit()
                get_schedule_data.clear()
                return False, f"저장되었으나 재고생성 실패: {msg}"

        return True, "저장 완료"
    except Exception as e:
        rollback_session()
        return False, str(e)

def delete_schedule(sid, table_name='import_schedules'):
    try:
        with conn.session as s:
            s.execute(text(f"DELETE FROM {table_name} WHERE id = :sid"), {"sid": sid})
            s.commit()
        get_schedule_data.clear()
        return True, "삭제 완료"
    except Exception as e:
        rollback_session()
        return False, str(e)

def save_editor_changes(edited_rows, original_df, table_name='export_schedules'):
    """st.data_editor 변경사항 DB 저장"""
    try:
        success_cnt = 0
        for idx, changes in edited_rows.items():
            row_data = original_df.iloc[idx].to_dict()
            row_data.update(changes)
            ok, msg = save_schedule(row_data, row_data['id'], table_name)
            if ok: success_cnt += 1
        return True, f"{success_cnt}건 수정 완료"
    except Exception as e: return False, str(e)

# --- 삼각무역 전용 함수 ---
@st.cache_data(ttl=30)
def get_triangular_trades(import_id):
    """특정 수입 건에 연결된 삼각무역 태그 조회 (건당 여러 개 가능)"""
    try:
        with conn.session as s:
            df = pd.DataFrame(s.execute(text("SELECT * FROM triangular_trades WHERE import_id = :id ORDER BY id"), {"id": import_id}).fetchall())
            return df
    except Exception: return pd.DataFrame()

@st.cache_data(ttl=60)
def get_all_triangular_trades():
    """전체 삼각무역 태그 조회 (조회/검색용)"""
    try:
        with conn.session as s:
            df = pd.DataFrame(s.execute(text("""
                SELECT t.id, t.import_id, t.ck_code, t.origin, t.product_name, t.importer, t.size, t.packing,
                       t.open_qty, t.unit, t.open_amount, t.invoice_no, t.eta, t.payment_date, t.payment_amount, t.exchange_rate
                FROM triangular_trades t ORDER BY t.id DESC
            """)).fetchall())
            if not df.empty:
                df.columns = ['ID', '수입ID', 'CK관리번호', '원산지', '품명', '수입자', '사이즈', 'Packing',
                              '오픈수량', '단위', '오픈금액', 'InvoiceNo', 'ETA', '결제일', '결제금액', '환율']
            return df
    except Exception: return pd.DataFrame()

def save_triangular_trade(data, target_id=None):
    """삼각무역 태그 저장 (INSERT or UPDATE). 하나의 수입 건에 여러 개의 태그를 연결할 수 있다."""
    try:
        with conn.session as s:
            cols = ['import_id', 'ck_code', 'importer', 'origin', 'product_name', 'size', 'packing', 
                    'open_qty', 'unit', 'open_amount', 'invoice_no', 'eta', 'payment_date', 'payment_amount', 'exchange_rate']
            
            params = {}
            for k in cols:
                val = data.get(k)
                if k in ['open_qty', 'open_amount', 'payment_amount', 'exchange_rate']:
                    try: params[k] = float(str(val).replace(',', '').strip()) if val else 0.0
                    except: params[k] = 0.0
                elif k in ['eta', 'payment_date']:
                    params[k] = val if val else None
                else:
                    params[k] = val if val else None

            if target_id:
                # Update
                set_clause = ", ".join([f"{c} = :{c}" for c in cols])
                sql = f"UPDATE triangular_trades SET {set_clause} WHERE id = :id"
                params['id'] = target_id
                s.execute(text(sql), params)
                msg = "수정 완료"
            else:
                # Insert
                col_str = ", ".join(cols)
                val_str = ", ".join([f":{c}" for c in cols])
                s.execute(text(f"INSERT INTO triangular_trades ({col_str}) VALUES ({val_str})"), params)
                msg = "등록 완료"
                
            s.commit()
        get_all_triangular_trades.clear()
        get_triangular_trades.clear()
        return True, msg
    except Exception as e:
        rollback_session()
        return False, str(e)

def delete_triangular_trade(tid):
    try:
        with conn.session as s:
            s.execute(text("DELETE FROM triangular_trades WHERE id = :id"), {"id": tid})
            s.commit()
        get_all_triangular_trades.clear()
        get_triangular_trades.clear()
        return True, "삭제 완료"
    except Exception as e:
        rollback_session()
        return False, str(e)

# --- 유틸리티 ---
def safe_date_parse(val):
    if pd.isna(val) or str(val).strip() == '': return None
    try:
        if isinstance(val, datetime): return val.strftime('%Y-%m-%d')
        s_val = str(val).strip()
        if re.match(r'^\d{2}/\d{2}/\d{2}$', s_val): # 25/01/01
            dt = datetime.strptime(s_val, "%y/%m/%d")
            if dt.year < 2000: dt = dt.replace(year=dt.year+2000)
            return dt.strftime('%Y-%m-%d')
        if re.match(r'^\d{4}-\d{2}-\d{2}$', s_val): return datetime.strptime(s_val, "%Y-%m-%d").strftime('%Y-%m-%d')
        return pd.to_datetime(val).strftime('%Y-%m-%d')
    except: return None

def parse_json_field(val):
    """JSONB 컬럼은 psycopg2가 이미 list/dict로 파싱해서 돌려주므로, 문자열일 때만 json.loads 적용"""
    if isinstance(val, (list, dict)): return val
    if isinstance(val, str) and val.strip():
        try: return json.loads(val)
        except Exception: return []
    return []

def safe_float_parse(val):
    if pd.isna(val) or str(val).strip() == '': return 0.0
    try: return float(str(val).replace(',', '').replace(' ', '').strip())
    except: return 0.0

def find_col_loose(columns, keywords):
    """엑셀 업로드 시 헤더명이 정확히 일치하지 않아도 키워드 포함으로 컬럼을 찾는다"""
    for c in columns:
        c_clean = str(c).replace(' ', '').upper()
        for k in keywords:
            if k.replace(' ', '').upper() in c_clean:
                return c
    return None

def _esc(v, default='-'):
    return _html.escape(str(v)) if pd.notna(v) and str(v).strip() else default

STATUS_LABEL = {'PENDING': ('진행중', 'status-pending'), 'ARRIVED': ('입고완료', 'status-arrived'), 'CANCELED': ('취소', 'status-canceled')}

def render_progress_cards(df):
    """수입진행현황 모바일 카드뷰: 착지 > 품명/사이즈 카드 > 입고 건(날짜/수출자·단가/수량)"""
    out = ""
    multi_dest = df['destination'].nunique() > 1
    for dest, d_group in df.groupby('destination', dropna=False):
        if multi_dest or str(dest).strip() != '기본 착지':
            out += f"<div class='m-dest'>📍 착지 : {_esc(dest)}</div>"
        for (p_name, size), p_group in d_group.groupby(['product_name', 'size'], dropna=False):
            out += f"<div class='m-card'><div class='m-card-head'><b>{_esc(p_name)}</b><span class='sub'>{_esc(size)}</span></div>"
            for _, row in p_group.sort_values(by='expected_date').iterrows():
                eta = pd.to_datetime(row['expected_date']).strftime('%y/%m/%d') if pd.notna(row['expected_date']) else '-'
                qty = f"{int(row['quantity']):,}" if pd.notna(row['quantity']) else "0"
                pv = row.get('unit_price')
                price = f"${float(pv):,.2f}" if pd.notna(pv) and float(pv) > 0 else ''
                sup = _esc(row.get('supplier'))
                cls = {'PENDING': 'cell-pending', 'ARRIVED': 'cell-arrived'}.get(row['status'], '')
                out += f"<div class='m-ship {cls}'><span class='d'>{eta}</span><span class='s'>{sup} {price}</span><span class='q'>{qty}</span></div>"
            out += "</div>"
    st.markdown(out, unsafe_allow_html=True)

def render_ledger_cards(df, page_key, on_select, page_size=20):
    """수입장부 모바일 카드뷰: 핵심 항목만 요약 + 상세/수정 버튼, '더 보기'로 페이지 확장"""
    shown = st.session_state.get(page_key, page_size)
    st.caption(f"총 {len(df)}건 중 {min(shown, len(df))}건 표시")
    for _, row in df.head(shown).iterrows():
        label, cls = STATUS_LABEL.get(row.get('status'), ('-', ''))
        eta = pd.to_datetime(row.get('expected_date'), errors='coerce')
        eta_s = eta.strftime('%y/%m/%d') if pd.notna(eta) else '-'
        qty = f"{float(row['quantity']):,.0f}" if pd.notna(row.get('quantity')) else '-'
        pv = row.get('unit_price')
        price = f"${float(pv):,.2f}" if pd.notna(pv) and float(pv) > 0 else '-'
        tri = " 🔺삼각" if pd.notna(row.get('tri_cnt')) and row.get('tri_cnt') > 0 else ""
        st.markdown(f"""
        <div class='m-card' style='margin-bottom:4px'>
          <div class='m-card-head'><b>{_esc(row.get('product_name'))} <span style='font-weight:400'>{_esc(row.get('size'), '')}</span>{tri}</b>
            <span class='status-badge {cls}' style='padding:2px 8px'>{label}</span></div>
          <div class='m-kv'>
            <div><div class='k'>CK번호</div>{_esc(row.get('ck_code'))}</div><div><div class='k'>수출자</div>{_esc(row.get('supplier'))}</div>
            <div><div class='k'>ETA</div>{eta_s}</div><div><div class='k'>수량 / 단가</div>{qty} / {price}</div>
            <div><div class='k'>Invoice</div>{_esc(row.get('invoice_no'))}</div><div><div class='k'>착지</div>{_esc(row.get('destination'))}</div>
          </div>
        </div>""", unsafe_allow_html=True)
        if st.button("상세/수정", key=f"{page_key}_open_{row['id']}", use_container_width=True):
            on_select(row)
    if shown < len(df):
        if st.button(f"더 보기 (+{page_size})", key=f"{page_key}_more", use_container_width=True):
            st.session_state[page_key] = shown + page_size
            st.rerun()

def render_search_filters(df, key_prefix):
    """업체(공급/수출자) / 품목 / 도착일(ETA) 기준 조회 필터 위젯을 그리고, 필터링된 df를 반환"""
    if df.empty: return df

    if IS_MOBILE:
        c1, c2 = st.columns(2)
        c3, c4 = st.columns(2)
    else:
        c1, c2, c3, c4 = st.columns(4)
    supplier_opts = ['전체']
    if 'supplier' in df.columns:
        supplier_opts += sorted([str(x) for x in df['supplier'].dropna().unique() if str(x).strip()])
    product_opts = ['전체']
    if 'product_name' in df.columns:
        product_opts += sorted([str(x) for x in df['product_name'].dropna().unique() if str(x).strip()])

    sel_supplier = c1.selectbox("업체", supplier_opts, key=f"{key_prefix}_supplier")
    sel_product = c2.selectbox("품목", product_opts, key=f"{key_prefix}_product")
    date_from = c3.date_input("도착일(부터)", value=None, key=f"{key_prefix}_date_from")
    date_to = c4.date_input("도착일(까지)", value=None, key=f"{key_prefix}_date_to")

    out = df
    if sel_supplier != '전체' and 'supplier' in out.columns:
        out = out[out['supplier'].astype(str) == sel_supplier]
    if sel_product != '전체' and 'product_name' in out.columns:
        out = out[out['product_name'].astype(str) == sel_product]
    if 'expected_date' in out.columns and (date_from or date_to):
        dcol = pd.to_datetime(out['expected_date'], errors='coerce').dt.date
        if date_from: out = out[dcol.notna() & (dcol >= date_from)]
        if date_to: out = out[dcol.notna() & (dcol <= date_to)]
    return out

# --- 엑셀 파싱 함수 (복원) ---
def parse_import_full_excel(df):
    """'수입' 탭(상세 장부) 구조의 엑셀/CSV 파일 파싱"""
    valid_data = []
    errors = []
    notices = []  # 신규 품목코드 자동 생성 등 참고용 안내 (에러 아님)

    p_df = get_products_df()

    keywords = ['CK', '관리번호', '품명', '수량', '단가', '글로벌', '두진', '입고일', 'ETA']
    
    def clean_str(s):
        return str(s).replace('\n', '').replace('\r', '').replace(' ', '').upper().strip()

    data_df = pd.DataFrame()
    header_row_idx = -1
    
    # 헤더 찾기 로직
    col_str = "".join([clean_str(c) for c in df.columns])
    score_cols = sum(1 for k in keywords if k in col_str)
    
    if score_cols >= 2 and (('CK' in col_str or '관리번호' in col_str) and '품명' in col_str):
        data_df = df
    else:
        if df.empty: return [], ["파일 내용이 없습니다."], []
        max_score = 0
        for i in range(min(20, len(df))):
            row_vals = [clean_str(x) for x in df.iloc[i].values if pd.notna(x)]
            row_str = "".join(row_vals)
            score = sum(1 for k in keywords if k in row_str)
            if score > max_score and score >= 2:
                max_score = score
                header_row_idx = i
                
        if header_row_idx != -1:
            df.columns = df.iloc[header_row_idx]
            data_df = df.iloc[header_row_idx+1:].reset_index(drop=True)
        else:
            return [], ["헤더를 찾을 수 없습니다."], []

    data_df.columns = [clean_str(c) for c in data_df.columns]
    cols = list(data_df.columns)
    
    def find_col(keywords):
        for c in cols:
            for k in keywords:
                if k.replace(" ", "").upper() in c: return c
        return None

    col_map = {
        'ck': find_col(['CK', '관리번호']), 'global': find_col(['글로벌']), 'doojin': find_col(['두진']),
        'agency': find_col(['대행']), 'agency_contract': find_col(['대행계약서']),
        'supplier': find_col(['수출자', '수입자']), 'origin': find_col(['원산지']), 'name': find_col(['품명']),
        'size': find_col(['사이즈']), 'packing': find_col(['Packing']), 'open_qty': find_col(['오픈수량']),
        'unit': find_col(['단위']), 'doc_qty': find_col(['서류수량']), 'box_qty': find_col(['박스수량']),
        'price': find_col(['단가']), 'open_amt': find_col(['오픈금액']), 'doc_amt': find_col(['서류금액']),
        'tt': find_col(['T/T']), 'bank': find_col(['은행']), 'usance': find_col(['Usance']), 'at_sight': find_col(['AtSight']),
        'open_date': find_col(['개설일']), 'lc_no': find_col(['LCNo', 'L/C']), 'inv_no': find_col(['Invoice']),
        'bl_no': find_col(['BLNo', 'B/L']), 'lg_no': find_col(['LG', 'L/G']), 'insurance': find_col(['보험']),
        'broker_date': find_col(['관세사']), 'etd': find_col(['ETD']), 'eta': find_col(['ETA']),
        'arrival_date': find_col(['입고일']), 'wh': find_col(['창고']), 'real_in_qty': find_col(['실입고']),
        'dest': find_col(['착지']), 'note': find_col(['비고']), 'doc_acc': find_col(['서류인수']),
        'acc_rate': find_col(['인수수수료율']), 'mat_date': find_col(['만기일']), 'ext_date': find_col(['연장만기일']),
        'acc_fee': find_col(['인수수수료']), 'dis_fee': find_col(['인수할인료']), 'pay_date': find_col(['결제일']),
        'pay_amt': find_col(['결제금액']), 'ex_rate': find_col(['환율']), 'balance': find_col(['잔액']), 'avg_ex': find_col(['평균환율'])
    }
    
    if col_map['agency'] and '계약서' in str(col_map['agency']):
        col_map['agency'] = None
        for c in cols:
            if '대행' in c and '계약서' not in c: col_map['agency'] = c; break

    try:
        if col_map['price']:
            idx = cols.index(col_map['price'])
            col_map['unit2'] = cols[idx+1] if idx + 1 < len(cols) else None
        else: col_map['unit2'] = None
    except: col_map['unit2'] = None

    # 통관(최대 6건)/수입신고(최대 5건) 반복 컬럼은 매번 번호 표기가 달라(통관일자, 통관일자2, ...
    # 심지어 신고번호처럼 숫자가 어긋나는 경우도 있음) 키워드 매칭 대신 '통관일자' 앵커 위치 기준
    # 고정 폭(3칸씩 6세트 + 잔량)으로 잘라내고, 그 뒤를 신고정보(2칸씩)로 처리한다.
    def get_col_by_pos(i):
        return cols[i] if 0 <= i < len(cols) else None

    clearance_col_groups = []
    remaining_col = None
    declaration_col_groups = []
    try:
        clr_start = cols.index('통관일자')
        for i in range(6):
            base = clr_start + i * 3
            d_col, q_col, r_col = get_col_by_pos(base), get_col_by_pos(base + 1), get_col_by_pos(base + 2)
            if d_col is None: break
            clearance_col_groups.append((d_col, q_col, r_col))
        remaining_col = get_col_by_pos(clr_start + 18)
        decl_start = clr_start + 19
    except ValueError:
        remaining_col = find_col(['잔량'])
        decl_start = None

    transport_col = find_col(['운송현황'])
    if decl_start is not None:
        transport_idx = cols.index(transport_col) if transport_col in cols else len(cols)
        i = decl_start
        while i + 1 < transport_idx and len(declaration_col_groups) < 5:
            declaration_col_groups.append((cols[i], cols[i + 1]))
            i += 2

    for idx, row in data_df.iterrows():
        if not col_map['name']: continue
        name_val = str(row.get(col_map['name'], '')).strip()
        if not name_val or name_val.lower() == 'nan': continue

        origin_col, size_col = col_map.get('origin'), col_map.get('size')
        origin_val = str(row.get(origin_col, '') or '').strip() if origin_col else ''
        size_val = str(row.get(size_col, '') or '').strip() if size_col else ''

        # 품명+원산지+사이즈 조합으로 기존 품목코드 매칭. 일치하는 코드가 없으면
        # (품명 자체가 없거나, 품명은 있어도 원산지/사이즈 조합이 새로운 경우 모두) 임시 코드를 자동 생성한다.
        match = resolve_product_match(p_df, name_val, origin_val, size_val)
        if match is not None:
            pid = match['ID']
        else:
            variants = get_product_variants(p_df, name_val)
            inherit_cat = str(variants.iloc[0]['카테고리']) if not variants.empty and pd.notna(variants.iloc[0]['카테고리']) else None
            inherit_unit = str(variants.iloc[0]['단위']) if not variants.empty and pd.notna(variants.iloc[0]['단위']) else None
            ok_np, new_code, err_np = register_product_variant(name_val, origin_val, size_val, inherit_cat, inherit_unit)
            if not ok_np:
                errors.append(f"[행 {idx+2}] 신규 품목코드 생성 실패: '{name_val}' - {err_np}")
                continue
            p_df = get_products_df()
            new_row = p_df[p_df['품목코드'] == new_code]
            pid = new_row.iloc[0]['ID'] if not new_row.empty else None
            notices.append(f"[행 {idx+2}] 🆕 신규 품목코드 자동 생성: '{name_val}' / 원산지='{origin_val}' / 사이즈='{size_val}' → {new_code}")

        try:
            def get_val(key, parser=str):
                col = col_map.get(key)
                return parser(row.get(col)) if col else (0.0 if parser == safe_float_parse else None)

            clearance_list = []
            for d_col, q_col, r_col in clearance_col_groups:
                d_val = safe_date_parse(row.get(d_col)) if d_col else None
                q_val = safe_float_parse(row.get(q_col)) if q_col else 0.0
                r_val = safe_float_parse(row.get(r_col)) if r_col else 0.0
                if d_val or q_val: clearance_list.append({"date": d_val, "qty": q_val, "rate": r_val})

            declaration_list = []
            for d_col, n_col in declaration_col_groups:
                d_val = safe_date_parse(row.get(d_col)) if d_col else None
                n_raw = row.get(n_col) if n_col else None
                n_val = str(n_raw).strip() if pd.notna(n_raw) else ''
                if d_val or n_val: declaration_list.append({"date": d_val, "no": n_val})

            data = {
                'product_id': pid, 'ck_code': get_val('ck'),
                'global_code': get_val('global'), 'doojin_code': get_val('doojin'),
                'agency': get_val('agency'), 'agency_contract': get_val('agency_contract'),
                'supplier': get_val('supplier'), 'origin': get_val('origin'),
                'size': get_val('size'), 'packing': get_val('packing'),
                'open_qty': get_val('open_qty', safe_float_parse),
                'quantity': get_val('open_qty', safe_float_parse),
                'doc_qty': get_val('doc_qty', safe_float_parse),
                'box_qty': get_val('box_qty', safe_float_parse),
                'unit2': get_val('unit2'),
                'unit_price': get_val('price', safe_float_parse),
                'open_amount': get_val('open_amt', safe_float_parse),
                'doc_amount': get_val('doc_amt', safe_float_parse),
                'tt_check': get_val('tt'), 'bank': get_val('bank'),
                'usance': get_val('usance'), 'at_sight': get_val('at_sight'),
                'open_date': get_val('open_date', safe_date_parse),
                'lc_no': get_val('lc_no'), 'invoice_no': get_val('inv_no'),
                'bl_no': get_val('bl_no'), 'lg_no': get_val('lg_no'), 'insurance': get_val('insurance'),
                'customs_broker_date': get_val('broker_date', safe_date_parse),
                'etd': get_val('etd', safe_date_parse),
                'expected_date': get_val('eta', safe_date_parse) or get_kst_today(),
                'arrival_date': get_val('arrival_date', safe_date_parse),
                'warehouse': get_val('wh'), 
                'actual_in_qty': get_val('real_in_qty', safe_float_parse),
                'destination': get_val('dest'), 'note': get_val('note'),
                'doc_acceptance': get_val('doc_acc', safe_date_parse),
                'acceptance_rate': get_val('acc_rate', safe_float_parse),
                'maturity_date': get_val('mat_date', safe_date_parse),
                'ext_maturity_date': get_val('ext_date', safe_date_parse),
                'acceptance_fee': get_val('acc_fee', safe_float_parse),
                'discount_fee': get_val('dis_fee', safe_float_parse),
                'payment_date': get_val('pay_date', safe_date_parse),
                'payment_amount': get_val('pay_amt', safe_float_parse),
                'exchange_rate': get_val('ex_rate', safe_float_parse),
                'balance': get_val('balance', safe_float_parse),
                'avg_exchange_rate': get_val('avg_ex', safe_float_parse),
                'remaining_qty': safe_float_parse(row.get(remaining_col)) if remaining_col else 0.0,
                'transport_status': str(row.get(transport_col)).strip() if transport_col and pd.notna(row.get(transport_col)) else None,
                'clearance_info': clearance_list,
                'declaration_info': declaration_list,
                'status': 'PENDING'
            }
            valid_data.append(data)
        except Exception as e:
            errors.append(f"[행 {idx+2}] 파싱 오류: {str(e)}")
            
    return valid_data, errors, notices

# ==========================================
# 1-1. 로그인 게이트
# ==========================================
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

# 새로고침해도 로그인 유지 (URL의 세션 토큰으로 복원)
if not st.session_state['logged_in']:
    _token = st.query_params.get("isession")
    if _token:
        _restored = try_restore_import_session(_token)
        if _restored:
            st.session_state['logged_in'] = True
            st.session_state['import_user'] = _restored

if not st.session_state['logged_in']:
    st.title("🚢 수입/수출 통합 관리 시스템")
    st.markdown("### 🔐 로그인")
    st.caption("재고장과 동일한 사번/이름을 사용합니다. 접근 권한이 없는 계정은 관리자에게 문의하세요.")
    with st.form("import_login_form"):
        c1, c2 = st.columns(2)
        in_id = c1.text_input("사번")
        in_name = c2.text_input("이름")
        if st.form_submit_button("로그인", type="primary", use_container_width=True):
            user, err = check_import_login(in_id, in_name)
            if user:
                token = start_import_session(user['user_id'])
                st.session_state['logged_in'] = True
                st.session_state['import_user'] = user
                st.query_params['isession'] = token
                st.rerun()
            else:
                st.error(err)
    st.stop()

with st.sidebar:
    st.success(f"👤 {st.session_state['import_user']['name']}님")
    if st.button("🚪 로그아웃", use_container_width=True):
        clear_import_session(st.session_state['import_user']['user_id'])
        st.session_state['logged_in'] = False
        st.session_state['import_user'] = None
        st.query_params.clear()
        st.rerun()

# ==========================================
# 2. 메인 UI 구성 (st.radio로 탭 대체 - Key 기반)
# ==========================================

st.title("🚢 수입/수출 통합 관리 시스템")

# 탭 메뉴 정의
MENU_OPTIONS = [
    "📊 수입진행상황", 
    "📒 수입장부 (상세)", 
    "📤 수출 (Export)", 
    "📐 삼각무역 (Triangular)",
    "📝 수입 등록/관리", 
    "📦 품목 관리"
]

# [핵심] 네비게이션 상태 분리 (active_tab: 논리적 탭 / nav_radio: 위젯 키)
if 'active_tab' not in st.session_state:
    st.session_state['active_tab'] = MENU_OPTIONS[0]

# 탭 변경 콜백 (위젯 -> 상태 동기화)
def on_tab_change():
    st.session_state['active_tab'] = st.session_state['nav_radio']

# 현재 상태에 맞는 인덱스 계산
try:
    current_idx = MENU_OPTIONS.index(st.session_state['active_tab'])
except:
    current_idx = 0
    st.session_state['active_tab'] = MENU_OPTIONS[0]

# [중요] 데이터프레임 선택 초기화용 키
if 'df_key_tracker' not in st.session_state:
    st.session_state['df_key_tracker'] = 0

# 네비게이션 (라디오 버튼)
selected_tab_val = st.radio(
    "메뉴 이동", 
    MENU_OPTIONS, 
    index=current_idx,
    horizontal=True, 
    label_visibility="collapsed",
    key="nav_radio", 
    on_change=on_tab_change
)

# 실제 뷰는 논리적 상태(active_tab)를 따름
selected_tab = st.session_state['active_tab']

# --- TAB 1: 수입진행상황 ---
if selected_tab == MENU_OPTIONS[0]:
    st.markdown("### 📅 수입 진행 현황판")
    
    # 피벗 스타일 테이블 CSS
    st.markdown("""
    <style>
        .progress-table { width: 100%; border-collapse: collapse; font-size: 13px; margin-bottom: 30px; }
        .progress-table th, .progress-table td { border: 1px solid #adb5bd; text-align: center; vertical-align: middle; }
        .progress-table th { background-color: #f1f3f5; font-weight: bold; padding: 8px; color: #343a40; }
        .progress-table td { padding: 4px; }
        .progress-table .prod-cell { font-weight: bold; background-color: #f8f9fa; text-align: center; }
        .progress-table .size-cell { background-color: #f8f9fa; }
        .progress-table .date-cell { font-weight: bold; font-size: 12px; border-bottom: 1px dotted #ced4da; }
        .progress-table .qty-cell { font-weight: normal; color: #495057; border-bottom: 1px dotted #ced4da; }
        .progress-table .sup-cell { font-weight: normal; font-size: 11px; color: #868e96; }

        /* 상태별 배경색 (스크린샷 참고) */
        .cell-pending { background-color: #fff3bf !important; } /* 노란색 (진행중) */
        .cell-arrived { background-color: #3bc9db !important; color: white; } /* 파란색 (입고완료) */
        .cell-default { background-color: #ffffff !important; }
    </style>
    """, unsafe_allow_html=True)
    
    df = get_schedule_data('import_schedules', 'ALL')

    if df.empty:
        st.info("등록된 수입 일정이 없습니다.")
    else:
        # CANCELED(취소) 건은 현황판에서 제외
        df = df[df['status'] != 'CANCELED']

        with st.expander("🔍 조회 필터 (업체 / 품목 / 도착일)"):
            df = render_search_filters(df, "dash")

        if df.empty:
            st.warning("조회 조건에 맞는 데이터가 없습니다.")
            st.stop()

        # 착지(destination) 빈 값 처리
        df['destination'] = df['destination'].fillna('기본 착지').replace('', '기본 착지')

        if IS_MOBILE:
            render_progress_cards(df)
            st.stop()

        html_content = ""
        
        # 1. 착지(Destination) 별로 먼저 그룹화
        dest_groups = df.groupby('destination', dropna=False)
        
        for dest, d_group in dest_groups:
            dest_str = str(dest).strip()
            
            # 2. 품명, 사이즈로 그룹화
            prod_groups = d_group.groupby(['product_name', 'size'], dropna=False)
            
            # 테이블 레이아웃을 안 깨지게 하기 위해 가장 스케줄이 많은 건수 계산 (최소 5칸 확보)
            max_cols = prod_groups.size().max()
            if pd.isna(max_cols) or max_cols < 5: 
                max_cols = 5
            
            html_content += f"<table class='progress-table'>"
            
            # 여러 착지가 있거나 명시된 경우 하위 그룹 헤더(착지명) 추가
            if dest_str != '기본 착지' or len(dest_groups) > 1:
                html_content += f"<thead><tr><th colspan='{max_cols + 2}' style='background-color:#e9ecef; text-align:center; font-size: 14px;'>착지 : {dest_str}</th></tr></thead>"
            
            html_content += f"""
              <thead>
                <tr>
                  <th style="width: 18%;">품명</th>
                  <th style="width: 12%;">사이즈</th>
                  <th colspan="{max_cols}">입고일(ETA) / 수량 / 수출자·단가</th>
                </tr>
              </thead>
              <tbody>
            """

            # 각 품목/사이즈 별로 행(Row) 생성
            for (p_name, size), p_group in prod_groups:
                # 날짜순 정렬
                p_group = p_group.sort_values(by='expected_date')

                p_name_str = p_name if pd.notna(p_name) and str(p_name).strip() != '' else '-'
                size_str = size if pd.notna(size) and str(size).strip() != '' else '-'

                date_cells = ""
                qty_cells = ""
                sup_cells = ""

                # 가로로 나열될 날짜/수량/수출자·단가 셀 생성
                for _, row in p_group.iterrows():
                    eta = pd.to_datetime(row['expected_date']).strftime('%y/%m/%d') if pd.notna(row['expected_date']) else '-'
                    qty = f"{int(row['quantity']):,}" if pd.notna(row['quantity']) else "0"
                    status = row['status']
                    supplier_str = str(row['supplier']).strip() if pd.notna(row.get('supplier')) and str(row.get('supplier')).strip() else '-'
                    price_val = row.get('unit_price')
                    price_str = f"${float(price_val):,.2f}" if pd.notna(price_val) and float(price_val) > 0 else '-'

                    # 상태에 따른 배경색 클래스 할당
                    bg_class = "cell-default"
                    if status == 'PENDING':
                        bg_class = "cell-pending"
                    elif status == 'ARRIVED':
                        bg_class = "cell-arrived"

                    date_cells += f"<td class='date-cell {bg_class}'>{eta}</td>"
                    qty_cells += f"<td class='qty-cell {bg_class}'>{qty}</td>"
                    sup_cells += f"<td class='sup-cell {bg_class}'>{supplier_str}<br>{price_str}</td>"

                # 빈 칸 채우기 (테이블 레이아웃 유지용)
                empty_cols = max_cols - len(p_group)
                for _ in range(empty_cols):
                    date_cells += "<td class='date-cell cell-default'></td>"
                    qty_cells += "<td class='qty-cell cell-default'></td>"
                    sup_cells += "<td class='sup-cell cell-default'></td>"

                # 3줄을 묶어서 하나의 품목 행으로 렌더링 (날짜 줄, 수량 줄, 수출자·단가 줄)
                tr1 = f"<tr><td rowspan='3' class='prod-cell'>{p_name_str}</td><td rowspan='3' class='size-cell'>{size_str}</td>{date_cells}</tr>"
                tr2 = f"<tr>{qty_cells}</tr>"
                tr3 = f"<tr>{sup_cells}</tr>"

                html_content += tr1 + tr2 + tr3
                
            html_content += "</tbody></table>"
            
        st.markdown(f"<div class='progress-wrap'>{html_content}</div>", unsafe_allow_html=True)

# --- TAB 2: 수입장부 (상세) ---
elif selected_tab == MENU_OPTIONS[1]:
    st.markdown("### 📒 수입장부 상세 내역")
    st.info("💡 " + ("'상세/수정'을 누르면" if IS_MOBILE else "행을 클릭하면") + " 해당 건의 수정(등록/관리) 페이지로 이동합니다.")
    
    df_ledger = get_schedule_data('import_schedules', 'ALL')

    if not df_ledger.empty:
        with st.expander("🔍 조회 필터 (업체 / 품목 / 도착일)", expanded=not IS_MOBILE):
            df_ledger = render_search_filters(df_ledger, "ledger")

        if df_ledger.empty:
            st.warning("조회 조건에 맞는 데이터가 없습니다.")
            st.stop()

        if 'tri_cnt' in df_ledger.columns:
            df_ledger.insert(0, '구분', df_ledger['tri_cnt'].apply(lambda x: '삼각' if x > 0 else ''))

        # [수정] 동적 키 사용 (선택 상태 초기화용)
        dynamic_key = f"ledger_df_{st.session_state['df_key_tracker']}"

        if IS_MOBILE:
            def _open_from_ledger(row):
                r = row.to_dict()
                st.session_state['edit_mode'] = 'edit'
                st.session_state['selected_data'] = r
                st.session_state['clearance_list'] = parse_json_field(r.get('clearance_info'))
                st.session_state['declaration_list'] = parse_json_field(r.get('declaration_info'))
                st.session_state['m_show_form'] = True
                st.session_state['active_tab'] = MENU_OPTIONS[4]
                st.rerun()
            render_ledger_cards(df_ledger, "ledger_m_shown", _open_from_ledger)
            st.stop()

        event = st.dataframe(
            df_ledger,
            use_container_width=True,
            height=600,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            key=dynamic_key
        )

        if len(event.selection.rows) > 0:
            selected_idx = event.selection.rows[0]
            selected_row = df_ledger.iloc[selected_idx].to_dict()
            
            st.session_state['edit_mode'] = 'edit'
            st.session_state['selected_data'] = selected_row
            st.session_state['clearance_list'] = parse_json_field(selected_row.get('clearance_info'))
            st.session_state['declaration_list'] = parse_json_field(selected_row.get('declaration_info'))

            # [핵심 수정] 상태 변수를 변경하고 리런하여 라디오 버튼 인덱스 재계산 유도
            st.session_state['active_tab'] = MENU_OPTIONS[4] # "📝 수입 등록/관리"
            st.session_state['df_key_tracker'] += 1
            st.rerun()
            
    else: st.info("데이터가 없습니다.")

# --- TAB 3: 수출 (Export) - Editable ---
elif selected_tab == MENU_OPTIONS[2]:
    st.markdown("### 📤 수출 장부 (직접 입력 가능)")
    st.info("💡 엑셀처럼 셀을 " + ("탭하여" if IS_MOBILE else "더블클릭하여") + " 내용을 수정하세요. '수출자(수입자)' 칸은 바이어 정보를 입력하면 됩니다." + (" (모바일은 핵심 항목만 표시)" if IS_MOBILE else ""))
    
    df_export = get_schedule_data('export_schedules', 'ALL')
    
    if st.button("➕ 빈 행 추가 (신규 수출 건)"):
        save_schedule({'status': 'PENDING'}, None, 'export_schedules')
        st.rerun()

    if not df_export.empty:
        ui_cols = [
            'id', 'ck_code', 'global_code', 'doojin_code', 'supplier', 'origin', 'product_name', 'size', 'packing',
            'quantity', 'unit', 'unit_price', 'unit2', 'open_amount', 'tt_check', 'bank', 'lc_no', 
            'invoice_no', 'bl_no', 'etd', 'expected_date', 'status', 'note'
        ]
        ui_cols = [c for c in ui_cols if c in df_export.columns]
        
        edited_df = st.data_editor(
            df_export,
            column_config={
                "id": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "supplier": st.column_config.TextColumn("바이어(수입자)"),
                "product_name": st.column_config.TextColumn("품명 (수정불가, ID로 관리)", disabled=True),
                "expected_date": st.column_config.DateColumn("ETA", format="YYYY-MM-DD"),
                "etd": st.column_config.DateColumn("ETD", format="YYYY-MM-DD"),
            },
            column_order=(['supplier', 'product_name', 'size', 'quantity', 'unit_price', 'expected_date', 'status'] if IS_MOBILE else None),
            hide_index=True,
            use_container_width=True,
            num_rows="fixed",
            key="export_editor"
        )
        
        if st.button("💾 변경사항 저장 (수출)"):
            diff_count = 0
            for index, row in edited_df.iterrows():
                orig_row = df_export[df_export['id'] == row['id']].iloc[0]
                changed = {}
                for col in ui_cols:
                    if col == 'product_name': continue 
                    if str(row[col]) != str(orig_row[col]):
                        changed[col] = row[col]
                
                if changed:
                    save_schedule(changed, row['id'], 'export_schedules')
                    diff_count += 1
            
            if diff_count > 0:
                st.success(f"{diff_count}건 저장 완료!")
                time.sleep(1)
                st.rerun()
            else: st.info("변경 사항이 없습니다.")
    else: st.warning("등록된 수출 건이 없습니다.")

# --- TAB 4: 삼각무역 (Triangular) - Tag Management ---
elif selected_tab == MENU_OPTIONS[3]:
    st.markdown("### 📐 삼각무역 (부가 정보 관리)")
    st.markdown("기존 수입 건에 **삼각무역 관련 부가 정보(Tag)**를 연결하여 관리합니다. (건당 여러 개의 부가 정보 등록 가능)")

    with st.expander("📋 전체 삼각무역 등록 현황 (조회)", expanded=False):
        all_tri_df = get_all_triangular_trades()
        if all_tri_df.empty:
            st.info("등록된 삼각무역 정보가 없습니다.")
        else:
            search_tri = st.text_input("🔍 검색 (CK, 품명, 수입자 등)", key="tri_overview_search")
            view_df = all_tri_df
            if search_tri:
                mask = view_df.apply(lambda x: x.astype(str).str.contains(search_tri, case=False).any(), axis=1)
                view_df = view_df[mask]
            st.dataframe(view_df, use_container_width=True, hide_index=True)

    col_sel, col_detail = st.columns([1, 2])

    with col_sel:
        st.markdown("#### 1. 대상 수입 건 선택")
        imp_df = get_schedule_data('import_schedules', 'ALL')
        if imp_df.empty:
            st.warning("등록된 수입 건이 없습니다.")
            selected_imp_id = None
        else:
            imp_df['label'] = imp_df.apply(lambda x: f"[{x['ck_code'] or 'NO-CK'}] {x['product_name']}", axis=1)
            selected_imp_id = st.selectbox(
                "수입 건 목록", imp_df['id'], format_func=lambda x: imp_df[imp_df['id']==x]['label'].values[0],
                key="tri_selected_imp_id", on_change=lambda: st.session_state.update({'tri_edit_id': 'NEW'})
            )

    with col_detail:
        if selected_imp_id:
            target_row = imp_df[imp_df['id'] == selected_imp_id].iloc[0].to_dict()

            st.markdown("#### 2. 선택된 수입 건 정보 (참고용)")
            c1, c2, c3 = st.columns(3)
            c1.info(f"**CK관리번호**: {target_row.get('ck_code') or '-'}")
            c2.info(f"**원산지**: {target_row.get('origin') or '-'}")
            c3.info(f"**품명**: {target_row.get('product_name')}")

            # 이 수입 건에 연결된 삼각무역 태그 목록 (여러 건 가능)
            tri_df = get_triangular_trades(selected_imp_id)

            if 'tri_edit_id' not in st.session_state: st.session_state['tri_edit_id'] = 'NEW'

            if not tri_df.empty:
                st.markdown("#### 3. 등록된 부가 정보 목록")
                for _, trow in tri_df.iterrows():
                    with st.container(border=True):
                        cc1, cc2, cc3 = st.columns([3, 1, 1])
                        cc1.markdown(f"**수입자:** {trow.get('importer') or '-'} | **Invoice:** {trow.get('invoice_no') or '-'} | **ETA:** {trow.get('eta') or '-'}")
                        if cc2.button("수정", key=f"tri_edit_{trow['id']}", use_container_width=True):
                            st.session_state['tri_edit_id'] = trow['id']
                            st.rerun()
                        if cc3.button("삭제", key=f"tri_del_{trow['id']}", use_container_width=True):
                            ok, msg = delete_triangular_trade(trow['id'])
                            if ok:
                                st.session_state['tri_edit_id'] = 'NEW'
                                st.rerun()
                            else: st.error(msg)
                if st.button("➕ 새 부가 정보 추가", use_container_width=True):
                    st.session_state['tri_edit_id'] = 'NEW'
                    st.rerun()

            edit_id = st.session_state['tri_edit_id']
            existing_data = None
            if edit_id != 'NEW' and not tri_df.empty:
                match = tri_df[tri_df['id'] == edit_id]
                if not match.empty: existing_data = match.iloc[0].to_dict()

            action_txt = "수정" if existing_data else "신규 등록"
            st.markdown(f"#### 4. 삼각무역 부가 정보 ({action_txt})")

            with st.form(f"add_tri_tag_form_{edit_id}"):
                st.caption(f"이 수입 건에 대한 부가 정보를 {action_txt}합니다.")
                
                # 값 초기화 로직
                val_importer = existing_data.get('importer', '') if existing_data else ''
                val_size = existing_data.get('size', '') if existing_data else ''
                val_packing = existing_data.get('packing', '') if existing_data else ''
                
                val_qty = float(existing_data.get('open_qty', 0)) if existing_data else 0.0
                val_unit = existing_data.get('unit', '') if existing_data else ''
                val_amt = float(existing_data.get('open_amount', 0)) if existing_data else 0.0
                
                val_inv = existing_data.get('invoice_no', '') if existing_data else ''
                val_eta = safe_date_parse(existing_data.get('eta')) if existing_data and existing_data.get('eta') else None
                if val_eta: val_eta = datetime.strptime(val_eta, '%Y-%m-%d')
                
                val_pay_dt = safe_date_parse(existing_data.get('payment_date')) if existing_data and existing_data.get('payment_date') else None
                if val_pay_dt: val_pay_dt = datetime.strptime(val_pay_dt, '%Y-%m-%d')
                
                val_pay_amt = float(existing_data.get('payment_amount', 0)) if existing_data else 0.0
                val_ex_rate = float(existing_data.get('exchange_rate', 0)) if existing_data else 0.0

                c1, c2, c3 = st.columns(3)
                in_ck = c1.text_input("CK관리번호 (자동)", value=target_row.get('ck_code') or '', disabled=True)
                in_og = c2.text_input("원산지 (자동)", value=target_row.get('origin') or '', disabled=True)
                in_pn = c3.text_input("품명 (자동)", value=target_row.get('product_name') or '', disabled=True)

                c1, c2, c3 = st.columns(3)
                in_importer = c1.text_input("수입자", value=val_importer, placeholder="Buyer 입력")
                in_size = c2.text_input("사이즈", value=val_size)
                in_packing = c3.text_input("Packing", value=val_packing)
                
                c1, c2, c3 = st.columns(3)
                in_qty = c1.number_input("오픈수량", value=val_qty)
                in_unit = c2.text_input("단위", value=val_unit)
                in_amt = c3.number_input("오픈금액", value=val_amt)
                
                c1, c2 = st.columns(2)
                in_inv = c1.text_input("Invoice No.", value=val_inv)
                in_eta = c2.date_input("ETA", value=val_eta)
                
                c1, c2, c3 = st.columns(3)
                in_pay_dt = c1.date_input("결제일", value=val_pay_dt)
                in_pay_amt = c2.number_input("결제금액", value=val_pay_amt)
                in_ex_rate = c3.number_input("환율", value=val_ex_rate)

                if st.form_submit_button(f"💾 정보 {action_txt} (Tag)"):
                    save_data = {
                        'import_id': selected_imp_id,
                        'ck_code': target_row.get('ck_code'),
                        'origin': target_row.get('origin'),
                        'product_name': target_row.get('product_name'),
                        'importer': in_importer,
                        'size': in_size, 'packing': in_packing,
                        'open_qty': in_qty, 'unit': in_unit, 'open_amount': in_amt,
                        'invoice_no': in_inv, 'eta': in_eta,
                        'payment_date': in_pay_dt, 'payment_amount': in_pay_amt, 'exchange_rate': in_ex_rate
                    }
                    
                    tid = existing_data['id'] if existing_data else None
                    ok, msg = save_triangular_trade(save_data, tid)
                    if ok:
                        st.success(msg)
                        st.session_state['tri_edit_id'] = 'NEW'
                        time.sleep(1)
                        st.rerun()
                    else: st.error(f"오류: {msg}")

# --- TAB 5: 등록 및 관리 (복원됨) ---
elif selected_tab == MENU_OPTIONS[4]:
    m_form = IS_MOBILE and st.session_state.get('m_show_form', False)
    if IS_MOBILE:
        col_list, col_form = st.container(), st.container()  # 모바일: 목록/폼을 번갈아 전체 폭으로 표시
    else:
        col_list, col_form = st.columns([1, 2])

    if not m_form:
        with col_list:
            sub_t1, sub_t2 = st.tabs(["목록 선택", "엑셀 일괄 등록"])
        
            with sub_t1:
                st.subheader("등록 건 목록")
                df_list = get_schedule_data('import_schedules', 'ALL')

                with st.expander("🔍 조회 필터 (업체 / 품목 / 도착일)"):
                    fc1, fc2 = st.columns(2)
                    supplier_opts = ['전체']
                    if not df_list.empty and 'supplier' in df_list.columns:
                        supplier_opts += sorted([str(x) for x in df_list['supplier'].dropna().unique() if str(x).strip()])
                    product_opts = ['전체']
                    if not df_list.empty and 'product_name' in df_list.columns:
                        product_opts += sorted([str(x) for x in df_list['product_name'].dropna().unique() if str(x).strip()])
                    sel_supplier = fc1.selectbox("업체", supplier_opts, key="list_supplier")
                    sel_product = fc2.selectbox("품목", product_opts, key="list_product")
                    dc1, dc2 = st.columns(2)
                    date_from = dc1.date_input("도착일(부터)", value=None, key="list_date_from")
                    date_to = dc2.date_input("도착일(까지)", value=None, key="list_date_to")

                if not df_list.empty:
                    if sel_supplier != '전체': df_list = df_list[df_list['supplier'].astype(str) == sel_supplier]
                    if sel_product != '전체': df_list = df_list[df_list['product_name'].astype(str) == sel_product]
                    if date_from or date_to:
                        dcol = pd.to_datetime(df_list['expected_date'], errors='coerce').dt.date
                        if date_from: df_list = df_list[dcol.notna() & (dcol >= date_from)]
                        if date_to: df_list = df_list[dcol.notna() & (dcol <= date_to)]

                search_txt = st.text_input("🔍 키워드 검색 (CK, 품명 등)", key="list_search")
                if not df_list.empty and search_txt:
                    mask = df_list.apply(lambda x: x.astype(str).str.contains(search_txt, case=False).any(), axis=1)
                    df_list = df_list[mask]
            
                if st.button("➕ 신규 등록 (빈 양식)", type="primary", use_container_width=True):
                    st.session_state['edit_mode'] = 'new'
                    st.session_state['selected_data'] = None
                    st.session_state['clearance_list'] = []
                    st.session_state['declaration_list'] = []
                    st.session_state['m_show_form'] = True
                    st.rerun()

                st.markdown("---")
                if not df_list.empty:
                    for idx, row in df_list.iterrows():
                        st_icon = "🟢" if row['status'] == 'ARRIVED' else ("🟠" if row['status'] == 'PENDING' else "🔴")
                        label = f"{st_icon} **[{row['ck_code'] or 'NO-CK'}]** {row['product_name']}"
                        sub = f"{row['supplier'] or '-'} | ETA: {row['expected_date']}"
                        with st.container(border=True):
                            st.markdown(label)
                            st.caption(sub)
                            if st.button("상세/수정", key=f"sel_{row['id']}", use_container_width=True):
                                st.session_state['edit_mode'] = 'edit'
                                st.session_state['selected_data'] = row.to_dict()
                                st.session_state['clearance_list'] = parse_json_field(row.get('clearance_info'))
                                st.session_state['declaration_list'] = parse_json_field(row.get('declaration_info'))
                                st.session_state['m_show_form'] = True
                                st.rerun()
                else: st.info("데이터가 없습니다.")
        
            with sub_t2:
                st.subheader("엑셀 파일 업로드 (수입)")
                up_file = st.file_uploader("파일 선택", type=['csv', 'xlsx'])
                if up_file:
                    if st.button("분석 및 등록 시작", use_container_width=True):
                        try:
                            if up_file.name.endswith('.csv'):
                                try: df_up = pd.read_csv(up_file)
                                except: up_file.seek(0); df_up = pd.read_csv(up_file, encoding='cp949')
                            else: df_up = pd.read_excel(up_file)
                            
                            valid_rows, err_list, notice_list = parse_import_full_excel(df_up)

                            if notice_list:
                                st.info(f"🆕 신규 품목코드 {len(notice_list)}건이 자동 생성되었습니다 (ERP 실제 코드로는 [품목 관리] 탭에서 나중에 정정해주세요).")
                                with st.expander("신규 코드 생성 내역 보기"):
                                    for n in notice_list: st.write(f"- {n}")

                            if err_list:
                                st.error(f"{len(err_list)}건의 에러가 있습니다.")
                                with st.expander("에러 상세 보기"):
                                    for e in err_list: st.write(f"- {e}")

                            if valid_rows:
                                st.success(f"{len(valid_rows)}건의 유효 데이터를 찾았습니다.")
                                prog = st.progress(0)
                                cnt = 0
                                fail_reasons = [] 
                            
                                for i, d in enumerate(valid_rows):
                                    ok, msg = save_schedule(d)
                                    if ok: cnt += 1
                                    else: fail_reasons.append(f"행 {i+1}: {msg}")
                                    prog.progress((i+1)/len(valid_rows))
                            
                                if cnt > 0: st.toast(f"{cnt}건 일괄 등록 완료!"); st.success(f"총 {cnt}건 등록 성공")
                                if fail_reasons:
                                    with st.expander("실패 상세 사유 보기"):
                                        for reason in fail_reasons: st.write(reason)
                                time.sleep(1)
                        except Exception as e: st.error(f"오류 발생: {e}")

    if not IS_MOBILE or m_form:
        # [우측] 상세 입력 폼 (복원)
        with col_form:
            edit_mode = st.session_state.get('edit_mode', 'new')
            data = st.session_state.get('selected_data', {})
        
            if 'clearance_list' not in st.session_state: st.session_state['clearance_list'] = []
            if 'declaration_list' not in st.session_state: st.session_state['declaration_list'] = []
        
            if IS_MOBILE and st.button("← 목록으로", key="m_back_to_list", use_container_width=True):
                st.session_state['m_show_form'] = False
                st.rerun()

            title_prefix = "수정" if edit_mode == 'edit' else "신규 등록"
            st.subheader(f"📝 상세 정보 {title_prefix}")
        
            if edit_mode == 'edit' and not data:
                st.info("좌측 목록에서 항목을 선택해주세요.")
            else:
                rec_key = data.get('id', 'new') if data else 'new'

                # [품목 식별] 폼 바깥에 둬서 품명/원산지/사이즈를 바꿀 때마다 바로 매칭 결과가 갱신되게 함
                # (st.form 안에서는 제출 전까지 화면이 갱신되지 않아 실시간 매칭이 불가능함)
                st.markdown("<div class='form-header'>품목 식별 (품명+원산지+사이즈로 코드 자동 매칭)</div>", unsafe_allow_html=True)
                p_df = get_products_df()
                existing_names = sorted(p_df['품목명'].dropna().astype(str).unique().tolist()) if not p_df.empty else []
                NEW_NAME_SENTINEL = "🆕 새 품명 직접 입력"
                default_name = str(data.get('product_name') or '') if data else ''

                idc1, idc2, idc3 = st.columns(3)
                name_opts = [NEW_NAME_SENTINEL] + existing_names
                name_idx = name_opts.index(default_name) if default_name in existing_names else 0
                sel_name_choice = idc1.selectbox("품명", name_opts, index=name_idx, key=f"id_name_sel_{rec_key}")
                if sel_name_choice == NEW_NAME_SENTINEL:
                    id_name = idc1.text_input("새 품명 입력", value=default_name if default_name not in existing_names else '', key=f"id_name_new_{rec_key}")
                else:
                    id_name = sel_name_choice
                id_origin = idc2.text_input("원산지", value=data.get('origin', '') if data else '', key=f"id_origin_{rec_key}")
                id_size = idc3.text_input("사이즈", value=data.get('size', '') if data else '', key=f"id_size_{rec_key}")

                match_row = resolve_product_match(p_df, id_name, id_origin, id_size)
                pending_new_product = False
                if match_row is not None:
                    sel_pid = match_row['ID']
                    st.success(f"✅ 매칭된 품목코드: **{match_row['품목코드']}** ([{match_row['카테고리']}] {match_row['품목명']})")
                elif str(id_name).strip():
                    sel_pid = None
                    pending_new_product = True
                    variants = get_product_variants(p_df, id_name)
                    if not variants.empty:
                        with st.expander(f"ℹ️ '{id_name}'의 기존 원산지/사이즈 조합 {len(variants)}건 (참고)"):
                            st.dataframe(variants[['품목코드', '원산지', '사이즈']], hide_index=True, use_container_width=True)
                    st.warning("⚠️ 신규 품목 조합입니다 (일치하는 기존 코드가 없습니다). 저장 시 임시 코드가 자동 생성됩니다 (예: NEW-0001). 실제 ERP 코드로는 나중에 [품목 관리] 탭에서 정정해주세요.")
                else:
                    sel_pid = None
                    st.info("품명을 입력/선택하면 품목코드가 자동으로 매칭됩니다.")

                with st.form(f"detail_form_{rec_key}"):
                    ft1, ft2, ft3, ft4 = st.tabs(["기본/계약", "물류/일정", "결제/L/C", "통관/기타"])

                    with ft1:
                        st.markdown("<div class='form-header'>기본 식별 정보</div>", unsafe_allow_html=True)
                        c1, c2, c3 = st.columns(3)
                        ck_code = c1.text_input("CK 관리번호", value=data.get('ck_code', ''))
                        global_code = c2.text_input("글로벌 번호", value=data.get('global_code', ''))
                        doojin_code = c3.text_input("두진 번호", value=data.get('doojin_code', ''))

                        st.markdown("<div class='form-header'>계약 및 물품 정보</div>", unsafe_allow_html=True)
                        c1, c2, c3 = st.columns(3)
                        supplier = c1.text_input("수출자(수입자)", value=data.get('supplier', ''))
                        agency = c2.text_input("대행사", value=data.get('agency', ''))
                        agency_contract = c3.text_input("대행 계약서", value=data.get('agency_contract', ''))

                        packing = st.text_input("Packing", value=data.get('packing', ''))
                    
                        c1, c2, c3 = st.columns(3)
                        unit_price = c1.number_input("단가 (USD)", value=float(data.get('unit_price') or 0.0), step=0.01, format="%.2f")
                        unit2 = c2.text_input("단가 단위", value=data.get('unit2', 'kg'))
                        quantity = c3.number_input("오픈 수량", value=float(data.get('quantity') or 0.0))

                        c1, c2, c3 = st.columns(3)
                        doc_qty = c1.number_input("서류 수량", value=float(data.get('doc_qty') or 0.0))
                        box_qty = c2.number_input("박스 수량", value=float(data.get('box_qty') or 0.0))
                        open_amount = c3.number_input("오픈 금액", value=float(data.get('open_amount') or 0.0))

                        c1, c2 = st.columns(2)
                        doc_amount = c1.number_input("서류 금액", value=float(data.get('doc_amount') or 0.0))

                    with ft2:
                        st.markdown("<div class='form-header'>일정 및 물류 정보</div>", unsafe_allow_html=True)
                        c1, c2 = st.columns(2)
                        etd = c1.date_input("ETD (출항)", value=safe_date_parse(data.get('etd')))
                        eta = c2.date_input("ETA (입항/예정일)", value=safe_date_parse(data.get('expected_date')) or get_kst_today())

                        c1, c2, c3 = st.columns(3)
                        arrival_date = c1.date_input("실 입고일", value=safe_date_parse(data.get('arrival_date')))
                        actual_in_qty = c2.number_input("실 입고 수량", value=float(data.get('actual_in_qty') or 0.0))
                        remaining_qty = c3.number_input("잔량", value=float(data.get('remaining_qty') or 0.0))

                        c1, c2, c3 = st.columns(3)
                        warehouse = c1.text_input("창고", value=data.get('warehouse', ''))
                        destination = c2.text_input("착지", value=data.get('destination', ''))
                        arrival_exchange_rate = c3.number_input("도착일 환율", value=float(data.get('arrival_exchange_rate') or 0.0))

                        transport_status = st.text_input("운송현황", value=data.get('transport_status', '') or '')

                        st.markdown("<div class='form-header'>B/L 정보</div>", unsafe_allow_html=True)
                        c1, c2, c3 = st.columns(3)
                        invoice_no = c1.text_input("Invoice No.", value=data.get('invoice_no', ''))
                        bl_no = c2.text_input("B/L No.", value=data.get('bl_no', ''))
                        customs_broker_date = c3.date_input("관세사 전달일", value=safe_date_parse(data.get('customs_broker_date')))

                    with ft3:
                        st.markdown("<div class='form-header'>L/C 정보</div>", unsafe_allow_html=True)
                        c1, c2, c3 = st.columns(3)
                        tt_check = c1.text_input("T/T 여부", value=data.get('tt_check', ''))
                        bank = c2.text_input("개설 은행", value=data.get('bank', ''))
                        open_date = c3.date_input("개설일", value=safe_date_parse(data.get('open_date')))

                        c1, c2, c3 = st.columns(3)
                        lc_no = c1.text_input("L/C No.", value=data.get('lc_no', ''))
                        lg_no = c2.text_input("L/G", value=data.get('lg_no', ''))
                        insurance = c3.text_input("보험", value=data.get('insurance', ''))

                        c1, c2 = st.columns(2)
                        usance = c1.text_input("Usance", value=data.get('usance', ''))
                        at_sight = c2.text_input("At Sight", value=data.get('at_sight', ''))

                        st.markdown("<div class='form-header'>결제 및 인수</div>", unsafe_allow_html=True)
                        c1, c2, c3 = st.columns(3)
                        doc_acceptance = c1.date_input("서류 인수일", value=safe_date_parse(data.get('doc_acceptance')))
                        maturity_date = c2.date_input("만기일", value=safe_date_parse(data.get('maturity_date')))
                        ext_maturity_date = c3.date_input("연장 만기일", value=safe_date_parse(data.get('ext_maturity_date')))

                        c1, c2, c3 = st.columns(3)
                        acceptance_rate = c1.number_input("인수수수료율", value=float(data.get('acceptance_rate') or 0.0))
                        acceptance_fee = c2.number_input("인수수수료", value=float(data.get('acceptance_fee') or 0.0))
                        discount_fee = c3.number_input("인수할인료", value=float(data.get('discount_fee') or 0.0))

                        c1, c2, c3 = st.columns(3)
                        payment_date = c1.date_input("결제일", value=safe_date_parse(data.get('payment_date')))
                        payment_amount = c2.number_input("결제 금액", value=float(data.get('payment_amount') or 0.0))
                        exchange_rate = c3.number_input("환율", value=float(data.get('exchange_rate') or 0.0))

                        c1, c2 = st.columns(2)
                        balance = c1.number_input("잔액", value=float(data.get('balance') or 0.0))
                        avg_exchange_rate = c2.number_input("평균환율", value=float(data.get('avg_exchange_rate') or 0.0))

                    with ft4:
                        st.markdown("<div class='form-header'>통관 정보 (최대 6건)</div>", unsafe_allow_html=True)
                        clr_data = st.session_state['clearance_list']
                        new_clr_list = []

                        for i in range(6):
                            def_date = None; def_qty = 0.0; def_rate = 0.0
                            if i < len(clr_data):
                                try:
                                    if clr_data[i].get('date'): def_date = datetime.strptime(clr_data[i]['date'], '%Y-%m-%d').date()
                                    def_qty = float(clr_data[i].get('qty', 0))
                                    def_rate = float(clr_data[i].get('rate', 0))
                                except: pass
                        
                            cc1, cc2, cc3 = st.columns(3)
                            cd = cc1.date_input(f"통관일자 #{i+1}", value=def_date, key=f"clr_d_{rec_key}_{i}")
                            cq = cc2.number_input(f"수량 #{i+1}", value=def_qty, key=f"clr_q_{rec_key}_{i}")
                            cr = cc3.number_input(f"환율 #{i+1}", value=def_rate, key=f"clr_r_{rec_key}_{i}")
                            if cd or cq > 0: new_clr_list.append({"date": str(cd) if cd else None, "qty": cq, "rate": cr})

                        st.markdown("<div class='form-header'>수입신고 정보 (최대 5건)</div>", unsafe_allow_html=True)
                        decl_data = st.session_state['declaration_list']
                        new_decl_list = []
                    
                        for i in range(5):
                            d_def_date = None; d_def_no = ""
                            if i < len(decl_data):
                                try:
                                    if decl_data[i].get('date'): d_def_date = datetime.strptime(decl_data[i]['date'], '%Y-%m-%d').date()
                                    d_def_no = decl_data[i].get('no', "")
                                except: pass
                            
                            dc1, dc2 = st.columns(2)
                            dd = dc1.date_input(f"신고일 #{i+1}", value=d_def_date, key=f"decl_d_{rec_key}_{i}")
                            dn = dc2.text_input(f"신고번호 #{i+1}", value=d_def_no, key=f"decl_n_{rec_key}_{i}")
                            if dd or dn: new_decl_list.append({"date": str(dd) if dd else None, "no": dn})

                        st.markdown("---")
                        note = st.text_area("비고 / 메모", value=data.get('note', ''), height=100)
                    
                        st.markdown("##### 🏁 진행 상태 설정")
                        curr_status = data.get('status', 'PENDING')
                        status = st.radio("상태", ["PENDING", "ARRIVED", "CANCELED"], index=["PENDING", "ARRIVED", "CANCELED"].index(curr_status), horizontal=True)
                        st.caption("💡 PENDING 상태인 건은 품목/사이즈가 일치하면 재고장 화면에 '입고예정'으로 자동 표시됩니다.")

                        if status == 'ARRIVED' and curr_status != 'ARRIVED':
                             st.warning("⚠️ 'ARRIVED'로 저장 시 자동으로 재고 테이블에 등록됩니다.")

                    st.markdown("---")
                    c_submit, c_del = st.columns([4, 1])
                    with c_submit:
                        if st.form_submit_button("💾 정보 저장", type="primary", use_container_width=True):
                            final_pid = sel_pid
                            if not str(id_name).strip():
                                st.error("품목(품명)을 먼저 선택/입력해주세요.")
                                st.stop()
                            if pending_new_product:
                                variants = get_product_variants(p_df, id_name)
                                inherit_cat = str(variants.iloc[0]['카테고리']) if not variants.empty and pd.notna(variants.iloc[0]['카테고리']) else None
                                inherit_unit = str(variants.iloc[0]['단위']) if not variants.empty and pd.notna(variants.iloc[0]['단위']) else None
                                ok_np, new_code, err_np = register_product_variant(id_name, id_origin, id_size, inherit_cat, inherit_unit)
                                if not ok_np:
                                    st.error(f"신규 품목 생성 실패: {err_np}")
                                    st.stop()
                                new_p_df = get_products_df()
                                new_row = new_p_df[new_p_df['품목코드'] == new_code]
                                final_pid = new_row.iloc[0]['ID'] if not new_row.empty else None
                                st.toast(f"🆕 신규 품목코드 {new_code} 생성됨", icon="🆕")
                            save_data = {
                                'ck_code': ck_code, 'global_code': global_code, 'doojin_code': doojin_code,
                                'product_id': final_pid, 'agency': agency, 'agency_contract': agency_contract,
                                'supplier': supplier, 'origin': id_origin, 'size': id_size, 'packing': packing,
                                'unit_price': unit_price, 'unit2': unit2,
                                'quantity': quantity, 'open_qty': quantity, 'doc_qty': doc_qty, 'box_qty': box_qty,
                                'open_amount': open_amount, 'doc_amount': doc_amount,
                                'tt_check': tt_check, 'bank': bank, 'usance': usance, 'at_sight': at_sight,
                                'lc_no': lc_no, 'open_date': open_date,
                                'invoice_no': invoice_no, 'bl_no': bl_no, 'lg_no': lg_no, 'insurance': insurance,
                                'etd': etd, 'expected_date': eta, 'arrival_date': arrival_date, 'customs_broker_date': customs_broker_date,
                                'warehouse': warehouse, 'destination': destination, 'actual_in_qty': actual_in_qty,
                                'remaining_qty': remaining_qty, 'transport_status': transport_status,
                                'arrival_exchange_rate': arrival_exchange_rate,
                                'doc_acceptance': doc_acceptance, 'maturity_date': maturity_date, 'ext_maturity_date': ext_maturity_date,
                                'acceptance_rate': acceptance_rate, 'acceptance_fee': acceptance_fee, 'discount_fee': discount_fee,
                                'payment_date': payment_date, 'payment_amount': payment_amount, 'exchange_rate': exchange_rate,
                                'balance': balance, 'avg_exchange_rate': avg_exchange_rate,
                                'note': note, 'status': status,
                                'clearance_info': new_clr_list, 'declaration_info': new_decl_list
                            }
                            sid = data.get('id') if edit_mode == 'edit' else None
                            succ, msg = save_schedule(save_data, sid)
                            if succ:
                                st.success(msg)
                                st.session_state['m_show_form'] = False
                                time.sleep(1)
                                st.rerun()
                            else: st.error(f"저장 실패: {msg}")
                
                    with c_del:
                        if edit_mode == 'edit':
                            if st.form_submit_button("🗑️ 삭제"):
                                delete_schedule(data['id'])
                                st.session_state['edit_mode'] = 'new'
                                st.session_state['selected_data'] = None
                                st.session_state['m_show_form'] = False
                                st.rerun()

# --- TAB 6: 품목 관리 ---
elif selected_tab == MENU_OPTIONS[5]:
    st.markdown("### 📦 시스템 품목 관리")
    st.caption("품목코드는 '품명' 단위가 아니라 **품명+원산지+사이즈** 조합 단위로 부여됩니다 (같은 품명도 원산지/사이즈가 다르면 코드가 다릅니다).")
    col_p1, col_p2 = st.columns([1, 2])
    with col_p1:
        st.markdown("#### 신규 품목 등록")
        with st.form("new_prod_form"):
            new_code = st.text_input("품목코드 (고유값)", placeholder="예: P1001")
            new_name = st.text_input("품목명")
            c1, c2 = st.columns(2)
            new_origin = c1.text_input("원산지")
            new_size = c2.text_input("사이즈")
            new_cat = st.text_input("카테고리", placeholder="예: 수입")
            new_unit = st.text_input("기본 단위", value="Box")

            if st.form_submit_button("품목 저장", type="primary"):
                if new_code and new_name:
                    succ, msg = register_new_product(new_code, new_name, new_cat, new_unit)
                    if succ:
                        # 원산지/사이즈는 register_new_product 시그니처에 없으므로 바로 이어서 채워준다
                        try:
                            with conn.session as s:
                                s.execute(text("UPDATE products SET origin=:o, size=:s WHERE product_code=:c"),
                                          {"o": new_origin or None, "s": new_size or None, "c": new_code})
                                s.commit()
                            get_products_df.clear()
                        except Exception:
                            rollback_session()
                        st.success(msg)
                        time.sleep(1)
                        st.rerun()
                    else: st.error(msg)
                else: st.warning("코드와 품목명은 필수입니다.")

        st.markdown("---")
        st.markdown("#### 품목코드 마스터 일괄 업로드")
        st.caption("품목코드/제품명/원산지/사이즈(/규격) 컬럼이 있는 엑셀을 올리면, 코드가 이미 있으면 정보를 갱신하고 없으면 신규 등록합니다.")
        code_file = st.file_uploader("품목코드 마스터 파일 (xlsx)", type=['xlsx'], key="code_master_upload")
        if code_file and st.button("일괄 반영", use_container_width=True):
            try:
                df_cm = pd.read_excel(code_file)
                col_code = find_col_loose(df_cm.columns, ['품목코드', '코드'])
                col_name = find_col_loose(df_cm.columns, ['제품명', '품명', '품목명'])
                col_origin = find_col_loose(df_cm.columns, ['원산지'])
                col_size = find_col_loose(df_cm.columns, ['사이즈'])
                if not col_code or not col_name:
                    st.error("'품목코드'와 '제품명(품명)' 컬럼을 찾을 수 없습니다.")
                else:
                    rows = []
                    for _, r in df_cm.iterrows():
                        rows.append({
                            'code': r.get(col_code), 'name': r.get(col_name),
                            'origin': r.get(col_origin) if col_origin else None,
                            'size': r.get(col_size) if col_size else None,
                        })
                    ok_cnt, fail = upsert_product_code_master(rows)
                    st.success(f"{ok_cnt}건 반영 완료")
                    if fail:
                        with st.expander(f"실패 {len(fail)}건"):
                            for f in fail: st.write(f"- {f}")
                    time.sleep(1)
                    st.rerun()
            except Exception as e:
                st.error(f"업로드 처리 오류: {e}")

    with col_p2:
        st.markdown("#### 등록된 품목 리스트")
        curr_prods = get_products_df()
        if not curr_prods.empty:
            prod_search = st.text_input("🔍 품목명/코드/원산지 검색", key="prod_mgmt_search")
            view_prods = curr_prods
            if prod_search:
                mask = view_prods.apply(lambda x: x.astype(str).str.contains(prod_search, case=False).any(), axis=1)
                view_prods = view_prods[mask]
            st.caption(f"총 {len(curr_prods)}개 품목 중 {len(view_prods)}개 표시")
            st.dataframe(view_prods, use_container_width=True, hide_index=True)
        else: st.info("등록된 품목이 없습니다.")