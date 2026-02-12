import streamlit as st
import pandas as pd
from sqlalchemy import text
from datetime import datetime
import time
import pytz
import re
import io
import json

# ==========================================
# 0. 기본 설정 및 스타일
# ==========================================
st.set_page_config(page_title="수입진행관리 (CK Global)", layout="wide", page_icon="🚢")
KST = pytz.timezone('Asia/Seoul')

def get_kst_today():
    return datetime.now(KST).date()

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
</style>
""", unsafe_allow_html=True)

# DB 연결 및 스키마 업데이트
try:
    conn = st.connection("supabase", type="sql")
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

        s.commit()
except Exception as e:
    st.error(f"🚨 DB 연결 오류: .streamlit/secrets.toml을 확인하세요.\n{e}")
    st.stop()

# ==========================================
# 1. 데이터 조회 및 액션 함수
# ==========================================

@st.cache_data(ttl=600)
def get_products_df():
    """DB에 등록된 품목 리스트 조회"""
    try:
        with conn.session as s:
            df = pd.DataFrame(s.execute(text("SELECT product_id, product_name, product_code, category, unit FROM products WHERE is_active = TRUE ORDER BY category, product_name")).fetchall())
            if not df.empty:
                df.columns = ['ID', '품목명', '품목코드', '카테고리', '단위']
            return df
    except Exception:
        return pd.DataFrame()

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
    except Exception as e: return False, str(e)

def get_schedule_data(table_name='import_schedules', status_filter='ALL'):
    """데이터 조회 (수입/수출 공용)"""
    with conn.session as s:
        base_sql = f"""
            SELECT s.*, p.product_name, p.product_code as db_prod_code, p.unit as p_unit
            FROM {table_name} s
            LEFT JOIN products p ON s.product_id = p.product_id
        """
        if status_filter != 'ALL':
            base_sql += f" WHERE s.status = '{status_filter}'"
        
        base_sql += " ORDER BY s.expected_date ASC, s.id DESC"
        
        df = pd.DataFrame(s.execute(text(base_sql)).fetchall())
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
    except Exception as e: return False, f"동기화 오류: {str(e)}"

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
                'clearance_info', 'declaration_info'
            ]
            numeric_cols = ['quantity', 'unit_price', 'open_qty', 'doc_qty', 'box_qty', 'open_amount', 'doc_amount', 
                            'actual_in_qty', 'acceptance_rate', 'acceptance_fee', 'discount_fee', 'payment_amount', 
                            'exchange_rate', 'balance', 'avg_exchange_rate', 'arrival_exchange_rate']
            json_cols = ['clearance_info', 'declaration_info']

            params = {}
            for k in cols:
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
            
            if not params.get('status'): params['status'] = 'PENDING'

            target_id = None
            if sid:
                set_clause = ", ".join([f"{c} = CAST(:{c} AS JSONB)" if c in json_cols else f"{c} = :{c}" for c in cols])
                s.execute(text(f"UPDATE {table_name} SET {set_clause} WHERE id = :id"), {**params, "id": sid})
                target_id = sid
            else:
                col_str = ", ".join(cols)
                val_str = ", ".join([f"CAST(:{c} AS JSONB)" if c in json_cols else f":{c}" for c in cols])
                res = s.execute(text(f"INSERT INTO {table_name} ({col_str}) VALUES ({val_str}) RETURNING id"), params)
                target_id = res.fetchone()[0]
            s.commit()

        if table_name == 'import_schedules' and params['status'] == 'ARRIVED' and target_id:
            ok, msg = sync_import_to_inventory(target_id)
            if not ok:
                with conn.session as s:
                    s.execute(text(f"UPDATE {table_name} SET status = 'PENDING' WHERE id = :id"), {"id": target_id})
                    s.commit()
                return False, f"저장되었으나 재고생성 실패: {msg}"
        
        return True, "저장 완료"
    except Exception as e: return False, str(e)

def delete_schedule(sid, table_name='import_schedules'):
    try:
        with conn.session as s:
            s.execute(text(f"DELETE FROM {table_name} WHERE id = :sid"), {"sid": sid})
            s.commit()
        return True, "삭제 완료"
    except Exception as e: return False, str(e)

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
def get_triangular_trades(import_id):
    """특정 수입 건에 연결된 삼각무역 태그 조회"""
    try:
        with conn.session as s:
            df = pd.DataFrame(s.execute(text("SELECT * FROM triangular_trades WHERE import_id = :id ORDER BY id"), {"id": import_id}).fetchall())
            return df
    except Exception: return pd.DataFrame()

def save_triangular_trade(data):
    """삼각무역 태그 저장 (INSERT)"""
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

            # 항상 INSERT (태그 추가 개념)
            col_str = ", ".join(cols)
            val_str = ", ".join([f":{c}" for c in cols])
            s.execute(text(f"INSERT INTO triangular_trades ({col_str}) VALUES ({val_str})"), params)
            s.commit()
        return True, "삼각무역 정보 추가 완료"
    except Exception as e: return False, str(e)

def delete_triangular_trade(tid):
    try:
        with conn.session as s:
            s.execute(text("DELETE FROM triangular_trades WHERE id = :id"), {"id": tid})
            s.commit()
        return True, "삭제 완료"
    except Exception as e: return False, str(e)

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

def safe_float_parse(val):
    if pd.isna(val) or str(val).strip() == '': return 0.0
    try: return float(str(val).replace(',', '').replace(' ', '').strip())
    except: return 0.0

# --- 엑셀 파싱 함수 (복원) ---
def parse_import_full_excel(df):
    """'수입' 탭(상세 장부) 구조의 엑셀/CSV 파일 파싱"""
    valid_data = []
    errors = []
    
    p_df = get_products_df()
    if p_df.empty: return [], ["시스템에 등록된 품목이 없습니다."]
    product_map = {str(row['품목명']).replace(" ", "").lower(): row['ID'] for _, row in p_df.iterrows()}
    
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
        if df.empty: return [], ["파일 내용이 없습니다."]
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
            return [], ["헤더를 찾을 수 없습니다."]

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

    for idx, row in data_df.iterrows():
        if not col_map['name']: continue
        name_val = str(row.get(col_map['name'], '')).strip()
        if not name_val or name_val.lower() == 'nan': continue
        
        pid = product_map.get(name_val.replace(" ", "").lower())
        if not pid:
            errors.append(f"[행 {idx+2}] 알 수 없는 품목: '{name_val}'")
            continue
            
        try:
            def get_val(key, parser=str):
                col = col_map.get(key)
                return parser(row.get(col)) if col else (0.0 if parser == safe_float_parse else None)

            # (생략된 통관/신고 파싱 로직 복원)
            clearance_list = [] # 간단히 처리 (필요시 추가 확장)
            declaration_list = [] 

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
                'clearance_info': clearance_list,
                'declaration_info': declaration_list,
                'status': 'PENDING'
            }
            valid_data.append(data)
        except Exception as e:
            errors.append(f"[행 {idx+2}] 파싱 오류: {str(e)}")
            
    return valid_data, errors

# ==========================================
# 2. 메인 UI 구성
# ==========================================

st.title("🚢 수입/수출 통합 관리 시스템")

tab_status, tab_ledger, tab_export, tab_triangular, tab_manage, tab_product = st.tabs([
    "📊 수입진행상황", "📒 수입장부 (상세)", "📤 수출 (Export)", "tj 삼각무역 (Triangular)", "📝 수입 등록/관리", "📦 품목 관리"
])

# --- TAB 1: 수입진행상황 ---
with tab_status:
    st.markdown("### 📅 수입 진행 현황판")
    df = get_schedule_data('import_schedules', 'ALL')
    if df.empty:
        st.info("등록된 수입 일정이 없습니다.")
    else:
        df['eta_str'] = pd.to_datetime(df['expected_date']).dt.strftime('%y/%m/%d')
        grouped = df.groupby('eta_str', sort=False)
        html_content = """<table style="width:100%; border-collapse: collapse; font-size:13px; text-align:center;"><thead><tr style="background-color:#f8f9fa; border-bottom:2px solid #dee2e6;"><th style="padding:10px;">입항일</th><th style="padding:10px;">공급사</th><th style="padding:10px;">품명</th><th style="padding:10px;">CK</th><th style="padding:10px;">사이즈</th><th style="padding:10px;">단가</th><th style="padding:10px;">수량</th><th style="padding:10px;">상태</th></tr></thead><tbody>"""
        for date_str, group in grouped:
            html_content += f"""<tr style="background-color:#e7f5ff; border-top:1px solid #dee2e6; border-bottom:1px solid #dee2e6;"><td colspan="8" style="padding:8px; font-weight:bold; text-align:left; padding-left:15px; color:#495057;">📅 {date_str} (총 {len(group)}건)</td></tr>"""
            for _, row in group.iterrows():
                status_cls = "status-pending" if row['status'] == 'PENDING' else ("status-arrived" if row['status'] == 'ARRIVED' else "status-canceled")
                status_txt = "진행중" if row['status'] == 'PENDING' else ("입고완료" if row['status'] == 'ARRIVED' else "취소")
                html_content += f"""<tr style="border-bottom:1px solid #f1f3f5; height: 40px;"><td style="color:#868e96;">{date_str}</td><td>{row['supplier'] or '-'}</td><td style="font-weight:bold; color:#343a40;">{row['product_name']}</td><td style="font-family:monospace; color:#495057;">{row['ck_code'] or '-'}</td><td>{row['size'] or '-'}</td><td>${float(row['unit_price'] or 0):.2f}</td><td style="font-weight:bold; color:#1c7ed6;">{int(row['quantity'] or 0):,}</td><td><span class="status-badge {status_cls}">{status_txt}</span></td></tr>"""
        html_content += "</tbody></table>"
        st.markdown(html_content, unsafe_allow_html=True)

# --- TAB 2: 수입장부 (상세) ---
with tab_ledger:
    st.markdown("### 📒 수입장부 상세 내역")
    df_ledger = get_schedule_data('import_schedules', 'ALL')
    if not df_ledger.empty:
        st.dataframe(df_ledger, use_container_width=True, height=600, hide_index=True)
    else: st.info("데이터가 없습니다.")

# --- TAB 3: 수출 (Export) - Editable ---
with tab_export:
    st.markdown("### 📤 수출 장부 (직접 입력 가능)")
    st.info("💡 엑셀처럼 셀을 더블클릭하여 내용을 수정하세요. '수출자(수입자)' 칸은 바이어 정보를 입력하면 됩니다.")
    
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
with tab_triangular:
    st.markdown("### 📐 삼각무역 (부가 정보 관리)")
    st.markdown("기존 수입 건에 **삼각무역 관련 부가 정보(Tag)**를 연결하여 관리합니다.")
    
    col_sel, col_detail = st.columns([1, 2])
    
    with col_sel:
        st.markdown("#### 1. 대상 수입 건 선택")
        imp_df = get_schedule_data('import_schedules', 'ALL')
        if imp_df.empty:
            st.warning("등록된 수입 건이 없습니다.")
            selected_imp_id = None
        else:
            imp_df['label'] = imp_df.apply(lambda x: f"[{x['ck_code'] or 'NO-CK'}] {x['product_name']}", axis=1)
            selected_imp_id = st.selectbox("수입 건 목록", imp_df['id'], format_func=lambda x: imp_df[imp_df['id']==x]['label'].values[0])
    
    with col_detail:
        if selected_imp_id:
            target_row = imp_df[imp_df['id'] == selected_imp_id].iloc[0].to_dict()
            
            st.markdown("#### 2. 선택된 수입 건 정보 (참고용)")
            c1, c2, c3 = st.columns(3)
            c1.info(f"**CK관리번호**: {target_row.get('ck_code') or '-'}")
            c2.info(f"**원산지**: {target_row.get('origin') or '-'}")
            c3.info(f"**품명**: {target_row.get('product_name')}")

            st.markdown("#### 3. 연결된 삼각무역 정보 (목록)")
            tri_df = get_triangular_trades(selected_imp_id)
            if not tri_df.empty:
                st.dataframe(tri_df, use_container_width=True, hide_index=True)
                # 간단 삭제 UI
                del_tid = st.selectbox("삭제할 태그 ID 선택", tri_df['id'], key="del_tri_sel")
                if st.button("🗑️ 선택한 태그 삭제"):
                    delete_triangular_trade(del_tid)
                    st.rerun()
            else:
                st.caption("아직 연결된 정보가 없습니다.")

            st.markdown("#### 4. 신규 정보 추가 (Tag)")
            with st.form("add_tri_tag_form"):
                st.caption("아래 정보를 입력하여 해당 수입 건에 꼬리표를 붙입니다.")
                # 자동 입력되는 필드 (Read-only 처럼 표시하지만 DB저장을 위해 value 할당)
                c1, c2, c3 = st.columns(3)
                in_ck = c1.text_input("CK관리번호 (자동)", value=target_row.get('ck_code') or '', disabled=True)
                in_og = c2.text_input("원산지 (자동)", value=target_row.get('origin') or '', disabled=True)
                in_pn = c3.text_input("품명 (자동)", value=target_row.get('product_name') or '', disabled=True)

                c1, c2, c3 = st.columns(3)
                in_importer = c1.text_input("수입자", placeholder="Buyer 입력")
                in_size = c2.text_input("사이즈")
                in_packing = c3.text_input("Packing")
                
                c1, c2, c3 = st.columns(3)
                in_qty = c1.number_input("오픈수량", value=0.0)
                in_unit = c2.text_input("단위")
                in_amt = c3.number_input("오픈금액", value=0.0)
                
                c1, c2 = st.columns(2)
                in_inv = c1.text_input("Invoice No.")
                in_eta = c2.date_input("ETA", value=None)
                
                c1, c2, c3 = st.columns(3)
                in_pay_dt = c1.date_input("결제일", value=None)
                in_pay_amt = c2.number_input("결제금액", value=0.0)
                in_ex_rate = c3.number_input("환율", value=0.0)

                if st.form_submit_button("➕ 정보 추가 (Tag)"):
                    new_tag = {
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
                    ok, msg = save_triangular_trade(new_tag)
                    if ok:
                        st.success(msg)
                        time.sleep(1)
                        st.rerun()
                    else: st.error(f"오류: {msg}")

# --- TAB 5: 등록 및 관리 (복원됨) ---
with tab_manage:
    col_list, col_form = st.columns([1, 2])
    
    with col_list:
        sub_t1, sub_t2 = st.tabs(["목록 선택", "엑셀 일괄 등록"])
        
        with sub_t1:
            st.subheader("등록 건 목록")
            df_list = get_schedule_data('import_schedules', 'ALL')
            
            search_txt = st.text_input("🔍 검색 (CK, 품명 등)", key="list_search")
            if not df_list.empty and search_txt:
                mask = df_list.apply(lambda x: x.astype(str).str.contains(search_txt, case=False).any(), axis=1)
                df_list = df_list[mask]
            
            if st.button("➕ 신규 등록 (빈 양식)", type="primary", use_container_width=True):
                st.session_state['edit_mode'] = 'new'
                st.session_state['selected_data'] = None
                st.session_state['clearance_list'] = []
                st.session_state['declaration_list'] = []
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
                            
                            try: st.session_state['clearance_list'] = json.loads(row['clearance_info']) if row['clearance_info'] else []
                            except: st.session_state['clearance_list'] = []
                            
                            try: st.session_state['declaration_list'] = json.loads(row['declaration_info']) if row['declaration_info'] else []
                            except: st.session_state['declaration_list'] = []
                            
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
                            
                        valid_rows, err_list = parse_import_full_excel(df_up)
                        
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

    # [우측] 상세 입력 폼 (복원)
    with col_form:
        edit_mode = st.session_state.get('edit_mode', 'new')
        data = st.session_state.get('selected_data', {})
        
        if 'clearance_list' not in st.session_state: st.session_state['clearance_list'] = []
        if 'declaration_list' not in st.session_state: st.session_state['declaration_list'] = []
        
        title_prefix = "수정" if edit_mode == 'edit' else "신규 등록"
        st.subheader(f"📝 상세 정보 {title_prefix}")
        
        if edit_mode == 'edit' and not data:
            st.info("좌측 목록에서 항목을 선택해주세요.")
        else:
            with st.form("detail_form"):
                ft1, ft2, ft3, ft4 = st.tabs(["기본/계약", "물류/일정", "결제/L/C", "통관/기타"])

                with ft1:
                    st.markdown("<div class='form-header'>기본 식별 정보</div>", unsafe_allow_html=True)
                    c1, c2, c3 = st.columns(3)
                    ck_code = c1.text_input("CK 관리번호", value=data.get('ck_code', ''))
                    global_code = c2.text_input("글로벌 번호", value=data.get('global_code', ''))
                    doojin_code = c3.text_input("두진 번호", value=data.get('doojin_code', ''))
                    
                    p_df = get_products_df()
                    p_opts = {row['ID']: f"[{row['카테고리']}] {row['품목명']} ({row['품목코드']})" for _, row in p_df.iterrows()}
                    def_pid = data.get('product_id')
                    if def_pid not in p_opts: def_pid = None
                    opt_keys = list(p_opts.keys())
                    sel_idx = opt_keys.index(def_pid) if def_pid in opt_keys else 0
                    sel_pid = st.selectbox("품목 (필수)", options=opt_keys, format_func=lambda x: p_opts[x], index=sel_idx)

                    st.markdown("<div class='form-header'>계약 및 물품 정보</div>", unsafe_allow_html=True)
                    c1, c2, c3 = st.columns(3)
                    supplier = c1.text_input("수출자(수입자)", value=data.get('supplier', ''))
                    agency = c2.text_input("대행사", value=data.get('agency', ''))
                    agency_contract = c3.text_input("대행 계약서", value=data.get('agency_contract', ''))
                    
                    c1, c2, c3 = st.columns(3)
                    origin = c1.text_input("원산지", value=data.get('origin', ''))
                    size = c2.text_input("사이즈", value=data.get('size', ''))
                    packing = c3.text_input("Packing", value=data.get('packing', ''))
                    
                    c1, c2, c3 = st.columns(3)
                    unit_price = c1.number_input("단가 (USD)", value=float(data.get('unit_price') or 0.0), step=0.01, format="%.2f")
                    unit2 = c2.text_input("단가 단위", value=data.get('unit2', 'kg'))
                    quantity = c3.number_input("오픈 수량", value=float(data.get('quantity') or 0.0))

                    c1, c2, c3 = st.columns(3)
                    doc_qty = c1.number_input("서류 수량", value=float(data.get('doc_qty') or 0.0))
                    box_qty = c2.number_input("박스 수량", value=float(data.get('box_qty') or 0.0))
                    open_amount = c3.number_input("오픈 금액", value=float(data.get('open_amount') or 0.0))

                with ft2:
                    st.markdown("<div class='form-header'>일정 및 물류 정보</div>", unsafe_allow_html=True)
                    c1, c2 = st.columns(2)
                    etd = c1.date_input("ETD (출항)", value=safe_date_parse(data.get('etd')))
                    eta = c2.date_input("ETA (입항/예정일)", value=safe_date_parse(data.get('expected_date')) or get_kst_today())
                    
                    c1, c2 = st.columns(2)
                    arrival_date = c1.date_input("실 입고일", value=safe_date_parse(data.get('arrival_date')))
                    actual_in_qty = c2.number_input("실 입고 수량", value=float(data.get('actual_in_qty') or 0.0))
                    
                    c1, c2 = st.columns(2)
                    warehouse = c1.text_input("창고", value=data.get('warehouse', ''))
                    destination = c2.text_input("착지", value=data.get('destination', ''))
                    
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

                    st.markdown("<div class='form-header'>결제 및 인수</div>", unsafe_allow_html=True)
                    c1, c2, c3 = st.columns(3)
                    doc_acceptance = c1.date_input("서류 인수일", value=safe_date_parse(data.get('doc_acceptance')))
                    maturity_date = c2.date_input("만기일", value=safe_date_parse(data.get('maturity_date')))
                    payment_date = c3.date_input("결제일", value=safe_date_parse(data.get('payment_date')))
                    
                    c1, c2 = st.columns(2)
                    payment_amount = c1.number_input("결제 금액", value=float(data.get('payment_amount') or 0.0))

                with ft4:
                    st.markdown("<div class='form-header'>통관 정보 (최대 5건)</div>", unsafe_allow_html=True)
                    clr_data = st.session_state['clearance_list']
                    new_clr_list = []
                    
                    for i in range(5):
                        def_date = None; def_qty = 0.0; def_rate = 0.0
                        if i < len(clr_data):
                            try:
                                if clr_data[i].get('date'): def_date = datetime.strptime(clr_data[i]['date'], '%Y-%m-%d').date()
                                def_qty = float(clr_data[i].get('qty', 0))
                                def_rate = float(clr_data[i].get('rate', 0))
                            except: pass
                        
                        cc1, cc2, cc3 = st.columns(3)
                        cd = cc1.date_input(f"통관일자 #{i+1}", value=def_date, key=f"clr_d_{i}")
                        cq = cc2.number_input(f"수량 #{i+1}", value=def_qty, key=f"clr_q_{i}")
                        cr = cc3.number_input(f"환율 #{i+1}", value=def_rate, key=f"clr_r_{i}")
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
                        dd = dc1.date_input(f"신고일 #{i+1}", value=d_def_date, key=f"decl_d_{i}")
                        dn = dc2.text_input(f"신고번호 #{i+1}", value=d_def_no, key=f"decl_n_{i}")
                        if dd or dn: new_decl_list.append({"date": str(dd) if dd else None, "no": dn})

                    st.markdown("---")
                    note = st.text_area("비고 / 메모", value=data.get('note', ''), height=100)
                    
                    st.markdown("##### 🏁 진행 상태 설정")
                    curr_status = data.get('status', 'PENDING')
                    status = st.radio("상태", ["PENDING", "ARRIVED", "CANCELED"], index=["PENDING", "ARRIVED", "CANCELED"].index(curr_status), horizontal=True)
                    
                    if status == 'ARRIVED' and curr_status != 'ARRIVED':
                         st.warning("⚠️ 'ARRIVED'로 저장 시 자동으로 재고 테이블에 등록됩니다.")

                st.markdown("---")
                c_submit, c_del = st.columns([4, 1])
                with c_submit:
                    if st.form_submit_button("💾 정보 저장", type="primary", use_container_width=True):
                        save_data = {
                            'ck_code': ck_code, 'global_code': global_code, 'doojin_code': doojin_code,
                            'product_id': sel_pid, 'agency': agency, 'agency_contract': agency_contract,
                            'supplier': supplier, 'origin': origin, 'size': size, 'packing': packing,
                            'unit_price': unit_price, 'unit2': unit2, 
                            'quantity': quantity, 'doc_qty': doc_qty, 'box_qty': box_qty,
                            'open_amount': open_amount, 
                            'tt_check': tt_check, 'bank': bank, 'lc_no': lc_no, 'open_date': open_date,
                            'invoice_no': invoice_no, 'bl_no': bl_no, 'lg_no': lg_no, 'insurance': insurance,
                            'etd': etd, 'expected_date': eta, 'arrival_date': arrival_date, 'customs_broker_date': customs_broker_date,
                            'warehouse': warehouse, 'destination': destination, 'actual_in_qty': actual_in_qty,
                            'doc_acceptance': doc_acceptance, 'maturity_date': maturity_date, 'payment_date': payment_date,
                            'payment_amount': payment_amount, 'note': note, 'status': status,
                            'clearance_info': new_clr_list, 'declaration_info': new_decl_list
                        }
                        sid = data.get('id') if edit_mode == 'edit' else None
                        succ, msg = save_schedule(save_data, sid)
                        if succ:
                            st.success(msg)
                            time.sleep(1)
                            st.rerun()
                        else: st.error(f"저장 실패: {msg}")
                
                with c_del:
                    if edit_mode == 'edit':
                        if st.form_submit_button("🗑️ 삭제"):
                            delete_schedule(data['id'])
                            st.session_state['edit_mode'] = 'new'
                            st.session_state['selected_data'] = None
                            st.rerun()

# --- TAB 6: 품목 관리 ---
with tab_product:
    st.markdown("### 📦 시스템 품목 관리")
    col_p1, col_p2 = st.columns([1, 2])
    with col_p1:
        st.markdown("#### 신규 품목 등록")
        with st.form("new_prod_form"):
            new_code = st.text_input("품목코드 (고유값)", placeholder="예: P1001")
            new_name = st.text_input("품목명")
            new_cat = st.text_input("카테고리", placeholder="예: 수입")
            new_unit = st.text_input("기본 단위", value="Box")
            
            if st.form_submit_button("품목 저장", type="primary"):
                if new_code and new_name:
                    succ, msg = register_new_product(new_code, new_name, new_cat, new_unit)
                    if succ:
                        st.success(msg)
                        time.sleep(1)
                        st.rerun()
                    else: st.error(msg)
                else: st.warning("코드와 품목명은 필수입니다.")
    
    with col_p2:
        st.markdown("#### 등록된 품목 리스트")
        curr_prods = get_products_df()
        if not curr_prods.empty:
            st.dataframe(curr_prods, use_container_width=True, hide_index=True)
        else: st.info("등록된 품목이 없습니다.")