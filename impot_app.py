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
st.set_page_config(page_title="수입진행관리 (CK Global)", layout="wide")
KST = pytz.timezone('Asia/Seoul')

def get_kst_today():
    return datetime.now(KST).date()

st.markdown("""
<style>
    .block-container { max-width: 98% !important; padding-top: 1rem; }
    
    /* 상태 배지 스타일 */
    .status-badge { padding: 2px 6px; border-radius: 4px; font-weight: bold; font-size: 0.8em; }
    .status-pending { background-color: #fff3bf; color: #f08c00; }
    .status-arrived { background-color: #d3f9d8; color: #2b8a3e; }
    .status-canceled { background-color: #ffe3e3; color: #c92a2a; }

    /* 메트릭 박스 스타일 */
    .metric-box {
        background-color: #f8f9fa;
        border: 1px solid #dee2e6;
        padding: 10px;
        border-radius: 8px;
        text-align: center;
        font-size: 14px;
        box-shadow: 0 1px 2px rgba(0,0,0,0.05);
    }
    
    /* 폼 헤더 스타일 */
    .form-header {
        font-weight: 700;
        font-size: 1.1em;
        margin-top: 20px;
        margin-bottom: 10px;
        border-bottom: 2px solid #e9ecef;
        padding-bottom: 5px;
        color: #495057;
    }
    
    /* 데이터프레임 스타일 */
    .stDataFrame { font-size: 12px; }
</style>
""", unsafe_allow_html=True)

# DB 연결 및 스키마 업데이트
try:
    conn = st.connection("supabase", type="sql")
    with conn.session as s:
        # 모든 필요한 컬럼이 존재하는지 확인하고 없으면 추가
        cols_to_add = [
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
            # 통관 및 신고 정보 (JSONB로 저장하여 N개 데이터 지원)
            ("clearance_info", "JSONB"), ("declaration_info", "JSONB")
        ]
        for col_name, col_type in cols_to_add:
            s.execute(text(f"ALTER TABLE import_schedules ADD COLUMN IF NOT EXISTS {col_name} {col_type};"))
        s.commit()
except Exception as e:
    st.error(f"DB 연결 오류: .streamlit/secrets.toml을 확인하세요.\n{e}")
    st.stop()

# ==========================================
# 1. 데이터 조회 및 액션 함수
# ==========================================

@st.cache_data(ttl=600)
def get_products_df():
    """DB에 등록된 품목 리스트 조회"""
    with conn.session as s:
        df = pd.DataFrame(s.execute(text("SELECT product_id, product_name, product_code, category, unit FROM products WHERE is_active = TRUE ORDER BY category, product_name")).fetchall())
        if not df.empty:
            df.columns = ['ID', '품목명', '품목코드', '카테고리', '단위']
        return df

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
        get_products_df.clear() # 캐시 초기화
        return True, "품목 등록 완료"
    except Exception as e: return False, str(e)

def get_full_schedule_data(status_filter='ALL'):
    """모든 상세 정보를 포함한 데이터 조회"""
    with conn.session as s:
        # 모든 컬럼 선택 (LEFT JOIN으로 변경하여 제품 정보 없어도 조회 가능하도록)
        base_sql = """
            SELECT s.*, p.product_name, p.product_code as db_prod_code, p.unit as p_unit
            FROM import_schedules s
            LEFT JOIN products p ON s.product_id = p.product_id
        """
        if status_filter != 'ALL':
            base_sql += f" WHERE s.status = '{status_filter}'"
        
        base_sql += " ORDER BY s.expected_date ASC, s.id DESC"
        
        df = pd.DataFrame(s.execute(text(base_sql)).fetchall())
        return df

def save_full_schedule(data, sid=None):
    """상세 정보 저장 (INSERT or UPDATE)"""
    try:
        with conn.session as s:
            cols = [
                'product_id', 'expected_date', 'quantity', 'note', 'status', 'size', 'supplier', 'unit_price', 'ck_code',
                'global_code', 'doojin_code', 'agency', 'agency_contract', 'origin', 'packing', 
                'open_qty', 'doc_qty', 'box_qty', 'unit2', 'open_amount', 'doc_amount',
                'tt_check', 'bank', 'usance', 'at_sight', 'open_date', 'lc_no', 'invoice_no', 'bl_no', 'lg_no', 'insurance',
                'customs_broker_date', 'etd', 'arrival_date', 'warehouse', 'actual_in_qty', 'destination',
                'doc_acceptance', 'acceptance_rate', 'maturity_date', 'ext_maturity_date', 'acceptance_fee', 'discount_fee',
                'payment_date', 'payment_amount', 'exchange_rate', 'balance', 'avg_exchange_rate',
                'clearance_info', 'declaration_info'
            ]
            
            # 숫자형 컬럼 리스트 (0으로 처리할 것들)
            numeric_cols = ['quantity', 'unit_price', 'open_qty', 'doc_qty', 'box_qty', 'open_amount', 'doc_amount', 
                            'actual_in_qty', 'acceptance_rate', 'acceptance_fee', 'discount_fee', 'payment_amount', 
                            'exchange_rate', 'balance', 'avg_exchange_rate']
            
            # JSON 컬럼 리스트
            json_cols = ['clearance_info', 'declaration_info']

            params = {}
            for k in cols:
                val = data.get(k)
                if k in numeric_cols:
                    if val is None or str(val).strip() == '':
                        params[k] = 0
                    else:
                        try: params[k] = float(str(val).replace(',', '').strip())
                        except: params[k] = 0
                elif k in json_cols:
                    # JSON 데이터 처리
                    if isinstance(val, (list, dict)):
                        params[k] = json.dumps(val, ensure_ascii=False)
                    elif isinstance(val, str) and (val.startswith('[') or val.startswith('{')):
                        params[k] = val # 이미 JSON 문자열인 경우
                    else:
                        params[k] = '[]' # 기본값
                else:
                    if val is None or str(val).strip() == '' or str(val).lower() == 'nan':
                        params[k] = None
                    else:
                        params[k] = val
            
            # status 값 강제 설정 (값이 없으면 PENDING)
            if not params.get('status'):
                params['status'] = 'PENDING'

            if sid:
                # UPDATE
                set_clause = ", ".join([f"{col} = :{col}::jsonb" if col in json_cols else f"{col} = :{col}" for col in cols])
                sql = f"UPDATE import_schedules SET {set_clause} WHERE id = :id"
                params['id'] = sid
                s.execute(text(sql), params)
                msg = "수정 완료"
            else:
                # INSERT
                col_str = ", ".join(cols)
                val_str = ", ".join([f":{col}::jsonb" if col in json_cols else f":{col}" for col in cols])
                sql = f"INSERT INTO import_schedules ({col_str}) VALUES ({val_str})"
                s.execute(text(sql), params)
                msg = "등록 완료"
                
            s.commit()
        return True, msg
    except Exception as e: return False, str(e)

def delete_schedule(sid):
    try:
        with conn.session as s:
            s.execute(text("DELETE FROM import_schedules WHERE id = :sid"), {"sid": sid})
            s.commit()
        return True, "삭제 완료"
    except Exception as e: return False, str(e)

# --- 유틸리티 함수 ---
def safe_date_parse(val):
    if pd.isna(val) or str(val).strip() == '': return None
    try:
        if isinstance(val, datetime): return val.strftime('%Y-%m-%d')
        s_val = str(val).strip()
        # 다양한 날짜 형식 지원
        if re.match(r'^\d{2}/\d{2}/\d{2}$', s_val): # 25/01/01
            dt = datetime.strptime(s_val, "%y/%m/%d")
            if dt.year < 2000: dt = dt.replace(year=dt.year+2000)
            return dt.strftime('%Y-%m-%d')
        if re.match(r'^\d{4}-\d{2}-\d{2}$', s_val): return datetime.strptime(s_val, "%Y-%m-%d").strftime('%Y-%m-%d')
        if re.match(r'^\d{4}\.\d{2}\.\d{2}$', s_val): return datetime.strptime(s_val, "%Y.%m.%d").strftime('%Y-%m-%d')
        if re.match(r'^\d{8}$', s_val): return datetime.strptime(s_val, "%Y%m%d").strftime('%Y-%m-%d')
        return pd.to_datetime(val).strftime('%Y-%m-%d')
    except: return None

def safe_float_parse(val):
    if pd.isna(val) or str(val).strip() == '': return 0.0
    try: return float(str(val).replace(',', '').replace(' ', '').strip())
    except: return 0.0

def parse_import_full_excel(df):
    """
    '수입' 탭(상세 장부) 구조의 엑셀/CSV 파일 파싱
    헤더를 찾아 컬럼 매핑 후 데이터 추출
    """
    valid_data = []
    errors = []
    
    p_df = get_products_df()
    if p_df.empty: return [], ["시스템에 등록된 품목이 없습니다."]
    product_map = {str(row['품목명']).replace(" ", "").lower(): row['ID'] for _, row in p_df.iterrows()}
    
    # 1. 헤더 행 찾기 (스코어링 방식 강화)
    keywords = ['CK', '관리번호', '품명', '수량', '단가', '글로벌', '두진', '입고일', 'ETA']
    
    # 문자열 정제 헬퍼 (공백, 줄바꿈 제거)
    def clean_str(s):
        return str(s).replace('\n', '').replace('\r', '').replace(' ', '').upper().strip()

    # 현재 컬럼명이 헤더인지 먼저 확인
    col_str = "".join([clean_str(c) for c in df.columns])
    score_cols = 0
    for k in keywords:
        if k in col_str: score_cols += 1
    
    # 필수 키워드 (CK/관리번호 + 품명) 확인
    has_mandatory = ('CK' in col_str or '관리번호' in col_str) and '품명' in col_str

    data_df = pd.DataFrame()
    header_row_idx = -1

    if score_cols >= 2 and has_mandatory:
        data_df = df
    else:
        if df.empty: return [], ["파일 내용이 없습니다."]

        max_score = 0
        for i in range(min(20, len(df))):
            row_vals = [clean_str(x) for x in df.iloc[i].values if pd.notna(x)]
            row_str = "".join(row_vals)
            score = 0
            for k in keywords:
                if k in row_str: score += 1
            
            row_has_mandatory = ('CK' in row_str or '관리번호' in row_str) and '품명' in row_str
            
            if score > max_score and score >= 2 and row_has_mandatory:
                max_score = score
                header_row_idx = i
                
        if header_row_idx != -1:
            df.columns = df.iloc[header_row_idx]
            data_df = df.iloc[header_row_idx+1:].reset_index(drop=True)
        else:
            return [], ["헤더('CK', '품명' 등)를 찾을 수 없습니다. (상위 20행 검색 실패)"]

    # 컬럼 이름 정제 (모든 공백/줄바꿈 제거)
    data_df.columns = [clean_str(c) for c in data_df.columns]
    cols = list(data_df.columns)
    
    # 3. 컬럼 매핑 (공백 제거된 키워드 사용)
    def find_col(keywords):
        for c in cols:
            # c는 이미 clean_str 처리됨
            for k in keywords:
                k_clean = k.replace(" ", "").upper()
                if k_clean in c: return c
        return None

    # 키워드도 공백 없이 검색
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
        'broker_date': find_col(['관세사', '관세사발송일']), 'etd': find_col(['ETD']), 'eta': find_col(['ETA']),
        'arrival_date': find_col(['입고일']), 'wh': find_col(['창고']), 'real_in_qty': find_col(['실입고', '실입고수량']),
        'dest': find_col(['착지']), 'note': find_col(['비고']), 'doc_acc': find_col(['서류인수']),
        'acc_rate': find_col(['인수수수료율']), 'mat_date': find_col(['만기일']), 'ext_date': find_col(['연장만기일']),
        'acc_fee': find_col(['인수수수료']), 'dis_fee': find_col(['인수할인료']), 'pay_date': find_col(['결제일']),
        'pay_amt': find_col(['결제금액']), 'ex_rate': find_col(['환율']), 'balance': find_col(['잔액']), 'avg_ex': find_col(['평균환율'])
    }
    
    # 'agency' 재확인
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

    # 데이터 추출
    for idx, row in data_df.iterrows():
        if not col_map['name']: continue
        name_val = str(row.get(col_map['name'], '')).strip()
        if not name_val or name_val.lower() == 'nan': continue
        
        search_key = name_val.replace(" ", "").lower()
        pid = product_map.get(search_key)
        ck_val = str(row.get(col_map['ck'], '')).strip() if col_map['ck'] else ""
        if ck_val.lower() == 'nan': ck_val = ""
        
        if not pid:
            errors.append(f"[행 {idx+2}] 알 수 없는 품목: '{name_val}' (CK: {ck_val})")
            continue
            
        try:
            # 헬퍼 함수
            def get_val(key, parser=str):
                col = col_map.get(key)
                if col:
                    val = row.get(col)
                    return parser(val)
                return 0.0 if parser == safe_float_parse else (None if parser == safe_date_parse else '')

            # 통관 정보 추출
            clearance_list = []
            for i in range(1, 11):
                suffix = str(i) if i > 1 else ""
                c_date_col = find_col([f"통관일자{suffix}"])
                c_qty_col = find_col([f"통관수량{suffix}"])
                c_rate_col = find_col([f"통관환율{suffix}"])
                
                if c_date_col or c_qty_col:
                    d_val = safe_date_parse(row.get(c_date_col)) if c_date_col else None
                    q_val = safe_float_parse(row.get(c_qty_col)) if c_qty_col else 0.0
                    r_val = safe_float_parse(row.get(c_rate_col)) if c_rate_col else 0.0
                    if d_val or q_val > 0:
                        clearance_list.append({"date": d_val, "qty": q_val, "rate": r_val})

            # 수입신고 정보 추출
            declaration_list = []
            for i in range(1, 11):
                suffix = str(i) if i > 1 else ""
                d_date_col = find_col([f"신고일{suffix}"])
                d_no_col = find_col([f"신고번호{suffix}"])
                
                if d_date_col or d_no_col:
                    date_val = safe_date_parse(row.get(d_date_col)) if d_date_col else None
                    no_val = str(row.get(d_no_col, '')).strip() if d_no_col else ''
                    if date_val or no_val:
                        declaration_list.append({"date": date_val, "no": no_val})

            data = {
                'product_id': pid, 'ck_code': ck_val,
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
            data['unit'] = str(row.get(col_map.get('unit'), '')).strip() if col_map.get('unit') else ''

            for k, v in data.items():
                if isinstance(v, str) and (v.lower() == 'nan' or v.lower() == 'nat'): data[k] = ''
            valid_data.append(data)
        except Exception as e:
            errors.append(f"[행 {idx+2}] 데이터 파싱 오류: {str(e)}")
            
    return valid_data, errors

# ==========================================
# 2. 메인 UI 구성
# ==========================================

st.title("🚢 수입 관리 시스템")

tab_status, tab_ledger, tab_manage, tab_product = st.tabs(["📊 수입진행상황", "📒 수입장부 (상세)", "📝 등록 및 관리", "📦 품목 관리"])

# --- TAB 1: 수입진행상황 (HTML 뷰) ---
with tab_status:
    st.markdown("### 📅 수입 진행 현황판")
    
    df = get_full_schedule_data('ALL')
    
    if df.empty:
        st.info("등록된 수입 일정이 없습니다.")
    else:
        df['eta_str'] = pd.to_datetime(df['expected_date']).dt.strftime('%y/%m/%d')
        grouped = df.groupby('eta_str', sort=False)
        
        # HTML 렌더링 - 들여쓰기 제거
        html_content = """<table style="width:100%; border-collapse: collapse; font-size:13px; text-align:center;"><thead><tr style="background-color:#f1f3f5; border-bottom:2px solid #dee2e6;"><th style="padding:8px;">입항일</th><th style="padding:8px;">공급사</th><th style="padding:8px;">품명</th><th style="padding:8px;">CK</th><th style="padding:8px;">사이즈</th><th style="padding:8px;">단가</th><th style="padding:8px;">수량</th><th style="padding:8px;">상태</th></tr></thead><tbody>"""
        
        for date_str, group in grouped:
            html_content += f"""<tr style="background-color:#e7f5ff; border-top:1px solid #dee2e6; border-bottom:1px solid #dee2e6;"><td colspan="8" style="padding:6px; font-weight:bold; text-align:left; padding-left:15px;">📅 {date_str} (총 {len(group)}건)</td></tr>"""
            
            for _, row in group.iterrows():
                status_cls = "status-pending" if row['status'] == 'PENDING' else ("status-arrived" if row['status'] == 'ARRIVED'